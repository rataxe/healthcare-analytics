"""Fast medications-only upload using fast_executemany."""
import logging
import struct
from pathlib import Path

import pandas as pd
import pyodbc
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SERVER = "sql-hca-demo.database.windows.net"
DATABASE = "HealthcareAnalyticsDB"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"


def get_connection():
    credential = AzureCliCredential()
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};",
        attrs_before={1256: token_struct},
    )
    return conn


def main():
    conn = get_connection()
    cursor = conn.cursor()

    # Check current state
    cursor.execute("SELECT COUNT(*) FROM hca.medications")
    current = cursor.fetchone()[0]
    log.info("Current medications: %d / 60563", current)

    # Clear and re-upload
    log.info("Clearing medications table...")
    cursor.execute("DELETE FROM hca.medications")
    conn.commit()
    log.info("Table cleared")

    # Load CSV
    df = pd.read_csv(DATA_DIR / "medications.csv")
    pk_col = df.columns[0]
    df = df.drop_duplicates(subset=[pk_col], keep="first")
    df = df.where(df.notna(), None)
    log.info("Uploading %d rows with fast_executemany...", len(df))

    cols = df.columns.tolist()
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    insert_sql = f"INSERT INTO hca.medications ({col_names}) VALUES ({placeholders})"

    # Enable fast_executemany for bulk performance
    cursor.fast_executemany = True

    batch_size = 5000
    total = len(df)
    for i in range(0, total, batch_size):
        # Reconnect every 30000 rows to keep token fresh
        if i > 0 and i % 30000 == 0:
            log.info("  Reconnecting at row %d...", i)
            conn.close()
            conn = get_connection()
            cursor = conn.cursor()
            cursor.fast_executemany = True

        batch = df.iloc[i : i + batch_size]
        rows = [
            tuple(None if v is None or (isinstance(v, float) and pd.isna(v)) else v for v in row)
            for row in batch.itertuples(index=False)
        ]
        try:
            cursor.executemany(insert_sql, rows)
            conn.commit()
            log.info("  Progress: %d / %d", min(i + batch_size, total), total)
        except pyodbc.Error as e:
            log.error("  Batch %d-%d failed: %s", i, i + batch_size, e)
            conn.rollback()
            # Fall back to smaller batches
            log.info("  Retrying in smaller batches of 500...")
            small_batch = 500
            for j in range(0, len(rows), small_batch):
                sub = rows[j : j + small_batch]
                try:
                    cursor.executemany(insert_sql, sub)
                    conn.commit()
                except pyodbc.Error as e2:
                    log.error("    Sub-batch failed: %s", e2)
                    conn.rollback()

    conn.close()

    # Verify
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM hca.medications")
    final = cursor.fetchone()[0]
    conn.close()
    status = "✅" if final >= 60563 else "❌"
    log.info("%s medications: %d / 60563", status, final)


if __name__ == "__main__":
    main()
