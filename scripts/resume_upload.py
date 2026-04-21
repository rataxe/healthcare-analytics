"""Resume data upload for tables that are incomplete. Uses fast_executemany and reconnects per table."""
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

TABLES = {
    "patients": "hca.patients",
    "encounters": "hca.encounters",
    "diagnoses": "hca.diagnoses",
    "vitals_labs": "hca.vitals_labs",
    "medications": "hca.medications",
}


def get_connection():
    credential = AzureCliCredential()
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};DATABASE={DATABASE};"
        f"Encrypt=yes;TrustServerCertificate=no;",
        attrs_before={1256: token_struct},
    )
    return conn


def main():
    conn = get_connection()
    cursor = conn.cursor()

    # Check current state
    log.info("Current row counts:")
    needs_upload = {}
    for csv_name, table_name in TABLES.items():
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        current = cursor.fetchone()[0]
        csv_path = DATA_DIR / f"{csv_name}.csv"
        expected = len(pd.read_csv(csv_path)) if csv_path.exists() else 0
        status = "✅" if current >= expected else "❌"
        log.info("  %s %s: %d / %d", status, table_name, current, expected)
        if current < expected:
            needs_upload[csv_name] = table_name
    conn.close()

    if not needs_upload:
        log.info("All tables complete!")
        return

    log.info("Tables needing upload: %s", list(needs_upload.keys()))

    for csv_name, table_name in needs_upload.items():
        log.info("Reconnecting for %s...", table_name)
        conn = get_connection()
        cursor = conn.cursor()

        # Clear incomplete data
        try:
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()
            log.info("  Cleared existing rows from %s", table_name)
        except pyodbc.Error as e:
            log.warning("  Delete failed: %s", e)
            conn.rollback()

        csv_path = DATA_DIR / f"{csv_name}.csv"
        df = pd.read_csv(csv_path)
        # Convert float columns that should be int (SMALLINT/INT in SQL) — pandas uses float64 when NaN present
        int_cols = {
            "vitals_labs": ["systolic_bp", "diastolic_bp", "heart_rate"],
        }
        for col in int_cols.get(csv_name, []):
            if col in df.columns:
                df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) else None)
        # Convert NaN to None for all columns
        df = df.where(df.notna(), None)
        log.info("  Uploading %s → %s (%d rows)", csv_name, table_name, len(df))

        cols = df.columns.tolist()
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

        batch_size = 1000
        reconnect_every = 10000  # Fresh connection every N rows to avoid timeout
        total = len(df)
        for i in range(0, total, batch_size):
            # Reconnect periodically to avoid token/connection timeout
            if i > 0 and i % reconnect_every == 0:
                log.info("  Reconnecting at row %d...", i)
                conn.close()
                conn = get_connection()
                cursor = conn.cursor()

            batch = df.iloc[i : i + batch_size]
            rows = [tuple(None if v is None or (isinstance(v, float) and pd.isna(v)) else v for v in row) for row in batch.itertuples(index=False)]
            try:
                cursor.executemany(insert_sql, rows)
                conn.commit()
            except pyodbc.Error as e:
                log.error("  Batch %d-%d failed: %s", i, i + batch_size, e)
                conn.rollback()
                # Reconnect and retry
                log.info("  Reconnecting after error...")
                conn.close()
                conn = get_connection()
                cursor = conn.cursor()
                cursor.executemany(insert_sql, rows)
                conn.commit()

            if (i + batch_size) % 5000 == 0 or i + batch_size >= total:
                log.info("  Progress: %d / %d", min(i + batch_size, total), total)

        log.info("  ✅ %s: %d rows uploaded", table_name, len(df))
        conn.close()

    # Final verification
    conn = get_connection()
    cursor = conn.cursor()
    log.info("Final row counts:")
    for csv_name, table_name in TABLES.items():
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        log.info("  %s: %d", table_name, count)
    conn.close()
    log.info("✅ Done!")


if __name__ == "__main__":
    main()
