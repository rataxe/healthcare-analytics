"""Deploy SQL schema and upload CSV data to Azure SQL Database using AAD auth."""
import logging
import struct
import sys
from pathlib import Path

import pandas as pd
import pyodbc
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SERVER = "sql-hca-demo.database.windows.net"
DATABASE = "HealthcareAnalyticsDB"

SQL_DDL_PATH = Path(__file__).parent.parent / "src" / "sql" / "01_schema_ddl.sql"
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

# Table mappings: csv filename -> sql table name
TABLES = {
    "patients": "hca.patients",
    "encounters": "hca.encounters",
    "diagnoses": "hca.diagnoses",
    "vitals_labs": "hca.vitals_labs",
    "medications": "hca.medications",
}


def get_connection():
    """Get pyodbc connection using Azure AD token."""
    credential = AzureCliCredential()
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Encrypt=yes;TrustServerCertificate=no;",
        attrs_before={1256: token_struct},  # SQL_COPT_SS_ACCESS_TOKEN
    )
    return conn


def deploy_schema(conn):
    """Execute DDL script."""
    log.info("Deploying schema from %s", SQL_DDL_PATH)
    ddl = SQL_DDL_PATH.read_text(encoding="utf-8")

    # Fix T-SQL incompatibilities
    ddl = ddl.replace(
        "CREATE SCHEMA IF NOT EXISTS hca;",
        "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'hca') EXEC('CREATE SCHEMA hca');",
    )

    # Split on GO statements
    statements = []
    current = []
    for line in ddl.splitlines():
        if line.strip().upper() == "GO":
            if current:
                statements.append("\n".join(current))
                current = []
        else:
            current.append(line)
    if current:
        statements.append("\n".join(current))

    cursor = conn.cursor()
    for stmt in statements:
        stmt_stripped = stmt.strip()
        if stmt_stripped:
            # Remove comment-only lines to check if there's actual SQL
            sql_lines = [l for l in stmt_stripped.splitlines() if not l.strip().startswith("--")]
            actual_sql = "\n".join(sql_lines).strip()
            if not actual_sql:
                continue
            try:
                cursor.execute(stmt_stripped)
                conn.commit()
                log.info("  OK: %s...", stmt_stripped[:80].replace("\n", " "))
            except pyodbc.Error as e:
                err_str = str(e)
                if "already an object" in err_str or "already exists" in err_str or "There is already" in err_str:
                    log.warning("  Skipping (already exists): %s...", stmt_stripped[:80].replace("\n", " "))
                    conn.rollback()
                else:
                    log.error("  FAILED: %s", err_str[:200])
                    conn.rollback()
                    raise
    log.info("✅ Schema deployed")


def upload_data(conn):
    """Upload CSV data to Azure SQL tables."""
    cursor = conn.cursor()

    # Truncate in reverse order (children first) to respect FK constraints
    for csv_name, table_name in reversed(list(TABLES.items())):
        try:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            conn.commit()
        except pyodbc.Error:
            # If truncate fails, try delete
            conn.rollback()
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()

    # Insert in order (parents first)
    for csv_name, table_name in TABLES.items():
        csv_path = DATA_DIR / f"{csv_name}.csv"
        if not csv_path.exists():
            log.warning("Skipping %s — file not found", csv_path)
            continue

        df = pd.read_csv(csv_path)
        log.info("Uploading %s → %s (%d rows)", csv_name, table_name, len(df))

        # Insert in batches
        cols = df.columns.tolist()
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"

        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]
            rows = [tuple(None if pd.isna(v) else v for v in row) for row in batch.itertuples(index=False)]
            cursor.executemany(insert_sql, rows)
            conn.commit()

        log.info("  ✅ %s: %d rows uploaded", table_name, len(df))

    log.info("✅ All data uploaded")


def main():
    log.info("Connecting to %s / %s (AAD auth)...", SERVER, DATABASE)
    conn = get_connection()
    log.info("✅ Connected")

    deploy_schema(conn)
    upload_data(conn)

    conn.close()
    log.info("Done!")


if __name__ == "__main__":
    main()
