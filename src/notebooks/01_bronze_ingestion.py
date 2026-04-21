# Fabric Notebook — Bronze Layer Ingestion
# SQL → Lakehouse (Delta/Parquet)
# Läs instruktioner i toppen innan du kör: konfigurera JDBC-strängen i parametercellen.
#
# Kör cell-för-cell i Microsoft Fabric Notebook.

# ── PARAMETERCELL (tagga som "parameters" i Fabric) ──────────────────────────
JDBC_SERVER      = "sql-hca-demo.database.windows.net"
JDBC_DATABASE    = "HealthcareAnalyticsDB"
KEY_VAULT_URL    = "https://kv-hca-demo.vault.azure.net/"
BRONZE_LAKEHOUSE = "bronze_lakehouse"   # monterat som default lakehouse
BRONZE_PATH      = "Tables"             # skriv till managed Delta-tabeller

TABLES_TO_INGEST = [
    "hca.patients",
    "hca.encounters",
    "hca.diagnoses",
    "hca.vitals_labs",
    "hca.medications",
]

# ── CELL 1: Importer & logging ───────────────────────────────────────────────
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("BronzeIngestion")
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
log.info("Spark version: %s", spark.version)

# ── CELL 2: Konfigurera JDBC med AAD-autentisering ──────────────────────────
# SQL Server använder AAD-only auth — hämta access token via mssparkutils
access_token = mssparkutils.credentials.getToken("https://database.windows.net/")

jdbc_url = (
    f"jdbc:sqlserver://{JDBC_SERVER}:1433;"
    f"database={JDBC_DATABASE};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
)
log.info("JDBC URL konfigurerad (AAD token auth)")

# ── CELL 3: Ingest-funktion ──────────────────────────────────────────────────
def ingest_table(table_name: str) -> None:
    """Läser en tabell från Azure SQL och skriver till Bronze Lakehouse som Delta."""
    schema_name, tbl = table_name.split(".")
    delta_table_name = f"{schema_name}_{tbl}"

    log.info("Startar ingestion: %s → %s", table_name, delta_table_name)

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("accessToken", access_token)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        # Parallell inläsning med partitionColumn (om tillämpligt)
        .option("fetchsize", "10000")
        .load()
    )

    # Lägg till metadata-kolumner för lineage
    df = (
        df.withColumn("_ingested_at", current_timestamp())
          .withColumn("_source_table", lit(table_name))
    )

    row_count = df.count()
    log.info("Läste %d rader från %s", row_count, table_name)

    # Skriv som Delta-tabell (overwrite för initial load, merge för inkrementell)
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"{BRONZE_LAKEHOUSE}.{delta_table_name}")
    )
    log.info("✅ Sparad: %s (%d rader)", delta_table_name, row_count)

# ── CELL 4: Kör ingestion för alla tabeller ──────────────────────────────────
results = {}
for table in TABLES_TO_INGEST:
    try:
        ingest_table(table)
        results[table] = "✅ SUCCESS"
    except Exception as e:
        log.error("❌ FAILED: %s — %s", table, str(e))
        results[table] = f"❌ FAILED: {e}"

# ── CELL 5: Sammanfattning ───────────────────────────────────────────────────
print("\n=== INGESTION SUMMARY ===")
for tbl, status in results.items():
    print(f"  {tbl}: {status}")

# Validera att tabellerna finns
display(spark.sql(f"SHOW TABLES IN {BRONZE_LAKEHOUSE}"))
