---
applyTo: "**/fabric/**,**/*.DataPipeline/**,**/*.Notebook/**"
---

# Fabric-instruktioner — Healthcare Analytics

## Dataflöde

```
Azure SQL Database
  → Fabric Data Pipeline (ingestion)
    → Bronze Lakehouse (rådata, Delta)
      → Silver Lakehouse (rensad, typad, SCD2)
        → Gold Lakehouse (aggregerad, ML-features, Power BI)
```

## Notebooks i projektet

| Notebook | Lager | Beskrivning |
|----------|-------|-------------|
| `01_bronze_ingestion.py` | Bronze | Ingesterar från Azure SQL → Delta |
| `02_silver_features.py` | Silver | Rensar, typerar, SCD2-logik |
| `03_ml_training.py` | Gold | LightGBM-träning med MLflow |
| `04_omop_diag.py` | Silver | OMOP CDM-diagnostabeller |
| `04_omop_transformation.py` | Silver | OMOP-transformation |
| `05_batch_scoring.py` | Gold | Batch-prediktion |
| `06_scoring_dashboard.py` | Gold | Power BI-förberedelse |

## Anslutningar

```python
# Azure SQL-anslutning via JDBC (Key Vault-hemligheter)
from notebookutils import mssparkutils

jdbc_url = mssparkutils.credentials.getSecret("kv-healthcare", "sql-connection-string")

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.visit_occurrence") \
    .load()
```

## SCD2-mönster (Slowly Changing Dimension Type 2)

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, silver_path)

target.alias("t").merge(
    source_df.alias("s"),
    "t.person_id = s.person_id AND t._is_current = true"
).whenMatchedUpdate(
    condition="t._row_hash <> s._row_hash",
    set={
        "_is_current": "false",
        "_valid_to": "current_timestamp()"
    }
).whenNotMatchedInsert(
    values={
        "person_id": "s.person_id",
        "_is_current": "true",
        "_valid_from": "current_timestamp()",
        "_valid_to": "cast('9999-12-31' as timestamp)",
        "_row_hash": "s._row_hash"
    }
).execute()
```

## Audit-kolumner (obligatoriska i Silver/Gold)

```python
from pyspark.sql import functions as F

df = df.withColumn("_processed_at", F.current_timestamp()) \
       .withColumn("_source_system", F.lit("azure-sql-healthcare")) \
       .withColumn("_row_hash", F.sha2(F.concat_ws("|", *tracked_cols), 256)) \
       .withColumn("_is_current", F.lit(True)) \
       .withColumn("_valid_from", F.current_timestamp()) \
       .withColumn("_valid_to", F.lit("9999-12-31").cast("timestamp"))
```

## Deduplicering (Silver)

```python
from pyspark.sql import Window

# Deduplicera baserat på primärnyckel, behåll senaste
w = Window.partitionBy("person_id").orderBy(F.col("_ingested_at").desc())
df_dedup = df.withColumn("_rn", F.row_number().over(w)) \
             .filter(F.col("_rn") == 1) \
             .drop("_rn")
```

## Viktiga regler

- **spark** är global – aldrig `SparkSession.builder`
- **Delta-format** alltid
- **SCD2** i Silver för dimensionstabeller
- **Deduplicering** före skrivning i Silver
- **Key Vault** – alla hemligheter via `mssparkutils.credentials.getSecret()`
- **OMOP CDM** – följ OMOP-namngivning för kliniska tabeller
- **ALDRIG** personnummer eller namn i loggar eller felmeddelanden
