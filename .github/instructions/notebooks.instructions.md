---
applyTo: "**/*.ipynb,**/notebooks/**/*.py,**/notebooks/**"
---

# Instruktioner för Fabric Notebooks

Dessa regler gäller automatiskt för alla `.ipynb`-filer och alla filer
under `notebooks/`-mapparna.

## Absoluta regler

- **`spark`-variabeln är global** i Fabric Notebooks – starta ALDRIG en ny session:
  ```python
  # ❌ FEL – duplicerar session och slukar minne
  spark = SparkSession.builder.getOrCreate()

  # ✅ RÄTT – använd den globala spark-variabeln direkt
  df = spark.read.format("delta").load("Tables/patients")
  ```

- **Delta-format alltid** – skriv och läs med `.format("delta")`
- **Audit-kolumner** – `_processed_at`, `_source_system` minst på silver/gold
- **Deduplicera INNAN MERGE** med `row_number()` – duplicat bryter MERGE
- **display()** för output i notebooks – aldrig `print(df.show())`

## Cell-ordning i notebook

1. **Cell 1**: Parametrar / konfiguration
2. **Cell 2**: Imports och hjälpfunktioner
3. **Cell 3+**: Bearbetning (en logisk steg per cell)
4. **Sista cell**: Validering / row counts

## SCD2 i silver

```python
# Obligatoriska kolumner
.withColumn("_valid_from", F.current_timestamp())
.withColumn("_valid_to",   F.lit(None).cast("timestamp"))
.withColumn("_is_current", F.lit(True))
.withColumn("_row_hash",   F.sha2(F.concat_ws("|", *key_cols), 256))
```

## Notebook-konventioner

- **Filnamn**: `NN_beskrivning.py` (t.ex. `01_bronze_ingestion.py`)
- **Medallion-lager**: bronze (raw) → silver (clean) → gold (aggregated)
