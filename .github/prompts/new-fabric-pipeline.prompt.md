---
description: "Ny Fabric notebook-pipeline (bronze → silver → gold)"
mode: ask
---

Skapa en ny Fabric notebook-pipeline för en datakälla.

## Input

| Fält | Värde |
|------|-------|
| Datakälla | `${input:source:Datakällans namn (t.ex. PatientRegistret, GMS)}` |
| Format | `${input:format:CSV / JSON / Parquet / API}` |
| Nyckelkolumner | `${input:keys:Kommaseparerade primärnycklar (t.ex. patient_id, visit_id)}` |

## Generera

### Bronze notebook
- Ingestning av rådata till bronze-tabeller
- Delta-format med `_ingested_at` audit-kolumn

### Silver notebook
- Cleansing: null-hantering, datatyper, deduplicering
- SCD2 med `_valid_from`, `_valid_to`, `_is_current`
- Audit-kolumner: `_processed_at`, `_source_system`, `_row_hash`

### Gold notebook
- Aggregeringar anpassade för analytics/ML
- Business-namn på kolumner

## Krav

- Delta-format alltid
- MERGE INTO alltid (aldrig INSERT OVERWRITE)
- `spark`-variabeln är global i Fabric – aldrig `SparkSession.builder`
