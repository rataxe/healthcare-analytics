---
applyTo: "**/*.sql,**/sql/**,**/warehouse/**"
---

# SQL-kodstil

Dessa regler gäller automatiskt för alla `.sql`-filer och filer under
`sql/` och `warehouse/`.

## Obligatoriskt

- **VERSALER** för SQL-nyckelord: `SELECT`, `FROM`, `WHERE`, `JOIN`
- **snake_case** för kolumnnamn: `patient_id`, `visit_date`
- **Explicit kolumnlista** – aldrig `SELECT *` i produktion
- **Alias med AS** – `FROM patients AS p` (aldrig implicit)
- **DATE-funktioner** – använd plattformens inbyggda: `GETDATE()`, `CURRENT_TIMESTAMP()`

## Formatting

```sql
-- ✅ RÄTT – tydlig formatering
SELECT
    p.patient_id,
    p.first_name,
    v.visit_date,
    d.diagnosis_code
FROM patients AS p
INNER JOIN visits AS v
    ON p.patient_id = v.patient_id
LEFT JOIN diagnoses AS d
    ON v.visit_id = d.visit_id
WHERE v.visit_date >= '2024-01-01'
ORDER BY v.visit_date DESC;
```

## Namnkonventioner

| Element | Stil | Exempel |
|---------|------|---------|
| Tabeller | `snake_case` | `condition_occurrence` |
| Kolumner | `snake_case` | `drug_exposure_start_date` |
| Vyer | `v_` prefix | `v_patient_summary` |
| Stored procs | `sp_` prefix | `sp_refresh_gold` |
| Indexar | `ix_tabell_kolumn` | `ix_visits_patient_id` |

## Medallion-tabeller

| Layer | Schema | Prefix |
|-------|--------|--------|
| Bronze | `bronze` | Ingen |
| Silver | `silver` | Ingen |
| Gold | `gold` | Ingen |

## Healthcare Analytics SQL-kontext

- **Schema**: DDL i `src/sql/01_schema_ddl.sql`
- **OMOP CDM**: `person`, `visit_occurrence`, `condition_occurrence`, `drug_exposure`, `measurement`, `specimen`
- **Warehouse**: Azure SQL `HealthcareAnalyticsDB` + Fabric Warehouse
