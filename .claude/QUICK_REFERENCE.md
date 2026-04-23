# Healthcare Analytics Project — Quick Reference

**Type:** ML Prediction Platform + Azure Purview Data Governance  
**Status:** Production Infrastructure (95% complete)  
**Location:** `c:\code\healthcare-analytics\healthcare-analytics`

## Quick Facts
- **ML Models:** LOS Predictor (LightGBM) + Readmission Classifier (Random Forest)
- **Architecture:** Bronze → Silver → Gold (Medallion on SQL + Fabric)
- **Purview:** 188 glossary terms, 12 classifications, 4 data products, 7 collections
- **Status:** Infrastructure deployed, governance 95% complete, lineage demo ready

## Azure Resources
| Resource | Name | Status |
|----------|------|--------|
| Purview | prviewacc | ✅ OPERATIONAL |
| SQL Server | sql-hca-demo.database.windows.net | ✅ Active |
| SQL Database | HealthcareAnalyticsDB | ✅ Active (S0, AAD-only) |
| Fabric Workspace | Healthcare Analytics (afda4639-...) | ✅ Active |
| Key Vault | kv-hca-demo | ⚠️ Access restricted |
| Managed Identities | prviewacc (system), mi-purview (user) | ✅ Configured |

## Purview Data Catalog
- **Glossary:** "Sjukvårdstermer" (d939ea20-9c67-48af-98d9-b66965f7cde1)
- **188 terms** in 6 categories (85% have descriptions)
- **12 custom classifications** (Swedish Personnummer, SNOMED CT, ICD-10, etc.)
- **4 data products** (Klinisk Patientanalys, BrainChild, OMOP, ML Feature Store)
- **Portal:** https://purview.microsoft.com/glossary/d939ea20-9c67-48af-98d9-b66965f7cde1

## Quick Commands
```bash
# View all glossary terms (Python)
python scripts/show_all_glossary_terms.py

# View all glossary terms (PowerShell)
.\scripts\Show-GlossaryTerms.ps1

# Verify Purview status
python scripts/purview_reality_check.py

# SQL lineage setup (run in Azure Portal Query Editor)
# File: scripts/setup_sql_lineage.sql

# SQL lineage demo (run in Azure Portal Query Editor)
# File: scripts/create_lineage_demo.sql
```

## SQL Lineage Demo
- **Bronze:** patients_raw, visits_raw, medications_raw
- **Silver:** patients_clean, visits_enriched, medications_classified
- **Gold:** patient_summary, department_metrics, high_risk_patients
- **Demo Data:** 5 patients, 7 visits, 7 medications

## Pending Tasks
1. ⏳ **SQL Lineage Setup** (15 min) — Run `setup_sql_lineage.sql` in Azure Portal Query Editor
2. ⏳ **Lineage Demo** (5 min) — Run `create_lineage_demo.sql` + scan database (10-20 min)
3. ⚠️ **Governance Domains** (20 min) — Manual creation in Portal UI (REST API not supported)
4. ⏳ **Data Source Registration** (30 min) — Register SQL + Fabric workspaces in Purview

## Key Guides
- `PURVIEW_LINEAGE_GUIDE.md` — How to view lineage in Purview
- `SQL_LINEAGE_STEP_BY_STEP.md` — SQL setup walkthrough
- `PURVIEW_MANUAL_GUIDE.md` — API vs Portal UI capabilities
- `MANUAL_GOVERNANCE_DOMAINS_GUIDE.md` — Governance domain creation

**Full Documentation:** `.claude/PROJECT_OVERVIEW.md`
