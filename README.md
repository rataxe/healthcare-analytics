# Healthcare Analytics: LOS & Readmission Predictor
> **Microsoft Fabric · Azure SQL · MLflow · Power BI DirectLake**

End-to-end prediktiv analysplattform för hälso- och sjukvård.  
Tränar två ML-modeller på syntetisk klinisk data:
- **LOS (Length of Stay)** — Regression med LightGBM (Poisson-objektiv)
- **Readmission 30d** — Klassificering med Random Forest (balanserade klasser)

---

## Arkitektur

```
Azure SQL (hca.*)
    │
    │  Fabric Data Pipeline (JDBC + AAD token)
    ▼
Bronze Lakehouse          ← rådata, Delta-format, linjeannoteringar
    │
    │  02_silver_features.py (Spark)
    ▼
Silver Lakehouse          ← Charlson CCI, prior admissions, feature-joins
    │
    │  03_ml_training.py (Scikit-learn + LightGBM + MLflow)
    ▼
Gold Lakehouse            ← prediktioner, high-risk-flaggning
    │
    ▼
Power BI (DirectLake)     ← LOS actual vs predicted, High-risk patients
    │
Microsoft Purview         ← datakatalog, PHI-klassificering, linjespårning
```

---

## Deployed Infrastructure

| Resurs | Namn | Region |
|---|---|---|
| Resource Group | rg-healthcare-analytics | swedencentral |
| Azure SQL Server | sql-hca-demo (AAD-only) | swedencentral |
| Azure SQL Database | HealthcareAnalyticsDB (S0) | swedencentral |
| Key Vault | kv-hca-demo (RBAC) | swedencentral |
| Fabric Workspace | Healthcare-Analytics | Sweden Central |
| Bronze Lakehouse | bronze_lakehouse | — |
| Silver Lakehouse | silver_lakehouse | — |
| Gold Lakehouse | gold_lakehouse | — |
| Fabric Pipeline | healthcare_etl_pipeline | — |
| Purview Account | prviewacc (tenant-level) | westus2 |

---

## Snabbstart (VS Code)

### 1. Klona och konfigurera
```bash
git clone https://github.com/your-org/healthcare-analytics.git
cd healthcare-analytics
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
```

### 2. Generera syntetisk data
```bash
python src/python/generate_synthetic_data.py --rows 10000 --output data/raw
```

### 3. Deploya SQL-schema och ladda data
```bash
python scripts/deploy_sql_schema.py
```
Skriptet kopplar sig till Azure SQL med AAD-token, deployar DDL och laddar CSV-data.

### 4. Deploya Fabric-artefakter
```bash
python scripts/upload_notebooks.py
```

### 5. Konfigurera Purview
```bash
python scripts/configure_purview.py
```

### 6. Validera deployment
```bash
python scripts/validate_deployment.py
```

### 7. Kör Fabric Notebooks (i ordning)
1. `01_bronze_ingestion.py` → Bronze Lakehouse
2. `02_silver_features.py` → Silver Lakehouse (Charlson CCI, feature engineering)
3. `03_ml_training.py` → Gold Lakehouse + MLflow Experiments

---

## Projektstruktur

```
healthcare-analytics/
├── .github/
│   ├── copilot-instructions.md    ← GitHub Copilot-kontext (VIKTIG!)
│   └── workflows/
│       └── fabric-deploy.yml      ← CI/CD till Fabric
├── .vscode/
│   ├── settings.json
│   └── extensions.json
├── src/
│   ├── sql/
│   │   └── 01_schema_ddl.sql      ← Azure SQL-schema (hca.*)
│   ├── python/
│   │   └── generate_synthetic_data.py
│   ├── notebooks/
│   │   ├── 01_bronze_ingestion.py  ← Kör i Fabric Notebook
│   │   ├── 02_silver_features.py   ← Feature engineering + CCI
│   │   └── 03_ml_training.py       ← LightGBM + RF + MLflow
│   └── pipelines/                  ← (Fabric Pipeline JSON-definitioner)
├── tests/                          ← pytest-tester
├── data/
│   └── raw/                        ← Genererad CSV (ej git-trackad)
├── docs/                           ← Arkitekturdiagram, projektdok
├── .env.template                   ← Miljövariabel-mall
├── requirements.txt
└── requirements-dev.txt
```

---

## Copilot-prompt: Nästa steg

Kopiera och kör dessa i **GitHub Copilot Chat** (Ctrl+Shift+I) inifrån VS Code:

### Skapa Azure SQL-tabell och ladda data
```
@workspace Baserat på src/sql/01_schema_ddl.sql och data/raw/patients.csv, 
skriv ett Python-skript som använder pyodbc och bulk insert för att 
ladda alla CSV-filer till Azure SQL med felhantering och logging.
```

### Lägg till Great Expectations-validering
```
@workspace I src/notebooks/02_silver_features.py, lägg till Great Expectations 
data validation suite som verifierar: los_days > 0, readmission_30d in [0,1], 
age_at_admission between 18-120, och att inga nulls finns i ML-targets.
```

### Power BI DirectLake-schema
```
@workspace Designa ett Power BI-schema (star schema) för DirectLake-läge 
baserat på gold_lakehouse.ml_predictions. Inkludera dim_patient, dim_date, 
fact_predictions med rätt relationships och DAX-measures för: 
avg_predicted_los, readmission_rate_pct, high_risk_count.
```

### Purview-integration
```
@workspace Skriv ett Python-skript som använder azure-purview-catalog SDK för att:
1. Skanna Azure SQL-databasen hca.*
2. Applicera klassificeringen "Swedish_PersonNummer" på patients.postal_code  
3. Tagga encounters-tabellen med sensitivity label "Confidential - Health"
```

---

## ML-modeller

| Modell | Algoritm | Target | Metric |
|--------|----------|--------|--------|
| LOS Predictor | LightGBM (Poisson) | `los_days` | MAE, RMSE |
| Readmission | Random Forest | `readmission_30d` | ROC-AUC |

### Key Features
- **Charlson Comorbidity Index (CCI)** — beräknad från ICD-10-koder
- **prior_admissions_12m** — antal tidigare inläggningar senaste 12 månader  
- **polypharmacy** — ≥5 mediciner vid inskrivning
- **latest vitals/labs** — senaste uppmätta värden per encounter

---

## Purview-integration

1. Skapa en **Collection** i Microsoft Purview: `HealthcareAnalytics`
2. Registrera datakälla: `Azure SQL / hca.*`
3. Kör en **Scan** med managed identity
4. Applicera classifications:
   - `hca.patients.birth_date` → `DATE_OF_BIRTH`
   - `hca.patients.postal_code` → `SWEDISH_ADDRESS`  
   - Alla tabeller i `hca.*` → Sensitivity label: `Confidential - Health Data`

---

## GitHub Secrets (för CI/CD)

Sätt dessa i GitHub → Settings → Secrets → Actions:

| Secret | Beskrivning |
|--------|-------------|
| `FABRIC_TENANT_ID` | Entra ID tenant GUID |
| `FABRIC_CLIENT_ID` | Service Principal client ID |
| `FABRIC_CLIENT_SECRET` | Service Principal secret |
| `FABRIC_WORKSPACE_ID` | Target Fabric workspace GUID |

---

## Kontakt
Johan Andolf — Solution Engineer Data, Microsoft Sweden  
Projekt: Healthcare Analytics POC (LOS & Readmission)
