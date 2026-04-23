---
applyTo: "**/*purview*,**/*governance*,**/*glossary*,**/*lineage*,**/*classification*,**/*catalog*"
---

# Microsoft Purview-instruktioner — Healthcare Analytics

## Purview i detta projekt

Projektet använder Microsoft Purview som centralt datakatalogsystem med:
- **188 glossary terms** organiserade i hierarkier
- **Klassificeringar** för känslig hälsodata (personnummer, diagnoser, läkemedel)
- **Lineage** från Azure SQL → Fabric Lakehouse → Power BI
- **Data products** som beskriver analytiska dataprodukter

## Purview REST API-mönster

```python
from azure.identity import DefaultAzureCredential
from azure.purview.catalog import PurviewCatalogClient

credential = DefaultAzureCredential()
client = PurviewCatalogClient(
    endpoint="https://<purview-account>.purview.azure.com",
    credential=credential
)
```

## Glossary Term-struktur

```python
# Obligatoriska fält per term
term = {
    "name": "Length of Stay",
    "qualifiedName": "Healthcare Analytics@Glossary.Healthcare Analytics.Clinical Measures.Length of Stay",
    "longDescription": "Antal dagar mellan inskrivning och utskrivning",
    "status": "Approved",
    "anchor": {"glossaryGuid": "<glossary-guid>"},
    "parentRelatedTerm": {"termGuid": "<parent-guid>"}  # för hierarkier
}
```

## Klassificeringar för hälsodata

| Klassificering | Beskrivning | Kolumner |
|---------------|-------------|----------|
| `Sweden.PersonalIdentityNumber` | Personnummer (12 siffror) | `person_id`, `national_id` |
| `EU.GDPR.SensitiveData.Health` | Hälsodata enligt GDPR Art 9 | `diagnosis_code`, `medication` |
| `Custom.ClinicalCode.ICD10` | ICD-10 diagnoskoder | `condition_source_value` |
| `Custom.ClinicalCode.ATC` | ATC läkemedelskoder | `drug_source_value` |

## Lineage-dokumentation

```python
# Lineage registreras för hela flödet:
# Azure SQL (källa) → Fabric Pipeline → Bronze → Silver → Gold → Power BI

# Process-entitet för lineage
process = {
    "typeName": "Process",
    "attributes": {
        "name": "Bronze Ingestion Pipeline",
        "qualifiedName": "healthcare-analytics://pipelines/bronze_ingestion"
    },
    "inputs": [{"guid": "<azure-sql-guid>"}],
    "outputs": [{"guid": "<bronze-table-guid>"}]
}
```

## Data Products

```python
# Data products beskriver analytiska dataprodukter
data_product = {
    "name": "Readmission Risk Score",
    "description": "ML-baserad riskpoäng för återinläggning inom 30 dagar",
    "owner": "Data Science Team",
    "glossaryTerms": ["Readmission Rate", "Length of Stay", "Risk Score"],
    "lineage": "gold.readmission_predictions → Power BI Dashboard"
}
```

## Viktiga regler

- **DefaultAzureCredential** för autentisering – aldrig API-nycklar
- **qualifiedName** måste vara globalt unikt inom Purview-kontot
- **Hierarkisk struktur** för glossary terms: Domain → Category → Term
- **Status-workflow**: Draft → Approved → Expired
- **Bulk-operationer** med `import_glossary_terms_via_csv` för stora dataset
- **ALDRIG** exponera personnummer eller annan PII i Purview-metadata
