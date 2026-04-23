# Purview Governance Skill

## Purpose
Data governance specialist for Microsoft Purview integration with the Healthcare Analytics Fabric Lakehouse.

## When to Use
- Configuring Purview classifications, glossary terms, or sensitivity labels
- Setting up or debugging lineage tracking across Bronze → Silver → Gold
- Registering data products or managing governance domains
- Auditing governance compliance

## Key Context

### Architecture
- **Microsoft Purview** connected to **Microsoft Fabric** workspace
- 188 glossary terms organized in governance domains
- Sensitivity labels: Confidential (PHI), Internal (aggregated), Public (anonymized)
- Lineage: Bronze (raw ingest) → Silver (OMOP CDM) → Gold (ML features / aggregates)

### Authentication
```python
from azure.identity import DefaultAzureCredential
from azure.purview.catalog import PurviewCatalogClient

credential = DefaultAzureCredential()
client = PurviewCatalogClient(
    endpoint=f"https://{purview_account_name}.purview.azure.com",
    credential=credential,
)
```

### Classification Patterns
| Classification | Sensitivity | Example Columns |
|---|---|---|
| PHI | Confidential | patient_id, birth_date, name |
| PII | Confidential | ssn, address, phone |
| ICD-10 | Internal | diagnosis_code, condition_code |
| Clinical | Internal | measurement_value, drug_exposure |
| Aggregated | Internal | avg_los, readmission_rate |
| Public | Public | hospital_region, age_group |

### Glossary Term Structure
Every glossary term must include:
- **Name**: PascalCase (e.g., `LengthOfStay`)
- **Definition**: Clear, clinical definition
- **Steward**: Assigned data steward
- **Status**: Draft → Approved → Published
- **Related terms**: Cross-references to other terms
- **Domain**: Healthcare domain category

### Key Scripts
- `scripts/configure_purview.py` – baseline Purview setup
- `scripts/configure_purview_full.py` – full configuration
- `scripts/purview_add_metadata.py` – metadata enrichment
- `scripts/purview_data_products.py` – data product registration
- `scripts/full_purview_audit.py` – governance audit
- `scripts/create_governance_domains.py` – domain creation
- `scripts/purview_fabric_glossary_tenant.py` – Fabric-Purview glossary sync

## Rules
1. Always use `DefaultAzureCredential()` – never hard-code tokens
2. Check existing state before creating/updating resources (idempotent operations)
3. Validate glossary term structure before submission
4. Map sensitivity labels to data classification consistently
5. Document all governance changes in audit trail
6. Use `swedencentral` region for GDPR compliance
7. Never log PHI or PII data in governance scripts
