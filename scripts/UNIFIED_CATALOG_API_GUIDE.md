# Purview Unified Catalog API - Setup & Användning

Komplett guide för att sätta upp och använda Purview Unified Catalog API för automatisering av governance i Region Gävleborgs Healthcare Analytics-miljö.

## 📋 Innehåll

1. [Översikt](#översikt)
2. [Förutsättningar](#förutsättningar)
3. [Setup-guide](#setup-guide)
4. [Användning](#användning)
5. [Praktiska exempel](#praktiska-exempel)
6. [Troubleshooting](#troubleshooting)

---

## Översikt

**Purview Unified Catalog API** (2025-09-15-preview) är Microsofts nya REST API för programmatisk hantering av governance-resurser:

- ✅ **Business Domains** - Organisera data i governance-domäner
- ✅ **Data Products** - Hantera dataprodukter med metadata och kvalitet
- ✅ **Glossary Terms** - Bulk-importera termer från terminologisystem
- ✅ **Critical Data Elements** - Definiera kritiska datafält
- ✅ **OKRs** - Koppla datamål till verksamhetsmål
- ✅ **Data Policies** - Automatisera åtkomstregler

### Varför Unified Catalog API?

**Tidigare (Atlas API v2):**
- ❌ Kan inte länka entities till governance domains → DomainReference errors
- ❌ Begränsad metadata-struktur
- ❌ Manuell hantering i portal

**Nu (Unified Catalog API):**
- ✅ Full domain-integration utan errors
- ✅ Rik metadata-modell för data products
- ✅ Bulk-operationer för skalbarhet
- ✅ CI/CD-integration möjlig

---

## Förutsättningar

### Azure Resources
- ✅ Purview account (prviewacc.purview.azure.com)
- ✅ Microsoft Entra ID tenant (71c4b6d5-0065-4c6c-a125-841a582754eb)
- ✅ Azure subscription med Purview aktiverat

### Behörigheter
Du behöver skapa en **Service Principal** med rätt roller. Detta görs automatiskt i setup-scriptet.

### Python Dependencies
```bash
pip install requests azure-identity
```

---

## Setup-guide

### Steg 1: Kör Setup-scriptet

```bash
cd healthcare-analytics
python scripts/setup_unified_catalog_access.py
```

Välj alternativ **[1] Genomföra hela setup** för att genomföra alla steg:

1. **Skapa Service Principal i Entra ID**
   - Får instruktioner för Azure Portal
   - Skapar App Registration
   - Genererar Client Secret
   
2. **Tilldela roller i Purview**
   - Data Steward (rekommenderat för full access)
   - Data Catalog Reader (för read-only)
   
3. **Spara credentials**
   - Skapar `.env.purview` fil
   - Lägger till i `.gitignore` automatiskt
   
4. **Testa API-åtkomst**
   - Verifierar alla endpoints
   - Ger feedback om behörigheter

### Steg 2: Verifiera Setup

```bash
python scripts/unified_catalog_client.py
```

Du bör se:
```
✅ Client initialized successfully
Found 4 domain(s):
  • Klinisk Vård: Clinical care and patient data
  • Forskning & Genomik: Research and genomics data
  ...
```

---

## Användning

### Grundläggande Client

```python
from unified_catalog_client import UnifiedCatalogClient

# Initialisera client (läser från .env.purview)
client = UnifiedCatalogClient()

# Lista business domains
domains = client.list_business_domains()
for domain in domains:
    print(f"Domain: {domain['name']}")

# Lista data products
products = client.list_data_products()
for product in products:
    print(f"Product: {product['name']}")
```

### Skapa Data Product

```python
# Skapa en ny data product
product = client.create_data_product(
    name='OMOP Clinical Data',
    description='Clinical data warehouse following OMOP CDM v5.4',
    domain_id='<forskning-genomik-domain-id>',
    owners=['clinical-team@gavleborg.se'],
    quality_score=0.95,
    sla={'availability': '99.5%', 'latency': '<1h'},
    tables=['person', 'visit_occurrence', 'condition_occurrence']
)

print(f"Created: {product['id']}")
```

### Bulk-import Glossary Terms

```python
# Importera FHIR-termer
fhir_terms = [
    {
        'name': 'FHIR Patient',
        'definition': 'Demographics and administrative information...',
        'domainId': '<domain-id>',
        'source': 'HL7 FHIR R4'
    },
    {
        'name': 'FHIR Observation',
        'definition': 'Measurements and assertions...',
        'domainId': '<domain-id>',
        'source': 'HL7 FHIR R4'
    }
]

result = client.bulk_create_glossary_terms(fhir_terms)
print(f"Created: {result['created']}, Failed: {result['failed']}")
```

---

## Praktiska exempel

### Exempel 1: Bulk-import av FHIR-termer

Importera standardiserade FHIR-resurser från terminologisystem:

```bash
python scripts/unified_catalog_examples.py
# Välj [1] Bulk-import av FHIR-termer
```

Importerar:
- Patient, Observation, Encounter, Condition
- Procedure, MedicationRequest
- Med svenska synonymer och definitioner från HL7 FHIR

### Exempel 2: Auto-skapa Data Product

Skapa data product automatiskt när ny Lakehouse deployas:

```bash
python scripts/unified_catalog_examples.py
# Välj [2] Auto-skapa Data Product
```

Skapar product med:
- Metadata från Fabric Lakehouse
- Tabellista och schema version
- Quality scores och SLA:er
- Automatisk länkning till domain

### Exempel 3: Quality Reporting för Power BI

Hämta data quality metrics och exportera till Power BI:

```bash
python scripts/unified_catalog_examples.py
# Välj [3] Data Quality reporting
```

Genererar `quality_report.json` med:
- Quality score per data product
- Completeness, accuracy, timeliness
- Ready för import i Power BI

### Exempel 4: CI/CD Pipeline för Governance

Integrera governance i deployment pipeline:

```bash
python scripts/unified_catalog_examples.py
# Välj [4] CI/CD pipeline
```

Visar exempel för:
- Azure DevOps pipeline YAML
- GitHub Actions workflow
- Automated metadata updates vid deployment

---

## Praktiska användningsfall för Region Gävleborg

### 📋 Bulk-import av FHIR-termer till Glossary
```python
# Patient, Diagnos, Observation, Encounter etc.
# Direkt från terminologisystem (SNOMED CT, LOINC, ICD-10)
# Automatisk uppdatering när standarder ändras

fhir_terms = load_from_terminology_system()
result = client.bulk_create_glossary_terms(fhir_terms)
```

### 📦 Skapa Data Products programmatiskt
```python
# När nya datamängder landar i Fabric
# Automatisk metadata från Lakehouse/Warehouse schema
# Länkning till governance domains och glossary terms

@fabric_deployment_hook
def on_lakehouse_created(lakehouse):
    product = client.create_data_product(
        name=lakehouse.name,
        tables=lakehouse.get_tables(),
        domain_id=get_domain_for_workspace(lakehouse.workspace)
    )
```

### 📊 POC-rapportering i Power BI
```python
# Hämta data quality scores via API
# Real-time governance dashboards
# SLA-monitoring för data products

products = client.list_data_products()
quality_data = [
    {
        'product': p['name'],
        'score': p['quality']['score'],
        'updated': p['lastUpdated']
    }
    for p in products
]
export_to_powerbi(quality_data)
```

### 🚀 CI/CD-pipeline för Governance
```yaml
# Sätt governance metadata som del av Fabric-deployment
# Version control för data product definitioner
# Automated testing av data policies och quality rules

- name: Update Purview Governance
  run: |
    python scripts/update_purview_governance.py
    python scripts/validate_policies.py
```

---

## Troubleshooting

### Problem: 403 Forbidden på alla endpoints

**Orsak:** Service Principal saknar roller i Purview

**Lösning:**
1. Gå till Purview Portal → Unified Catalog
2. Catalog Management → Governance domains → Root domain
3. Roles → Lägg till Service Principal
4. Tilldela "Data Steward" roll
5. Vänta 5-10 minuter för propagering

### Problem: 404 Not Found - API inte aktiverat

**Orsak:** Unified Catalog API är preview och måste aktiveras

**Lösning:**
1. Kontakta Azure Support
2. Begär aktivering av "Purview Unified Catalog API preview"
3. Ange Purview account: prviewacc.purview.azure.com
4. Vänta på bekräftelse (1-3 dagar)

### Problem: DomainReference authorization policy error

**Orsak:** Använder gamla Atlas API v2 istället för Unified Catalog API

**Lösning:**
```python
# ANVÄND INTE Atlas API för domain links:
# ❌ requests.post(f'{ATLAS_BASE}/entity/bulk', ...)

# ANVÄND Unified Catalog API:
# ✅ client.create_data_product(domainId='...', ...)
```

### Problem: Token expiration

**Orsak:** OAuth2 token har gått ut (vanligt efter 1 timme)

**Lösning:**
```python
# Client hanterar detta automatiskt
# Men för manuella requests:
client._get_access_token()  # Hämtar ny token
```

---

## Nästa steg

1. ✅ **Kör setup:** `python scripts/setup_unified_catalog_access.py`
2. ✅ **Testa exempel:** `python scripts/unified_catalog_examples.py`
3. ✅ **Anpassa för er miljö:** Uppdatera domain IDs, team emails, etc.
4. ✅ **Integrera i CI/CD:** Lägg till governance i deployment pipeline
5. ✅ **Skapa Power BI dashboard:** Importera quality metrics

---

## Support & Dokumentation

- **Microsoft Learn:** [Purview Unified Catalog API](https://learn.microsoft.com/azure/purview/unified-catalog-api)
- **API Reference:** [REST API Documentation](https://learn.microsoft.com/rest/api/purview/)
- **Region Gävleborg:** Kontakta Healthcare Analytics-teamet

---

**Skapad:** 2026-04-22  
**Version:** 1.0  
**Status:** Production Ready
