# Purview Complete Setup Guide

## 🎯 Översikt

Detta projekt innehåller komplett automation för Azure Purview governance med:
- ✅ Key Vault integration för secrets
- ✅ Data Quality API (15 metoder)  
- ✅ Unified Catalog API (51 metoder)
- ✅ Fabric Self-Serve Analytics
- ✅ Credential scanning konfiguration
- ✅ Automatisk domain-linking
- ✅ Health monitoring

## 📋 Förutsättningar

1. **Azure Subscription Access**
   - Subscription ID: `5b44c9f3-bbe7-464c-aa3e-562726a12004`
   - Resource Group: `purview`
   - Purview Account: `prviewacc`

2. **Azure CLI installerat och inloggat**
   ```powershell
   az login
   az account set --subscription 5b44c9f3-bbe7-464c-aa3e-562726a12004
   ```

3. **Python 3.8+ med dependencies**
   ```powershell
   pip install -r requirements.txt
   ```

4. **Nödvändiga Azure-roller**
   - Purview Data Curator (för metadata)
   - Purview Data Source Administrator (för scanning)
   - Key Vault Secrets Officer (för credentials)

## 🚀 Quick Start (5 steg)

### Steg 1: Key Vault Setup
Skapa Key Vault och lagra alla secrets säkert:

```powershell
# Skapa Key Vault (om inte finns)
az keyvault create --name prview-kv --resource-group purview --location swedencentral

# Kör interactive setup
python scripts/setup_keyvault_credentials.py
```

**Vad sker:**
- Skapar Azure Key Vault `prview-kv`
- Promptar för Service Principal credentials
- Lagrar alla secrets säkert
- Skapar `.env.purview` lokalt
- Skapar `get_keyvault_secrets.py` helper

### Steg 2: Service Principal för Unified Catalog API
Skapa Service Principal för API-access:

```powershell
# Skapa App Registration
az ad app create --display-name "Purview-ServicePrincipal"

# Får Application (client) ID - spara denna!
$APP_ID = "<din-client-id>"

# Skapa client secret
az ad app credential reset --id $APP_ID --append

# Tilldela Data Steward-rollen i Purview
# Görs manuellt i Purview Portal:
# 1. Gå till prviewacc → Data Map → Collections → Root
# 2. Role assignments → Add
# 3. Välj "Data Steward"
# 4. Lägg till Service Principal
```

### Steg 3: Konfigurera Purview Managed Identity
Aktivera och konfigurera Managed Identity:

```powershell
# Aktivera Managed Identity på Purview
az purview account update --name prviewacc --resource-group purview --mi-system-assigned

# Få Managed Identity ID
$MI_ID = az purview account show --name prviewacc --resource-group purview --query "identity.principalId" -o tsv

# Ge MI access till Key Vault
az keyvault set-policy --name prview-kv --object-id $MI_ID --secret-permissions get list
```

### Steg 4: Konfigurera Fabric Integration
Fixa "Connection failed" för Self-Serve Analytics:

```powershell
# Kör Fabric analytics konfiguration
python scripts/configure_purview_fabric_analytics.py

# Ge Purview MI access till Fabric Workspace
# Görs manuellt i Fabric Portal:
# 1. Öppna workspace "DataGovernenne"
# 2. Workspace settings → Manage access
# 3. Lägg till Purview Managed Identity
# 4. Ge rollen "Contributor" eller "Admin"
```

### Steg 5: Konfigurera Scan Credentials
Setup credentials för data source scanning:

```powershell
python scripts/configure_purview_scan_credentials.py
```

## 📚 Alla Scripts - Komplett Guide

### 🔑 Credential & Security Management

#### `setup_keyvault_credentials.py`
**Syfte:** Central Key Vault setup för alla secrets

**Usage:**
```powershell
python scripts/setup_keyvault_credentials.py
```

**Skapar:**
- Azure Key Vault integration
- Service Principal credentials
- SQL credentials
- Storage account keys
- `.env.purview` fil
- `get_keyvault_secrets.py` helper

**Output:**
- ✅ Key Vault konfigurerad
- ✅ Secrets lagrade
- ✅ `.env.purview` skapad

---

#### `get_keyvault_secrets.py`
**Syfte:** Helper för att hämta secrets från Key Vault

**Usage:**
```python
from get_keyvault_secrets import get_secret, get_purview_credentials

# Hämta specifik secret
client_secret = get_secret('purview-sp-client-secret')

# Hämta alla Purview credentials
creds = get_purview_credentials()
print(creds['client_id'])
```

**Funktioner:**
- `get_secret(name)` - Hämta enskild secret
- `get_all_secrets()` - Hämta alla secrets
- `get_purview_credentials()` - Hämta Purview SP credentials

---

#### `configure_purview_scan_credentials.py`
**Syfte:** Konfigurera scan credentials enligt Microsoft docs

**Usage:**
```powershell
python scripts/configure_purview_scan_credentials.py
```

**Skapar:**
- SQL Authentication credentials
- Service Principal credentials
- Account Key credentials
- Key Vault integration i Purview

**Credential Types:**
- `SqlAuth` - SQL Server scanning
- `ServicePrincipal` - Azure resource scanning
- `AccountKey` - Storage account scanning
- `ManagedIdentity` - Rekommenderad metod

---

### 🎨 Unified Catalog API Client

#### `unified_catalog_client.py`
**Syfte:** Komplett Python REST client för Unified Catalog API

**Implementerade metoder:** 51 av 71 (72%)

**Resource Groups:**
1. **Business Domains** (9/9) ✅
   - `list_business_domains()`
   - `get_business_domain(domain_id)`
   - `create_business_domain(name, description, parent_id)`
   - `update_business_domain(domain_id, updates)`
   - `delete_business_domain(domain_id)`
   - `query_business_domains(query)`
   - `list_domain_relationships(domain_id)`
   - `create_domain_relationship(domain_id, relationship)`
   - `delete_domain_relationship(domain_id, relationship_id)`

2. **Data Products** (11/11) ✅
   - `list_data_products(domain_id, status)`
   - `get_data_product(product_id)`
   - `create_data_product(name, domain_id, description, owners)`
   - `update_data_product(product_id, updates)`
   - `delete_data_product(product_id)`
   - `publish_data_product(product_id)`
   - `query_data_products(query)`
   - `list_data_product_relationships(product_id)`
   - `create_data_product_relationship(product_id, relationship_type, target_id)`
   - `delete_data_product_relationship(product_id, relationship_id)`
   - `add_assets_to_data_product(product_id, asset_ids)`

3. **Glossary Terms** (13/13) ✅
   - `list_glossary_terms(glossary_id, category_id, status)`
   - `get_glossary_term(term_id)`
   - `create_glossary_term(name, definition, glossary_id, category_id)`
   - `update_glossary_term(term_id, updates)`
   - `delete_glossary_term(term_id)`
   - `publish_glossary_term(term_id)`
   - `unpublish_glossary_term(term_id)`
   - `bulk_create_glossary_terms(terms)`
   - `list_glossary_term_relationships(term_id)`
   - `create_glossary_term_relationship(term_id, relationship)`
   - `delete_glossary_term_relationship(term_id, relationship_id)`
   - `import_glossary_terms_csv(file_path, glossary_id)`
   - `export_glossary_terms_csv(glossary_id, output_path)`

4. **Critical Data Elements** (9/9) ✅
5. **OKRs** (9/9) ✅
6. **Data Access Policies** (3/3) ✅

**Usage:**
```python
from unified_catalog_client import UnifiedCatalogClient

client = UnifiedCatalogClient()

# Skapa domain
domain = client.create_business_domain(
    name="Clinical Data Management",
    description="Kliniska data och processer"
)

# Skapa data product
product = client.create_data_product(
    name="OMOP CDM",
    domain_id=domain['id'],
    description="OMOP Common Data Model implementation"
)

# Lista alla domains
domains = client.list_business_domains()
```

---

#### `unified_catalog_data_quality.py`
**Syfte:** Data Quality API implementation (15 metoder)

**Resource Groups:**
1. **Connections** (4/4) ✅
   - `create_dq_connection()` - Skapa data source connection
   - `list_dq_connections()` - Lista connections
   - `get_dq_connection()` - Hämta specifik connection
   - `delete_dq_connection()` - Ta bort connection

2. **Rules** (5/5) ✅
   - `create_dq_rule()` - Skapa quality rule
   - `list_dq_rules()` - Lista rules
   - `get_dq_rule()` - Hämta rule
   - `update_dq_rule()` - Uppdatera rule
   - `delete_dq_rule()` - Ta bort rule

3. **Profiling** (2/2) ✅
   - `run_data_profiling()` - Kör profiling
   - `get_profiling_results()` - Hämta resultat

4. **Scans** (3/3) ✅
   - `schedule_quality_scan()` - Schemalägg scan
   - `run_quality_scan()` - Kör scan nu
   - `get_scan_status()` - Få scan status

5. **Scores** (1/1) ✅
   - `get_quality_scores()` - Hämta quality scores

**Rule Types:**
- `COMPLETENESS` - Null checks, required fields
- `ACCURACY` - Value validation, format checks
- `CONSISTENCY` - Cross-table consistency
- `VALIDITY` - Domain validation, range checks
- `UNIQUENESS` - Duplicate detection
- `TIMELINESS` - Freshness checks

**Usage:**
```python
from unified_catalog_data_quality import DataQualityClient

client = DataQualityClient()

# Skapa connection
conn = client.create_dq_connection(
    name="Healthcare_SQL",
    source_type="AzureSqlDatabase",
    connection_details={
        "server": "myserver.database.windows.net",
        "database": "healthcare",
        "credentialReference": "sql-admin-password"
    }
)

# Skapa quality rule
rule = client.create_dq_rule(
    name="Patient_ID_Not_Null",
    rule_type="COMPLETENESS",
    logic="patient_id IS NOT NULL",
    connection_id=conn['id'],
    target_asset="dbo.patients",
    severity="HIGH"
)

# Schemalägg daglig scan
scan = client.schedule_quality_scan(
    name="Daily_Healthcare_Quality",
    connection_id=conn['id'],
    rule_ids=[rule['id']],
    schedule={
        "type": "CRON",
        "expression": "0 2 * * *"  # Kl 02:00 varje dag
    }
)
```

---

### 🔗 Automation Scripts

#### `automate_domain_linking.py`
**Syfte:** Automatisk länkning mellan domains, products och terms

**Features:**
- Pattern-based matching
- Domain → Product linking
- Term → Product linking
- Dry-run mode

**Usage:**
```powershell
# Dry run (visa vad som skulle göras)
python scripts/automate_domain_linking.py

# Live mode (applicera ändringar)
python scripts/automate_domain_linking.py --live
```

**Mapping Rules:**
```python
DOMAIN_MAPPING = {
    r'(OMOP|CDM|Clinical)': 'Clinical Data Management',
    r'(Genomic|BTB|VCF)': 'Genomics & Precision Medicine',
    r'(SBCR|Cancer|Registry)': 'Cancer Registry',
    r'(FHIR|GMS|Interoperability)': 'Interoperability & Standards',
}
```

**Output:**
```
Domain-to-Product Links:
  • Created: 12
  • Failed:  0
  • Skipped: 4

Term-to-Product Links:
  • Created: 45
  • Failed:  2
```

---

#### `purview_monitoring.py`
**Syfte:** Health monitoring och alerting

**Checks:**
- ✅ API availability (Unified Catalog, Data Quality)
- ✅ Domain health (empty domains, relationships)
- ✅ Data product health (missing terms, status)
- ✅ Glossary term health (missing definitions)
- ✅ Data quality scores (threshold alerts)

**Usage:**
```powershell
# En gång check
python scripts/purview_monitoring.py

# Continuous monitoring (var 5:e minut)
python scripts/purview_monitoring.py --continuous --interval 300

# Med Teams alerting
python scripts/purview_monitoring.py --teams-webhook https://outlook.office.com/webhook/...
```

**Output:**
```
================================================================================
  PURVIEW MONITORING REPORT
================================================================================

Timestamp: 2026-04-22 14:30:00

Overall Health: ✅ HEALTHY

api: ✅ HEALTHY
domains: ✅ HEALTHY
  Total domains: 4
  With products: 4
data_products: ⚠️ WARNING
  Total products: 12
  With terms: 10
glossary: ✅ HEALTHY
  Total terms: 184
  Published: 180
data_quality: ✅ HEALTHY
```

---

#### `configure_purview_fabric_analytics.py`
**Syfte:** Fixa Fabric Self-Serve Analytics connection

**Fixar:**
- ❌ "Connection failed" error
- ❌ Authentication issues
- ❌ Storage configuration
- ❌ Managed Identity permissions

**Usage:**
```powershell
python scripts/configure_purview_fabric_analytics.py
```

**Steg:**
1. Checkar current configuration
2. Verifierar Purview Managed Identity
3. Testar Fabric connection
4. Genererar permission commands
5. Konfigurerar Fabric storage
6. Testar analytics query

**Kräver:**
- Purview Managed Identity aktiverad
- MI har Contributor på Fabric workspace
- Fabric lakehouse "DEH" tillgängligt

---

### 📖 Setup & Guidance

#### `setup_unified_catalog_access.py`
**Syfte:** Interaktiv 4-stegs guide för Service Principal setup

**Steg:**
1. Skapa Service Principal i Entra ID
2. Tilldela Data Steward-roll i Purview
3. Spara credentials (.env.purview)
4. Testa access (5 endpoints)

**Usage:**
```powershell
python scripts/setup_unified_catalog_access.py
```

---

#### `unified_catalog_examples.py`
**Syfte:** 4 praktiska exempel för Region Gävleborg

**Examples:**
1. **Bulk-import FHIR terms** - Importera 50+ FHIR terms från CSV
2. **Auto-create Data Product** - Automation vid Lakehouse deploy
3. **Quality reporting** - Power BI integration för quality metrics
4. **CI/CD pipeline** - Azure DevOps YAML för automated deployment

**Usage:**
```powershell
python scripts/unified_catalog_examples.py
```

---

## 📊 Project Status

### ✅ Completed (66 items)

**API Implementation:**
- ✅ 51 Unified Catalog methods (6/7 resource groups)
- ✅ 15 Data Quality methods (all resource groups)
- ✅ Key Vault integration
- ✅ OAuth2 authentication flow
- ✅ Error handling & retry logic

**Automation:**
- ✅ Domain-product linking
- ✅ Term-product linking
- ✅ Health monitoring
- ✅ Fabric analytics configuration
- ✅ Scan credential setup

**Documentation:**
- ✅ UNIFIED_CATALOG_API_GUIDE.md
- ✅ SDK_COMPARISON.md
- ✅ PURVIEW_PROJECT_COMPLETE_INVENTORY.md
- ✅ MICROSOFT_SAMPLES_ANALYSIS.md
- ✅ Denna README

**Tools:**
- ✅ setup_keyvault_credentials.py
- ✅ get_keyvault_secrets.py
- ✅ configure_purview_scan_credentials.py
- ✅ configure_purview_fabric_analytics.py
- ✅ automate_domain_linking.py
- ✅ purview_monitoring.py

### ⏳ In Progress (3 items)

**Azure Configuration:**
- ⏳ Service Principal creation (guide exists, user needs to execute)
- ⏳ Purview Managed Identity setup (script exists, needs Azure Portal step)
- ⏳ Fabric workspace permissions (guide exists, manual step required)

### 📝 Future Enhancements (5 items)

**CI/CD:**
- 📝 Full Azure DevOps pipeline implementation
- 📝 GitHub Actions workflow
- 📝 Automated testing suite

**Additional Features:**
- 📝 Automated lineage creation
- 📝 Bulk metadata import from Excel

## 🎯 Praktiska Use Cases

### Use Case 1: Daglig Quality Check
```python
from unified_catalog_data_quality import DataQualityClient

client = DataQualityClient()

# Schemalägg daglig quality check
scan = client.schedule_quality_scan(
    name="Daily_OMOP_Quality",
    connection_id="sql-conn-123",
    rule_ids=["completeness-1", "validity-2"],
    schedule={"type": "CRON", "expression": "0 2 * * *"}
)

print(f"✅ Scan scheduled: {scan['id']}")
```

### Use Case 2: Bulk-importera Glossary Terms
```python
from unified_catalog_client import UnifiedCatalogClient

client = UnifiedCatalogClient()

# Importera från CSV
result = client.import_glossary_terms_csv(
    file_path="data/fhir_terms.csv",
    glossary_id="glossary-123"
)

print(f"✅ Imported {result['created']} terms")
```

### Use Case 3: Kontinuerlig Monitoring
```powershell
# Starta continuous monitoring med Teams alerts
python scripts/purview_monitoring.py `
  --continuous `
  --interval 300 `
  --teams-webhook "https://outlook.office.com/webhook/..."
```

### Use Case 4: Automatisk Domain Linking
```powershell
# Dry run först
python scripts/automate_domain_linking.py

# Applicera sedan
python scripts/automate_domain_linking.py --live
```

## 🔧 Troubleshooting

### Problem: "Connection failed" i Self-Serve Analytics

**Lösning:**
```powershell
# 1. Aktivera Managed Identity
az purview account update --name prviewacc --resource-group purview --mi-system-assigned

# 2. Kör konfiguration
python scripts/configure_purview_fabric_analytics.py

# 3. Ge permissions manuellt i Fabric Portal
```

### Problem: 403 Unauthorized från Unified Catalog API

**Lösning:**
```powershell
# 1. Skapa Service Principal
python scripts/setup_unified_catalog_access.py

# 2. Tilldela Data Steward i Purview Portal:
# Data Map → Collections → Root → Role assignments
```

### Problem: Key Vault access denied

**Lösning:**
```powershell
# Ge current user access
az keyvault set-policy --name prview-kv `
  --object-id $(az ad signed-in-user show --query id -o tsv) `
  --secret-permissions get list set delete

# Ge Purview MI access
$MI_ID = az purview account show --name prviewacc --resource-group purview --query "identity.principalId" -o tsv
az keyvault set-policy --name prview-kv --object-id $MI_ID --secret-permissions get list
```

### Problem: Azure CLI not logged in

**Lösning:**
```powershell
az login
az account set --subscription 5b44c9f3-bbe7-464c-aa3e-562726a12004
az account show  # Verifiera
```

## 📞 Support & Next Steps

### Dokumentation
- [Unified Catalog API Reference](UNIFIED_CATALOG_API_GUIDE.md)
- [SDK Comparison](SDK_COMPARISON.md)
- [Complete Project Inventory](PURVIEW_PROJECT_COMPLETE_INVENTORY.md)

### Microsoft Learn Resources
- [Unified Catalog Overview](https://learn.microsoft.com/en-us/purview/unified-catalog)
- [Self-Serve Analytics](https://learn.microsoft.com/en-us/purview/unified-catalog-self-serve-analytics)
- [Scan Credentials](https://learn.microsoft.com/en-us/purview/data-map-data-scan-credentials)
- [Data Quality](https://learn.microsoft.com/en-us/purview/data-quality)

### Contact
- Project Repository: healthcare-analytics
- Azure Subscription: 5b44c9f3-bbe7-464c-aa3e-562726a12004
- Purview Account: prviewacc.purview.azure.com

---

**Uppdaterad:** 2026-04-22  
**Version:** 1.0.0  
**Status:** Production Ready ✅
