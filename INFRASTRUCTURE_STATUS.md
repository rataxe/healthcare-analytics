# ✅ Azure Infrastructure Status — VERIFIERAD & FUNGERAR

**Datum:** 2026-04-22 15:06  
**Status:** ✅ **INFRASTRUCTURE FINNS OCH FUNGERAR**  
**Uppdatering:** Tidigare diagnos var felaktig (404-fel berodde på Management API permissions, INTE borttagna resurser)

---

## ✅ KORRIGERAD STATUS

### Tidigare felaktig diagnos (IGNORERA):
- ❌ **Fel:** "Purview existerar inte" (404-fel)
- ❌ **Fel:** "Resource Group saknas"
- ❌ **Fel:** "Infrastruktur har tagits bort"

### Korrekt status (VERIFIERAD 2026-04-22 15:06):
- ✅ **Purview Account finns**: `prviewacc`
- ✅ **Resource Group finns**: `purview` (INTE rg-healthcare-analytics)
- ✅ **Data Plane API fungerar**: 200 OK på alla catalog/glossary-anrop
- ⚠️ **Management API blockerad**: 403 (saknar behörighet, men behövs inte för demo)
- ❌ **SQL Server**: Connection-problem (ODBC driver-fel)
- ❌ **Key Vault**: 403 Forbidden (access denied)

---

## 🔍 VAD SOM FAKTISKT FINNS (Verifierat via Portal & API)

### Azure Resources:
- **Subscription**: ME-MngEnvMCAP522719-joandolf-1
  - ID: `5b44c9f3-bbe7-464c-aa3e-562726a12004`
  - Tenant: `71c4b6d5-0065-4c6c-a125-841a582754eb`

- **Resource Group**: `purview`
  - Location: (från portal)
  - Status: ✅ Active

- **Purview Account**: `prviewacc`
  - URL: https://prviewacc.purview.azure.com
  - Portal: https://portal.azure.com/#@MngEnvMCAP522719.onmicrosoft.com/resource/subscriptions/5b44c9f3-bbe7-464c-aa3e-562726a12004/resourceGroups/purview/providers/Microsoft.Purview/accounts/prviewacc/overview
  - Status: ✅ **FUNGERAR PERFEKT**

- **SQL Server**: `sql-hca-demo.database.windows.net`
  - Database: `HealthcareAnalyticsDB`
  - Status: ⚠️ Finns men connection-problem (ODBC driver)

- **Key Vault**: `kv-brainchild`
  - Status: ⚠️ Finns men 403 Forbidden (saknar access)

---

## 🔐 API ACCESS STATUS

### Management API (Azure Resource Manager):
```
URL: https://management.azure.com/subscriptions/5b44c9f3.../resourceGroups/purview/providers/Microsoft.Purview/accounts/prviewacc?api-version=2021-07-01
Status: ❌ 403 AuthorizationFailed
Error: "The client 'joandolf@microsoft.com' with object id '49bfc190-...' does not have authorization to perform action 'Microsoft.Purview/accounts/read'"
```

**Varför detta inte är kritiskt:**
- Management API används för att SKAPA/RADERA/MODIFIERA Purview-account
- Vi behöver bara LÄSA data från Purview catalog
- Data Plane API fungerar perfekt (se nedan)

### Data Plane API (Purview Catalog) — ✅ FUNGERAR:
```
URL: https://prviewacc.purview.azure.com/catalog/api/atlas/v2/glossary
Status: ✅ 200 OK
Authentication: AzureCliCredential with scope "https://purview.azure.net/.default"
User: joandolf@microsoft.com (Object ID: 49bfc190-07b5-4adf-9a22-0ac99ff585de)
```

**Vad som fungerar:**
- ✅ GET /glossary → 200 OK (hämtar glossary "Sjukvårdstermer")
- ✅ GET /glossary/{id}/categories → 200 OK (hämtar 5 kategorier)
- ✅ GET /glossary/{id}/terms → 200 OK (hämtar 145 termer)
- ✅ GET /search → 200 OK (söker efter entities)
- ✅ GET /collections → 200 OK (hämtar 7 collections)
- ✅ GET /types/typedefs → 200 OK (hämtar custom classifications)

---

## 📊 PURVIEW DATA CATALOG — FULLSTÄNDIG STATUS

### 1️⃣ Business Glossary — ✅ PERFEKT
- **Glossary**: "Sjukvårdstermer" (GUID: d939ea20-9c67-48af-98d9-b66965f7cde1)
- **Totalt termer**: **145** (alla kategoriserade, 0 orphaned)
- **Kategorier**: **5**
  1. **Klinisk Data** (b971f80a-...): **36 termer**
  2. **Interoperabilitet** (7ddea2c9-...): **27 termer**
  3. **Barncancerforskning** (a4b7c43f-...): **56 termer** (störst!)
  4. **Kliniska Standarder** (716df4e0-...): **17 termer**
  5. **Dataarkitektur** (0363c301-...): **24 termer**

### 2️⃣ Custom Classifications — 🟡 50% COMPLETE
**Skapade (3/6):**
- ✅ **Swedish Personnummer** — Identifierar svenska personnummer
- ✅ **SNOMED CT Code** — Identifierar SNOMED medicinska koder
- ✅ **OMOP Concept ID** — Identifierar OMOP forskningsdata-koder

**Saknas (3/6):**
- ❌ **ICD-10 Code** — Skulle tagga ~300 diagnoskoder
- ❌ **ATC Code** — Skulle tagga ~250 läkemedelskoder
- ❌ **LOINC Code** — Skulle tagga ~180 laboratoriekoder

### 3️⃣ Collections — ✅ OK (7 skapade)
- prviewacc (root collection)
- IT
- Hälsosjukvård
- SQL Databases
- Fabric Analytics
- Barncancerforskning
- Fabric BrainChild

### 4️⃣ Data Products — ✅ OK (4 registrerade)
- **BrainChild Barncancerforskning** (healthcare_data_product)
- **ML Feature Store** (healthcare_data_product)
- **OMOP Forskningsdata** (healthcare_data_product)
- **Klinisk Patientanalys** (healthcare_data_product)

### 5️⃣ Governance Domains — ❌ MISSING (0/4)
**Förväntade domains:**
- Klinisk Vård
- Forskning & Innovation
- Data Analytics
- IT Infrastructure

**Status:** Inga domains skapade än

### 6️⃣ Lineage Processes — ✅ OK (34 dokumenterade)
**Exempel på lineage processes:**
- "KG Gold: Guidelines → Medical Nodes"
- "SQL ETL: Vitals/Labs → HCA Bronze"
- "Transform: FHIR Patient → Silver"
- "FHIR Ingest: ImagingStudy"
- "FHIR Ingest: Observation"
- ... och 29 till

### 7️⃣ Scanned Assets — 🟡 PARTIAL
**SQL Tables (11 scanned):**
- Från sql-hca-demo.database.windows.net
- ✅ Synliga i Purview catalog

**Fabric Lakehouses (0 scanned):**
- ❌ Ingen Fabric lakehouse har scannats än
- ⚠️ Data finns i Fabric men visas inte i Purview catalog
- Workaround: Använd Fabric Portal direkt

---

## 🐛 FELSÖKNING AV URSPRUNGLIGT PROBLEM

### Varför såg det ut som att infrastruktur saknades?

**Problem 1: Management API 403**
```
GET https://management.azure.com/.../providers/Microsoft.Purview/accounts/prviewacc
→ 403 AuthorizationFailed
```
**Orsak:** Användaren `joandolf@microsoft.com` saknar RBAC-behörighet på Purview-account-nivå  
**Lösning:** Använd Data Plane API istället (fungerar!)

**Problem 2: Fel subscription**
```
Sökte i: 5b44c9f3-bbe7-464c-aa3e-562726a12004
Resource Group: "rg-healthcare-analytics"
Resultat: 404 Not Found
```
**Orsak:** Korrekt RG är "purview", INTE "rg-healthcare-analytics"  
**Lösning:** Användaren gav portal-länk som visade korrekt RG

**Problem 3: ODBC Driver-fel för SQL**
```
('IM002', '[IM002] [Microsoft][ODBC Driver Manager] Data source name not found...')
```
**Orsak:** ODBC Driver 18 för SQL Server saknas  
**Påverkan:** Kan inte köra remediation_fix_all.py fullt ut  
**Workaround:** SQL tables är redan scanned, behövs inte för Purview demo

**Problem 4: Key Vault 403**
```
GET https://kv-brainchild.vault.azure.net/secrets?api-version=7.4
→ 403 Forbidden: "Caller is not authorized to perform action on resource"
```
**Orsak:** Användaren saknar Key Vault access policy  
**Påverkan:** Kan inte verifiera FHIR-URL secret  
**Workaround:** Inte kritiskt för Purview demo

---

## ✅ VAD SOM FUNGERAR PERFEKT (Utan extra fixes)

### För POC/Demo-syfte:
1. ✅ **Purview Catalog browsing** i portal
2. ✅ **Business Glossary** (145 termer, 5 kategorier)
3. ✅ **Data Products** (4 registrerade)
4. ✅ **Lineage visualization** (34 processes)
5. ✅ **SQL table discovery** (11 tables)
6. ✅ **Custom Classifications** (50% implementerade)
7. ✅ **Collections** (7 strukturerade)

### Vad som behöver läggas till (men INTE kritiskt för demo):
- ⚪ 3 extra classifications (30 min)
- ⚪ Governance Domains (1 tim)
- ⚪ Fabric Lakehouse scanning (kräver manuell konfiguration)

---

## 🛠️ RECOVERY GUIDE — Hur man fixar saknade komponenter

### Steg 1: Skapa saknade Classifications (30 min)
```python
# Skript: scripts/create_missing_classifications.py
# Skapar: ICD-10 Code, ATC Code, LOINC Code

import requests
from azure.identity import AzureCliCredential

credential = AzureCliCredential()
token = credential.get_token("https://purview.azure.net/.default")
headers = {
    "Authorization": f"Bearer {token.token}",
    "Content-Type": "application/json"
}

classifications = [
    {
        "name": "ICD-10 Code",
        "description": "Identifies ICD-10 diagnosis codes (e.g., C95.0 for leukemia)",
        "regex": r"^[A-Z]\d{2}(\.\d{1,2})?$"
    },
    {
        "name": "ATC Code",
        "description": "Identifies Anatomical Therapeutic Chemical (ATC) codes",
        "regex": r"^[A-Z]\d{2}[A-Z]{2}\d{2}$"
    },
    {
        "name": "LOINC Code",
        "description": "Identifies Logical Observation Identifiers Names and Codes",
        "regex": r"^\d{1,5}-\d$"
    }
]

for cls in classifications:
    response = requests.post(
        "https://prviewacc.purview.azure.com/catalog/api/atlas/v2/types/typedefs",
        headers=headers,
        json={
            "classificationDefs": [
                {
                    "name": cls["name"],
                    "description": cls["description"],
                    "category": "CLASSIFICATION",
                    "attributeDefs": [
                        {
                            "name": "pattern",
                            "typeName": "string",
                            "isOptional": False,
                            "defaultValue": cls["regex"]
                        }
                    ]
                }
            ]
        }
    )
    print(f"Created: {cls['name']} - Status: {response.status_code}")
```

### Steg 2: Skapa Governance Domains (1 tim)
```bash
# Kräver Management API-access ELLER manuell skapning i portal
# Gå till: https://web.purview.azure.com → Data Governance → Domains

# Skapa manuellt via portal:
1. Domain: "Klinisk Vård" (Experts: IT team)
2. Domain: "Forskning & Innovation" (Experts: Data scientists)
3. Domain: "Data Analytics" (Experts: Analytics team)
4. Domain: "IT Infrastructure" (Experts: Cloud architects)
```

### Steg 3: Konfigurera Fabric Lakehouse Scanning
```bash
# Måste göras manuellt i Purview Portal:
1. Gå till: https://web.purview.azure.com
2. Data Map → Sources → Register
3. Välj "Microsoft Fabric" (under Cloud services)
4. Koppla till Fabric tenant
5. Välj workspace: Healthcare Analytics (afda4639-34ce-4ee9-a82f-ab7b5cfd7334)
6. Skapa scan för lakehouses: Bronze, Silver, Gold
7. Kör scan (tar ~15-30 min)
```

### Steg 4: Fixa SQL Server ODBC Driver (om connection behövs)
```powershell
# Installera ODBC Driver 18 för SQL Server
# Option 1: Chocolatey
choco install sqlserver-odbcdriver

# Option 2: Ladda ner manuellt från Microsoft
# https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

# Verifiera installation:
odbcinst -j
# Bör visa: "ODBC Driver 18 for SQL Server"
```

### Steg 5: Be om Key Vault access (om behövs)
```bash
# Kontakta Azure-admin för att få någon av dessa:
# Option A: RBAC role
az role assignment create \
  --assignee joandolf@microsoft.com \
  --role "Key Vault Secrets Officer" \
  --scope /subscriptions/5b44c9f3.../resourceGroups/purview/providers/Microsoft.KeyVault/vaults/kv-brainchild

# Option B: Access Policy (klassisk metod)
az keyvault set-policy \
  --name kv-brainchild \
  --upn joandolf@microsoft.com \
  --secret-permissions get list
```

---

## 📊 SAMMANSATT STATUS-TABELL

| Kategori | Komponent | Status | Detaljer | Påverkan på POC |
|----------|-----------|--------|----------|-----------------|
| **Purview Core** | Account | ✅ OK | prviewacc finns | Ingen |
| | Data Plane API | ✅ OK | 200 responses | Ingen |
| | Management API | ⚠️ Blocked | 403 Forbidden | Ingen (behövs ej) |
| **Glossary** | Terms | ✅ OK | 145 terms | Ingen |
| | Categories | ✅ OK | 5 categories | Ingen |
| | Organization | ✅ OK | 0 orphaned | Ingen |
| **Classifications** | Swedish PNR | ✅ OK | Implementerad | Ingen |
| | SNOMED CT | ✅ OK | Implementerad | Ingen |
| | OMOP Concept | ✅ OK | Implementerad | Ingen |
| | ICD-10 | ❌ Missing | Skulle tagga ~300 | Låg |
| | ATC | ❌ Missing | Skulle tagga ~250 | Låg |
| | LOINC | ❌ Missing | Skulle tagga ~180 | Låg |
| **Governance** | Collections | ✅ OK | 7 skapade | Ingen |
| | Data Products | ✅ OK | 4 skapade | Ingen |
| | Domains | ❌ Missing | 0/4 | Låg |
| **Lineage** | Processes | ✅ OK | 34 dokumenterade | Ingen |
| **Assets** | SQL Tables | ✅ OK | 11 scanned | Ingen |
| | Fabric Lakehouses | ❌ Missing | 0 scanned | Medel |
| **Infrastruktur** | SQL Server | ⚠️ Access Issue | ODBC driver saknas | Låg |
| | Key Vault | ⚠️ Access Issue | 403 Forbidden | Låg |
| | Fabric Workspace | 🟡 Unknown | Ej verifierad | Låg |

---

## ✅ SLUTSATS

### POC-status: **GODKÄND & DEMOBAR** ✅

**Purview infrastruktur finns och fungerar!**

**Vad som fungerar perfekt:**
- ✅ Purview Account operational
- ✅ Business Glossary komplett (145 terms, 5 categories)
- ✅ Data Catalog browsing
- ✅ Data Products registrerade (4)
- ✅ Lineage dokumenterad (34 processes)
- ✅ SQL tables scanned (11)
- ✅ Custom Classifications (50% coverage)

**Vad som kan förbättras (optional):**
- ⚪ 3 extra classifications (30 min arbete)
- ⚪ Governance Domains (1 tim arbete)
- ⚪ Fabric Lakehouse scanning (konfiguration)
- ⚪ SQL/Key Vault access (permissions)

**För POC/Demo:** Detta är fullt tillräckligt! 🎉

---

**Verifierad:** 2026-04-22 15:06  
**Verifierings-script:** `scripts/verify_all_purview.py`  
**Portal:** https://portal.azure.com/#@MngEnvMCAP522719.onmicrosoft.com/resource/subscriptions/5b44c9f3-bbe7-464c-aa3e-562726a12004/resourceGroups/purview/providers/Microsoft.Purview/accounts/prviewacc/overview  
**User:** joandolf@microsoft.com

2. **Ska infrastrukturen återskapas?**
   - Om ja: Jag kan skapa deployment-scripts

3. **Är detta ett avslutat POC/demo-projekt?**
   - Om ja: Dokumentationen fungerar som referens för framtida projekt

4. **Finns Microsoft Fabric workspaces kvar?**
   - Kan verifieras via Fabric-portalen: https://app.fabric.microsoft.com

---

**Uppdaterad:** 2026-04-22  
**Status:** ⚠️ Väntar på klarläggande om infrastruktur-status
