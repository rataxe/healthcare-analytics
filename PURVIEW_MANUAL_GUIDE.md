# PURVIEW MANUAL SETUP GUIDE
## Vad som faktiskt behöver göras (ärlig bedömning)

## ✅ Vad som FUNGERAR via API:
- **188 glossary termer** finns redan
- **4 data products** finns som entities
- **12 custom classifications** finns
- **7 collections** finns

## ❌ Vad som MÅSTE göras MANUELLT:

### 1. GOVERNANCE DOMAINS (20 min) - MÅSTE göras i Portal UI
**Varför manuellt?** REST API:t för governance domains fungerar inte (404/403 på alla endpoints)

**Steg:**
1. Gå till: https://purview.microsoft.com/governance/domains
2. Klicka **"+ New domain"** 
3. Skapa dessa 4 domains:

#### Domain 1: Klinisk Vård
- **Name:** Klinisk Vård
- **Description:** Klinisk vård och patientdata
- Efter skapandet, lägg till **data product**: "Klinisk Patientanalys"

#### Domain 2: Forskning & Genomik
- **Name:** Forskning & Genomik
- **Description:** Forskning, genomik och precision medicine
- Efter skapandet, lägg till **data product**: "BrainChild Barncancerforskning"

#### Domain 3: Interoperabilitet & Standarder
- **Name:** Interoperabilitet & Standarder
- **Description:** OMOP, FHIR och datautbyte
- Efter skapandet, lägg till **data product**: "OMOP Forskningsdata"

#### Domain 4: Data & Analytics
- **Name:** Data & Analytics
- **Description:** ML, analytics och feature stores
- Efter skapandet, lägg till **data product**: "ML Feature Store"

---

### 2. SQL LINEAGE CONFIGURATION (15 min) - MÅSTE köras i Azure Portal

**Varför manuellt?** Azure CLI subprocess-integration fungerar inte i scripten

#### Alternativ A: Azure Portal Query Editor (ENKLAST)
1. **Gå till:** https://portal.azure.com
2. **Navigera till:** SQL Database → `HealthcareAnalyticsDB`
3. **Välj:** "Query editor" från vänstermenyn
4. **Logga in med:** Azure Active Directory authentication
5. **Kör detta SQL-script:**

```sql
-- === PURVIEW LINEAGE SETUP SCRIPT ===

-- 1. Create Master Key (om den inte finns)
IF NOT EXISTS (
    SELECT * FROM sys.symmetric_keys 
    WHERE name = '##MS_DatabaseMasterKey##'
)
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'PurviewLineage2026!Str0ngP@ssw0rd#Secure$';
    PRINT 'Master key created';
END
ELSE
    PRINT 'Master key already exists';
GO

-- 2. Create prviewacc user
IF NOT EXISTS (
    SELECT * FROM sys.database_principals 
    WHERE name = 'prviewacc' AND type = 'E'
)
BEGIN
    CREATE USER [prviewacc] FROM EXTERNAL PROVIDER;
    PRINT 'User prviewacc created';
END
ELSE
    PRINT 'User prviewacc already exists';
GO

-- 3. Grant db_owner to prviewacc
ALTER ROLE db_owner ADD MEMBER [prviewacc];
PRINT 'Granted db_owner to prviewacc';
GO

-- 4. Create mi-purview user
IF NOT EXISTS (
    SELECT * FROM sys.database_principals 
    WHERE name = 'mi-purview' AND type = 'E'
)
BEGIN
    CREATE USER [mi-purview] FROM EXTERNAL PROVIDER;
    PRINT 'User mi-purview created';
END
ELSE
    PRINT 'User mi-purview already exists';
GO

-- 5. Grant db_owner to mi-purview
ALTER ROLE db_owner ADD MEMBER [mi-purview];
PRINT 'Granted db_owner to mi-purview';
GO

-- 6. Verify configuration
SELECT 
    'Users' as Check_Type,
    name as Name, 
    type_desc as Type
FROM sys.database_principals
WHERE name IN ('prviewacc', 'mi-purview');

SELECT 
    'Roles' as Check_Type,
    rp.name as Role_Name,
    mp.name as Member_Name
FROM sys.database_role_members rm
JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
WHERE mp.name IN ('prviewacc', 'mi-purview');
```

#### Alternativ B: Azure Data Studio
1. Anslut till: `sql-hca-demo.database.windows.net`
2. Database: `HealthcareAnalyticsDB`
3. Authentication: **Azure Active Directory**
4. Kör samma SQL-script som ovan

---

### 3. DATA SOURCES - REGISTRERA OCH SCANNA (30 min)

#### 3a. Registrera Azure SQL Database
1. **Purview Portal:** https://purview.microsoft.com → Data Map → Sources
2. **Klicka:** Register
3. **Välj:** Azure SQL Database
4. **Fyll i:**
   - Name: `sql-hca-demo-healthcaredb`
   - Server: `sql-hca-demo.database.windows.net`
   - Database: `HealthcareAnalyticsDB`
   - Collection: `sql-databases`
   - Managed Identity: `prviewacc`
5. **Enable:** "Lineage extraction" (efter SQL-setup ovan är klart)
6. **Register**

#### 3b. Registrera Fabric OneLake (Healthcare Analytics)
1. **Purview Portal:** Data Map → Sources
2. **Klicka:** Register
3. **Välj:** Microsoft Fabric
4. **Fyll i:**
   - Name: `fabric-healthcare-analytics`
   - Tenant ID: `71c4b6d5-0065-4c6c-a125-841a582754eb`
   - Workspace ID: `afda4639-34ce-4ee9-a82f-ab7b5cfd7334`
   - Collection: `fabric-analytics`
5. **Register**

**OBS:** `mi-purview` måste ha **Contributor** eller **Admin** role på Fabric workspace!
Verifiera i: https://app.powerbi.com → Workspace settings → Access

#### 3c. Registrera Fabric OneLake (BrainChild)
1. **Purview Portal:** Data Map → Sources
2. **Klicka:** Register
3. **Välj:** Microsoft Fabric
4. **Fyll i:**
   - Name: `fabric-brainchild`
   - Tenant ID: `71c4b6d5-0065-4c6c-a125-841a582754eb`
   - Workspace ID: [BEHÖVER WORKSPACE ID FÖR BRAINCHILD]
   - Collection: `fabric-brainchild`
5. **Register**

#### 3d. Kör Scans
För varje registrerad data source:
1. Gå till data source
2. **New scan**
3. Välj authentication (Managed Identity för SQL, Service Principal för Fabric)
4. **Run scan**
5. Vänta på att scan blir klar (5-30 min beroende på datamängd)

---

### 4. LÄNKA DATA ASSETS TILL GLOSSARY TERMER (10 min per asset)

Efter scanning är klar:
1. **Data Catalog:** Sök efter assets (tabeller, kolumner)
2. **Öppna asset**
3. **Schema tab:** Länka kolumner till glossary termer
4. **Related tab:** Länka till data products

Exempel:
- Kolumn `patient_id` → Glossary term "Patient ID"
- Kolumn `diagnosis_code` → Glossary term "ICD-10 Code"
- Tabell `omop.person` → Data product "OMOP Forskningsdata"

---

### 5. APPLICERA CLASSIFICATIONS (10 min)

För känslig data:
1. **Sök** efter tabeller med PHI/PII
2. **Edit** asset
3. **Add classification:**
   - `Swedish Personnummer` på personnummer-kolumner
   - `Patient Name PHI` på namn-kolumner
   - `SNOMED CT Code` på diagnosis-kolumner
   - Etc.

---

## VERIFIKATION

### Checklist
- [ ] **4 governance domains** syns i Portal UI (Governance → Domains)
- [ ] **4 data products** länkade till domains (Data Catalog → Data Products)
- [ ] **SQL Database registered** och scannad (Data Map → Sources)
- [ ] **Fabric workspaces registered** och scannade
- [ ] **Lineage extraction enabled** för SQL (Data source settings)
- [ ] **Glossary termer** länkade till kolumner (Data Catalog → asset → Schema)
- [ ] **Classifications** applicerade på känslig data

### Verifierings-URLs
- Governance Domains: https://purview.microsoft.com/governance/domains
- Data Products: https://purview.microsoft.com/catalog/browse/dataproducts
- Data Catalog: https://purview.microsoft.com/catalog
- Data Map Sources: https://purview.microsoft.com/datamap/sources
- Glossary: https://purview.microsoft.com/glossary

---

## VARFÖR INTE AUTOMATION?

### Vad som INTE fungerar via API:
1. **Governance Domains** - REST API returnerar 404/403, måste skapas i Portal UI
2. **Data Source Registration** - API ger 403 Unauthorized
3. **Scanning** - Kräver komplex auth och credential management
4. **Asset-to-glossary linking** - Bulk operations inte stabila via API
5. **Lineage extraction** - Kräver SQL-konfiguration som subprocess/CLI inte fungerar för

### Vad som FUNGERAR via API:
- ✅ Glossary termer skapande/uppdatering
- ✅ Custom classifications skapande
- ✅ Entity metadata läsning
- ✅ Search och discovery

---

## TIDSESTIMAT

| Aktivitet | Tid | Status |
|-----------|-----|--------|
| **Glossary termer** | 0 min | ✅ Redan klart (188 termer) |
| **Data products** | 0 min | ✅ Redan klart (4 products) |
| **Classifications** | 0 min | ✅ Redan klart (12 custom) |
| **Collections** | 0 min | ✅ Redan klart (7 collections) |
| **Governance Domains** | 20 min | ❌ Manuellt i Portal UI |
| **SQL Lineage Setup** | 15 min | ❌ SQL i Azure Portal |
| **Register Data Sources** | 30 min | ❌ Manuellt i Portal UI |
| **Run Scans** | 30-60 min | ❌ Väntetid efter start |
| **Link Assets** | 30 min | ❌ Manuell länkning |
| **Apply Classifications** | 10 min | ❌ Manuell applicering |
| **TOTALT** | **~2-3 timmar** | **50% klart** |

---

## REKOMMENDATION

**BÖRJA MED:**
1. ✅ Governance Domains (20 min) - HÖGSTA PRIORITET
2. ✅ SQL Lineage Setup (15 min) - KRITISKT för lineage
3. ✅ Register SQL Database (5 min)
4. ⏳ Run SQL scan (vänta 10-20 min)
5. ✅ Register Fabric workspaces (10 min)
6. ⏳ Run Fabric scans (vänta 20-40 min)

**EFTER SCANNING:**
7. Länka assets till glossary termer
8. Applicera classifications
9. Verifiera lineage

---

## SUPPORT

Om problem uppstår:
- **SQL Access:** Verifiera Azure AD admin är satt på SQL Server
- **Fabric Access:** Verifiera mi-purview har Contributor role på workspaces
- **Purview Permissions:** Verifiera Root Collection Admin permissions
- **Scanning errors:** Check scan logs i Purview Portal

**Script för diagnostic check:**
```powershell
python scripts/purview_reality_check.py
```
