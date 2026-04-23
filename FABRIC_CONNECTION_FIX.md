# 🔧 FIX: Purview Unified Catalog "Connection Failed" - Fabric OneLake

## ❌ Problem Summary

Din Purview Unified Catalog Self-Serve Analytics visar **"Connection failed"** när du försöker ansluta till Fabric OneLake.

**Screenshot details:**
- **Storage type:** Fabric
- **Location:** `https://onelake.dfs.fabric.microsoft.com/DataGovernenee/DEH.Lakehouse/Files/DEH`
- **Error:** Test connection failed (röd text)
- **Last saved:** 01/22/2025, 12:32 PM

**Root causes identified:**
1. ✅ **Azure CLI fungerar** - `az account show` lyckades
2. ❌ **Purview Managed Identity EJ aktiverad** - Kräver manuell aktivering
3. ❌ **OneLake 403 Access Denied** - Ingen access till Fabric workspace
4. ⚠️ **Workspace-namn stavfel?** - "DataGovernenee" → "DataGovernance"?
5. ❌ **Saknar permissions** - Kan inte ändra Purview via Azure CLI

## ✅ Lösning: 4 Manuella Steg

### 📍 STEG 1: Aktivera Purview Managed Identity (Azure Portal)

**Du måste göra detta manuellt eftersom Azure CLI ger 403:**

1. **Öppna Azure Portal:** https://portal.azure.com

2. **Navigera till Purview:**
   - Resource Groups → `purview`
   - Klicka på: `prviewacc`

3. **Aktivera Managed Identity:**
   - Vänster meny → **Identity**
   - Under **System assigned** tab:
     - Status: **On**
     - Klicka **Save**
     - Vänta på att det sparas (~30 sekunder)

4. **Kopiera Principal ID:**
   - Efter save visar sidan **Object (principal) ID**
   - Kopiera detta GUID (behövs för steg 2)
   - Exempel: `a1b2c3d4-e5f6-7890-abcd-ef1234567890`

**Verifiera att det fungerade:**
```powershell
az resource show `
  --resource-group purview `
  --name prviewacc `
  --resource-type "Microsoft.Purview/accounts" `
  --query identity.principalId -o tsv
```
Om detta ger ett GUID (inte error) = SUCCESS! ✅

---

### 📍 STEG 2: Ge Purview Access till Fabric Workspace

**Du måste lägga till Purview som Contributor i Fabric:**

1. **Öppna Fabric Portal:** https://app.fabric.microsoft.com

2. **Hitta rätt workspace:**
   - Leta efter workspace-namn (troligen "DataGovernance" eller "DataGovernenee")
   - Workspace ID ska vara: `afda4639-34ce-4ee9-a82f-ab7b5cfd7334`
   - **VIKTIGT:** Notera det EXAKTA namnet som visas i Fabric

3. **Manage Access:**
   - Öppna workspace
   - Klicka på **⚙️ Settings** eller **Manage access** (brukar vara högst upp till höger)
   - Klicka **+ Add people or groups**

4. **Lägg till Purview MI:**
   - I sökfältet: Skriv `prviewacc`
   - Om inte syns: Klistra in Principal ID från steg 1
   - Välj rätt result (ska vara type "Service Principal")
   - **Roll:** Välj **Contributor** (minimum)
   - Klicka **Add**

5. **Verifiera:**
   - Under "Members" eller "Access" lista
   - Du ska nu se: `prviewacc` eller `prviewacc (Service Principal)`
   - Role: Contributor eller Admin

**Test efter steg 2:**
Kör detta igen (ska nu ge 200 OK istället för 403):
```powershell
python scripts/test_onelake_connection.py
```

---

### 📍 STEG 3: Fixa OneLake URL i Purview

**Problem:** URL i screenshot kan ha fel workspace-namn ("DataGovernenee")

**Rätt URL format:**

Du har 2 alternativ:

#### **Alternativ A: GUID-baserad URL (REKOMMENDERAD)**

```
https://onelake.dfs.fabric.microsoft.com/afda4639-34ce-4ee9-a82f-ab7b5cfd7334/2960eef0-5de6-4117-80b1-6ee783cdaeec/Files/DEH
```

**Fördelar:**
- ✅ Fungerar även om workspace/lakehouse byter namn
- ✅ Säkrare vid refactoring
- ✅ Workspace-ID och Lakehouse-ID är stabila

#### **Alternativ B: Namn-baserad URL**

```
https://onelake.dfs.fabric.microsoft.com/{WorkspaceName}/DEH.Lakehouse/Files/DEH
```

**Ersätt {WorkspaceName}:** med EXAKT namn från Fabric Portal (steg 2.2)

Exempel:
```
https://onelake.dfs.fabric.microsoft.com/DataGovernance/DEH.Lakehouse/Files/DEH
```

**Fördelar:**
- ✅ Lättare att läsa
- ✅ Mer user-friendly i logs

**OBS:** Om workspace heter "DataGovernenee" (med typo), använd det!

---

### 📍 STEG 4: Uppdatera Configuration i Purview

**Nu när MI är aktiverad och har access, uppdatera Purview:**

#### **Via Purview Portal (ENKLAST):**

1. **Öppna Purview Portal:** https://web.purview.azure.com

2. **Välj account:** `prviewacc`

3. **Navigera:**
   - Vänster meny → **Unified Catalog**
   - Välj → **Solution integrations**
   - Klicka på → **Self-serve analytics**

4. **Configure Storage:**
   - Under "Storage Configuration" section
   - Klicka **Edit** eller **⚙️ Configure**

5. **Uppdatera URL:**
   - **Storage type:** Fabric (behåll som är)
   - **Location URL:** 
     - Radera nuvarande: `...DataGovernenee...`
     - Klistra in ny: (från steg 3, alternativ A eller B)
   - **Authentication method:** System assigned managed identity (default)
   - Klicka **Save**

6. **Test Connection:**
   - Klicka **Test connection** knapp
   - Vänta 5-10 sekunder
   - Förväntat resultat: ✅ **"Connection successful"** (grön text)

**Om test misslyckas:**
- Gå tillbaka till steg 2 (verifiera Fabric permissions)
- Testa alternativ URL (A vs B)
- Kör `python scripts/test_onelake_connection.py` igen

#### **Via Python Script (ALTERNATIV):**

```powershell
# Efter steg 1-2 är klara
python scripts/configure_purview_fabric_analytics.py
```

Detta script kommer nu att:
- ✅ Verifiera MI är aktiverad
- ✅ Testa Fabric connection
- ✅ Konfigurera rätt URL automatiskt
- ✅ Test connection

---

## 🧪 Verifiering

### Test 1: OneLake Connectivity
```powershell
cd c:\code\healthcare-analytics\healthcare-analytics
python scripts/test_onelake_connection.py
```

**Förväntat output:**
```
✅ Success - Found X items
📁 Contents:
   📁 DEH (... bytes)
   📄 some_file.csv (... bytes)
```

### Test 2: Purview Portal
1. Öppna Purview Portal
2. Unified Catalog → Solution integrations → Self-serve analytics
3. Storage Configuration visar:
   - Status: ✅ Connected
   - Last test: Recent timestamp (e.g., 01/22/2025, 1:30 PM)
   - No error messages

### Test 3: Query Test
I Purview Self-Serve Analytics query editor:
```sql
SELECT * FROM information_schema.tables LIMIT 5;
```

Ska visa tabeller från DEH lakehouse.

---

## 🚨 Troubleshooting

### Problem: Managed Identity får inte aktiveras

**Error:** "You don't have permissions to update this resource"

**Lösning:**
1. Be din Azure admin att ge dig **Contributor** eller **Owner** role på Purview resource group
2. Eller be admin att aktivera MI åt dig:
   ```powershell
   az resource update `
     --resource-group purview `
     --name prviewacc `
     --resource-type "Microsoft.Purview/accounts" `
     --set identity.type=SystemAssigned
   ```

### Problem: Kan inte hitta Purview i Fabric workspace access

**Error:** "prviewacc" inte synlig i Fabric "Add people or groups"

**Lösning:**
1. Använd Principal ID istället för namn
2. Gå tillbaka till Azure Portal → Purview → Identity
3. Kopiera **Object (principal) ID**
4. Klistra in detta i Fabric search field
5. Service Principal ska nu visas

### Problem: 403 kvarstår efter steg 1-2

**Möjliga causes:**

#### Cause 1: Fel workspace ID
```powershell
# Verifiera workspace ID i Fabric Portal
# Workspace Settings → Properties → Workspace ID
# Ska matcha: afda4639-34ce-4ee9-a82f-ab7b5cfd7334
```

#### Cause 2: Fel lakehouse ID
```powershell
# Verifiera lakehouse ID
# Öppna DEH lakehouse → Settings → Properties
# Lakehouse ID ska matcha: 2960eef0-5de6-4117-80b1-6ee783cdaeec
```

#### Cause 3: Token cache issue
```powershell
# Rensa Azure CLI token cache
az account clear
az login

# Testa igen
python scripts/test_onelake_connection.py
```

### Problem: 404 Not Found

**Möjliga causes:**

#### Cause 1: Lakehouse existerar inte
- Gå till Fabric Portal
- Öppna workspace
- Verifiera att "DEH" lakehouse finns
- Om saknas: Skapa lakehouse eller använd rätt lakehouse-namn

#### Cause 2: Files/DEH folder saknas
- Öppna DEH lakehouse i Fabric
- Kontrollera att `Files/DEH` folder existerar
- Om saknas: Skapa folder eller använd annan path (t.ex. `Files/` endast)

#### Cause 3: Fel path syntax
Testa olika paths:
```powershell
# Test 1: Root Files
https://onelake.dfs.fabric.microsoft.com/.../Files

# Test 2: Tables instead
https://onelake.dfs.fabric.microsoft.com/.../Tables

# Test 3: Specific folder
https://onelake.dfs.fabric.microsoft.com/.../Files/bronze
```

### Problem: "Connection successful" men inga tabeller visas

**Lösning:**
1. Verifiera att DEH lakehouse innehåller data:
   - Fabric Portal → DEH lakehouse
   - Kolla Tables och Files sections
   - Om tom: Ladda upp data först

2. Kör data sync i Purview:
   - Purview Portal → Data Map
   - Register new source → OneLake
   - Scan lakehouse

3. Vänta 5-10 minuter på indexering

---

## 📊 Configuration Summary

### Aktuell Configuration

| Parameter | Värde | Status |
|-----------|-------|--------|
| **Purview Account** | prviewacc | ✅ Exists |
| **Resource Group** | purview | ✅ Exists |
| **Subscription** | 5b44c9f3-bbe7-464c-aa3e-562726a12004 | ✅ Active |
| **Managed Identity** | ⚠️ Not enabled | ❌ **FIX IN STEG 1** |
| **Fabric Workspace** | DataGovernance (?) | ⚠️ Verify name |
| **Workspace ID** | afda4639-34ce-4ee9-a82f-ab7b5cfd7334 | ✅ Known |
| **Lakehouse Name** | DEH | ✅ Known |
| **Lakehouse Gold ID** | 2960eef0-5de6-4117-80b1-6ee783cdaeec | ✅ Known |
| **OneLake URL** | Wrong (DataGovernenee) | ❌ **FIX IN STEG 3** |
| **Purview MI Access** | None | ❌ **FIX IN STEG 2** |
| **Azure CLI** | Logged in | ✅ Working |

### Rätt Configuration (Efter Fix)

| Parameter | Correct Value |
|-----------|---------------|
| **Managed Identity** | Enabled (SystemAssigned) |
| **MI Principal ID** | (från Azure Portal) |
| **Fabric Workspace Permission** | Contributor role |
| **OneLake URL (Option A)** | `https://onelake.dfs.fabric.microsoft.com/afda4639-34ce-4ee9-a82f-ab7b5cfd7334/2960eef0-5de6-4117-80b1-6ee783cdaeec/Files/DEH` |
| **OneLake URL (Option B)** | `https://onelake.dfs.fabric.microsoft.com/{ActualWorkspaceName}/DEH.Lakehouse/Files/DEH` |
| **Authentication** | System assigned managed identity |

---

## 🎯 Quick Reference

### Alla Commands i ett Block

```powershell
# 1. Verifiera Azure login
az account show

# 2. Test OneLake connectivity
cd c:\code\healthcare-analytics\healthcare-analytics
python scripts/test_onelake_connection.py

# 3. Verifiera MI aktiverad (efter manuellt steg 1)
az resource show `
  --resource-group purview `
  --name prviewacc `
  --resource-type "Microsoft.Purview/accounts" `
  --query identity.principalId -o tsv

# 4. Konfigurera Purview (efter alla manuella steg)
python scripts/configure_purview_fabric_analytics.py

# 5. Health check
python scripts/purview_monitoring.py
```

### URLs du behöver

| URL | Purpose |
|-----|---------|
| https://portal.azure.com | Enable Purview MI (steg 1) |
| https://app.fabric.microsoft.com | Add Purview to workspace (steg 2) |
| https://web.purview.azure.com | Update storage config (steg 4) |

---

## 📚 Next Steps (Efter Fix)

När connection fungerar:

### 1. Setup Data Quality Scanning
```powershell
python scripts/configure_purview_scan_credentials.py
```

### 2. Create Data Products
```powershell
python scripts/unified_catalog_examples.py
```

### 3. Setup Monitoring
```powershell
# One-time check
python scripts/purview_monitoring.py

# Continuous monitoring (Ctrl+C to stop)
python scripts/purview_monitoring.py --continuous --interval 300
```

### 4. Automate Domain Linking
```powershell
# Dry-run first
python scripts/automate_domain_linking.py

# Execute if looks good
python scripts/automate_domain_linking.py --live
```

### 5. Setup Service Principal (for API access)
```powershell
python scripts/setup_unified_catalog_access.py
```

---

## 📖 Documentation

**Microsoft Learn References:**
- Self-Serve Analytics: https://learn.microsoft.com/en-us/purview/unified-catalog-self-serve-analytics
- OneLake Overview: https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview
- Managed Identities: https://learn.microsoft.com/en-us/entra/identity/managed-identities-azure-resources/overview

**Local Documentation:**
- Complete Setup Guide: `PURVIEW_COMPLETE_SETUP.md`
- Manual Fix Guide: `scripts/fix_fabric_connection_manual.md`
- Project Inventory: `PURVIEW_PROJECT_COMPLETE_INVENTORY.md`

---

## ✅ Checklist

Print this och checka av:

- [ ] **STEG 1:** Purview Managed Identity enabled (Azure Portal)
- [ ] **STEG 1:** Principal ID kopie rad
- [ ] **STEG 2:** Öppnat Fabric workspace
- [ ] **STEG 2:** Verifierat workspace name (NOT "DataGovernenee")
- [ ] **STEG 2:** Lagt till Purview MI som Contributor
- [ ] **STEG 2:** Verifierat i Members list
- [ ] **STEG 3:** Testat `test_onelake_connection.py` - SUCCESS ✅
- [ ] **STEG 4:** Uppdaterat URL i Purview Portal
- [ ] **STEG 4:** Test connection - SUCCESS ✅
- [ ] **Verify:** SQL query fungerar i Self-Serve Analytics
- [ ] **Verify:** Tabeller från DEH synliga
- [ ] **Next:** Kört health monitoring
- [ ] **Next:** Setup data quality scanning

---

**Created:** 2026-04-22  
**Status:** READY FOR EXECUTION 🚀  
**Estimated time:** 15-20 minuter (alla 4 steg)

**Support:** Om problem kvarstår efter alla steg, kontakta Azure support eller Microsoft Purview team.
