# Fixa Fabric Self-Serve Analytics Connection - Manuell Guide

## Problem
Din Purview Unified Catalog visar "Connection failed" för Fabric storage:
- **URL:** `https://onelake.dfs.fabric.microsoft.com/DataGovernenee/DEH.Lakehouse/Files/DEH`
- **Storage type:** Fabric
- **Error:** "Test connection failed"

## Root Causes
1. ❌ Purview Managed Identity inte aktiverad
2. ❌ Fabric workspace-namn fel stavat ("DataGovernenee" → "DataGovernance"?)
3. ❌ Purview MI saknar permissions på Fabric workspace
4. ❌ Fel lakehouse path/struktur

## Lösning: 5 Steg

### ⚡ STEG 1: Aktivera Purview Managed Identity

**Via Azure Portal:**
1. Öppna [Azure Portal](https://portal.azure.com)
2. Navigera till: **Resource Group** → `purview` → **prviewacc**
3. Vänster meny: **Identity**
4. Under "System assigned":
   - Status: **On**
   - Klicka **Save**
5. Kopiera **Object (principal) ID** (behövs för steg 3)

**Via Azure CLI (alternativ):**
```powershell
# Korrekt kommando för Purview Managed Identity
az resource update `
  --resource-group purview `
  --name prviewacc `
  --resource-type "Microsoft.Purview/accounts" `
  --set identity.type=SystemAssigned

# Hämta Principal ID
az resource show `
  --resource-group purview `
  --name prviewacc `
  --resource-type "Microsoft.Purview/accounts" `
  --query identity.principalId -o tsv
```

### ⚡ STEG 2: Verifiera Fabric Workspace & Lakehouse

**Kontrollera workspace-namn:**
1. Öppna [Microsoft Fabric Portal](https://app.fabric.microsoft.com)
2. Navigera till din workspace
3. Verifiera att namnet är korrekt (skärmdumpen visar "DataGovernenee" - är detta rätt stavat?)
4. Notera workspace-namn exakt som det visas

**Verifiera lakehouse:**
1. Öppna workspace
2. Leta efter lakehouse "DEH" 
3. Klicka på DEH lakehouse
4. Högerklicka → **Properties**
5. Kopiera exakt path

### ⚡ STEG 3: Ge Purview MI Access till Fabric Workspace

**I Fabric Portal:**
1. Öppna workspace (DataGovernenee eller korrekt namn)
2. Klicka på **⚙️ (Settings)** eller **Workspace settings**
3. Välj **Manage access** / **Access**
4. Klicka **+ Add people or groups**
5. Sök efter: `prviewacc` (Purview Managed Identity)
   - Om inte syns, använd Object ID från steg 1
6. Välj roll: **Contributor** eller **Admin**
7. Klicka **Add**

**Verifiera access:**
```powershell
# Testa OneLake access med Purview credentials
az login --identity  # Om kör från VM med MI

# Lista OneLake innehåll
az storage fs directory list `
  --name "afda4639-34ce-4ee9-a82f-ab7b5cfd7334" `
  --file-system "2960eef0-5de6-4117-80b1-6ee783cdaeec" `
  --account-name onelake
```

### ⚡ STEG 4: Konfigurera Korrekt Storage URL

**Rätt format för OneLake URL:**

Det finns 2 möjliga format:

**Format 1: Workspace Name-baserad (REKOMMENDERAD)**
```
https://onelake.dfs.fabric.microsoft.com/{WorkspaceName}/{LakehouseName}.Lakehouse/Files/{FolderPath}
```

Exempel:
```
https://onelake.dfs.fabric.microsoft.com/DataGovernance/DEH.Lakehouse/Files/DEH
```
⚠️ Notera: "DataGovernance" istället för "DataGovernenee" (om det är stavfel)

**Format 2: GUID-baserad (SÄKRARE)**
```
https://onelake.dfs.fabric.microsoft.com/{WorkspaceID}/{LakehouseID}/Files/{FolderPath}
```

Exempel med dina GUIDs:
```
https://onelake.dfs.fabric.microsoft.com/afda4639-34ce-4ee9-a82f-ab7b5cfd7334/2960eef0-5de6-4117-80b1-6ee783cdaeec/Files/DEH
```

**Vilket format ska du använda?**
- Om workspace/lakehouse kan byta namn → Använd Format 2 (GUID)
- Om namn är stabila → Använd Format 1 (lättare att läsa)

### ⚡ STEG 5: Uppdatera Purview Configuration

**Via Purview Portal:**
1. Öppna [Purview Portal](https://web.purview.azure.com)
2. Välj account: **prviewacc**
3. Navigera: **Unified Catalog** → **Solution integrations**
4. Välj: **Self-serve analytics**
5. Under "Configure storage":
   - Klicka **Edit**
   - **Storage type:** Fabric (behåll)
   - **Location URL:** Uppdatera till korrekt URL (se ovan)
   - **Authentication:** System assigned managed identity (default)
   - Klicka **Save**
6. Klicka **Test connection**
7. Vänta på resultat (borde nu visa ✅)

**Via PowerShell/API (alternativ):**
```powershell
# Kör detta EFTER steg 1-3 är klara
cd c:\code\healthcare-analytics\healthcare-analytics
python scripts/configure_purview_fabric_analytics.py
```

## Felsökning

### Problem: "Connection failed" kvarstår

**Check 1: Managed Identity aktiverad?**
```powershell
az resource show `
  --resource-group purview `
  --name prviewacc `
  --resource-type "Microsoft.Purview/accounts" `
  --query identity
```
Förväntat output: `{"principalId": "...", "type": "SystemAssigned"}`

**Check 2: Fabric permissions?**
- Gå till Fabric workspace → Manage access
- Verifiera att Purview MI finns i listan
- Roll måste vara minst "Contributor"

**Check 3: URL syntax?**
- Testa format 2 (GUID-baserad) om format 1 misslyckas
- Dubbelkolla att "/Files/" finns i pathen
- Dubbelkolla att lakehouse-namnet slutar med ".Lakehouse"

**Check 4: Lakehouse struktur?**
```powershell
# Testa OneLake direkt
python -c "
import requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
token = cred.get_token('https://storage.azure.com/.default').token
headers = {'Authorization': f'Bearer {token}'}

url = 'https://onelake.dfs.fabric.microsoft.com/afda4639-34ce-4ee9-a82f-ab7b5cfd7334/2960eef0-5de6-4117-80b1-6ee783cdaeec/Files'
params = {'resource': 'filesystem', 'recursive': 'false'}

r = requests.get(url, headers=headers, params=params)
print(f'Status: {r.status_code}')
if r.status_code == 200:
    print('Files:', [p['name'] for p in r.json().get('paths', [])])
else:
    print('Error:', r.text)
"
```

### Problem: 403 Unauthorized

**Lösning:**
- Managed Identity inte aktiverad → Gå tillbaka till steg 1
- Fabric permissions saknas → Gå tillbaka till steg 3
- Fel token scope → Kontakta support

### Problem: 404 Not Found

**Lösning:**
- Fel workspace ID/namn → Verifiera i Fabric Portal
- Fel lakehouse ID/namn → Kontrollera lakehouse properties
- Fel path → Testa olika paths (Files/, Tables/, etc.)

## Snabb-Test Script

Kör detta för att testa varje komponent:

```powershell
# Test 1: Azure CLI login
Write-Host "Test 1: Azure CLI"
az account show --query name -o tsv

# Test 2: Purview Managed Identity
Write-Host "`nTest 2: Purview MI"
az resource show `
  --resource-group purview `
  --name prviewacc `
  --resource-type "Microsoft.Purview/accounts" `
  --query identity.principalId -o tsv

# Test 3: Key Vault access
Write-Host "`nTest 3: Key Vault"
az keyvault secret list --vault-name prview-kv --query "[].name" -o tsv

# Test 4: OneLake access
Write-Host "`nTest 4: OneLake"
python scripts/test_onelake_connection.py

# Test 5: Purview API
Write-Host "`nTest 5: Purview API"
python scripts/configure_purview_fabric_analytics.py
```

## Automatisk Fix (Efter manuella steg)

När steg 1-3 är klara manuellt, kör:

```powershell
# Kör master setup (hoppar över misslyckade steg)
python scripts/master_setup.py --step 5

# Eller kör bara Fabric-konfiguration
python scripts/configure_purview_fabric_analytics.py
```

## Verifiering

När allt är klart:

1. **Purview Portal:**
   - Unified Catalog → Solution integrations → Self-serve analytics
   - Status: ✅ Connection successful
   - Last test: Recent timestamp

2. **Test query:**
   ```sql
   SELECT * FROM information_schema.tables LIMIT 5
   ```

3. **Lakehouse data synlig:**
   - Purview ska kunna läsa metadata från DEH lakehouse
   - Self-serve analytics ska visa tabeller och data

## Next Steps

Efter fix:
- ✅ Run `python scripts/purview_monitoring.py` för health check
- ✅ Setup data quality scans på OneLake data
- ✅ Skapa data products baserat på lakehouse tables
- ✅ Konfigurera lineage mellan Fabric och Purview

---

**Support:**
- Microsoft Learn: https://learn.microsoft.com/en-us/purview/unified-catalog-self-serve-analytics
- Fabric OneLake: https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview

**Created:** 2026-04-22
**Status:** Ready for execution ⚡
