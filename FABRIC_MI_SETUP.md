# FABRIC WORKSPACE - MI CONFIGURATION

## 🎯 Mål
Ge `mi-purview` access till Fabric workspace så Purview kan scanna OneLake.

---

## 📋 MANUELLA STEG (Krävs om du inte har Workspace Admin)

### Steg 1: Öppna Fabric Portal
1. Gå till: https://app.fabric.microsoft.com
2. Logga in som användare med **Workspace Admin** rättigheter

### Steg 2: Navigera till Workspace
1. Hitta workspace: **Healthcare Analytics** (eller sök på ID: `afda4639-34ce-4ee9-a82f-ab7b5cfd7334`)
2. Klicka på workspace-namnet

### Steg 3: Lägg till Managed Identity
1. Klicka **Settings** (kugghjulet) → **Manage access**
2. Klicka **+ Add people or groups**
3. Sök efter: `mi-purview`
   - **Eller** använd Principal ID: `a1110d1d-6964-43c4-b171-13379215123a`
4. Välj roll: **Member** (eller **Contributor**)
5. Klicka **Add**

### Steg 4: Verifiera
Kör verifieringsscript:
```powershell
python scripts/verify_fabric_access.py
```

---

## 🤖 ALTERNATIV: PowerShell med Fabric Admin

Om du har Fabric Tenant Admin eller Workspace Admin via Azure AD:

```powershell
# Använd Fabric REST API
$token = (az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type" = "application/json"
}

$body = @{
    principal = @{
        id = "a1110d1d-6964-43c4-b171-13379215123a"
        type = "ServicePrincipal"
    }
    role = "Member"
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/afda4639-34ce-4ee9-a82f-ab7b5cfd7334/roleAssignments" `
    -Headers $headers `
    -Body $body
```

---

## 🔍 EFTER KONFIGURATION

### Konfigurera Purview Scan

1. **Azure Portal** → Purview Account (`prviewacc`)
2. **Data Map** → **Sources**
3. **Register** → **Microsoft Fabric OneLake**
4. Fyll i:
   - **Name**: Healthcare Analytics Lakehouse
   - **Workspace ID**: `afda4639-34ce-4ee9-a82f-ab7b5cfd7334`
   - **Authentication**: Managed Identity
   - **Managed Identity**: `mi-purview`
5. **Register**
6. **New Scan** → Välj tables att scanna
7. **Run scan**

### Verifiera Scan Results

Efter scan är klar (5-10 min):
```powershell
python scripts/verify_purview_onelake.py
```

Förväntat resultat:
- ✅ Purview hittar alla tables i Lakehouse Gold
- ✅ Schema och kolumner extraherade
- ✅ Lineage synlig mellan tables
- ✅ Glossary terms kan appliceras på OneLake-data

---

## 📊 CURRENT STATUS

- **MI Principal ID**: `a1110d1d-6964-43c4-b171-13379215123a`
- **MI Name**: `mi-purview`
- **Workspace ID**: `afda4639-34ce-4ee9-a82f-ab7b5cfd7334`
- **Lakehouse Gold ID**: `2960eef0-5de6-4117-80b1-6ee783cdaeec`
- **Status**: ⏳ Awaiting manual configuration

---

## ❓ TROUBLESHOOTING

### "Cannot find mi-purview when searching"
- Använd Principal ID istället: `a1110d1d-6964-43c4-b171-13379215123a`
- Eller full Object ID om den skiljer sig

### "Already has access"
- Verifiera att rollen är **Member** eller **Contributor** (inte bara Viewer)
- Vänta 5-10 minuter för permission propagation

### "Purview scan fails with 403"
- Kontrollera att MI har lagts till korrekt i workspace
- Verifiera att MI är kopplad till Purview account (ska vara automatiskt)
- Testa OneLake access med verifieringsscript

---

## ✅ SUCCESS CRITERIA

När allt är konfigurerat ska följande fungera:

1. ✅ `python scripts/verify_fabric_access.py` → Status: Connected
2. ✅ Purview scan completes without errors
3. ✅ Tables visible in Purview Data Catalog
4. ✅ Glossary terms applicerbara på OneLake assets
5. ✅ Lineage mellan Fabric notebooks och tables synlig

---

**Next**: Efter MI är konfigurerad, kör `python scripts/verify_purview_complete.py` för full status!
