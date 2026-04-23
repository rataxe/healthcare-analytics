# Purview Managed Identity - Status & Guide

**Last Checked:** 2026-04-22 18:50  
**Account:** prviewacc  
**Resource Group:** purview  

---

## ❌ Current Status: NOT ENABLED

```
Error: AuthorizationFailed
Your account (joandolf@microsoft.com) lacks permissions to read Purview account via Azure CLI.
```

---

## 🎯 Why You Need It

Purview **Self-Serve Analytics** requires Managed Identity to:
- ✅ Authenticate to Fabric OneLake
- ✅ Read data from lakehouse (DEH)
- ✅ Execute SQL queries in Self-Serve Analytics

**Without MI:** Connection fails with "Connection failed" error (current state)

---

## 🔧 How to Enable (Choose One Method)

### Method 1: Azure Portal (5 minutes) ⭐ RECOMMENDED

```
Step-by-step:
1. Open: https://portal.azure.com
2. Search: prviewacc
3. Click: Identity (left menu)
4. System assigned tab:
   Status: OFF → ON
5. Click: Save
6. Copy: Principal ID (appears after save)
   Example: 12345678-abcd-1234-abcd-123456789abc
```

**Why this method?**
- ✅ Quick and visual
- ✅ No CLI permissions needed
- ✅ Immediate confirmation

### Method 2: Request Azure Admin Help

```
Email template:

Subject: Enable Purview Managed Identity

Hi Azure Admin,

I need Managed Identity enabled on our Purview account:
- Subscription: ME-MngEnvMCAP522719-joandolf-1
- Resource Group: purview
- Account: prviewacc
- Action: Enable System Assigned Managed Identity

Please send me the Principal ID after enabling.

Thank you!
```

### Method 3: PowerShell (If You Have Permissions)

```powershell
# This will fail for you (AuthorizationFailed), but for reference:
az resource update `
  --resource-group purview `
  --name prviewacc `
  --resource-type "Microsoft.Purview/accounts" `
  --set identity.type=SystemAssigned

# Get Principal ID:
az resource show `
  --resource-group purview `
  --name prviewacc `
  --resource-type "Microsoft.Purview/accounts" `
  --query identity.principalId -o tsv
```

---

## 📋 After Enabling MI - Next Steps

### Step 1: Verify MI is Enabled
```powershell
# Check via Azure Portal:
# prviewacc → Identity → System assigned: ON ✅
# Copy the Principal ID
```

### Step 2: Add MI to Fabric Workspace
```
1. Open: https://app.fabric.microsoft.com
2. Navigate to workspace: DataGovernenee (ID: afda4639-34ce-4ee9-a82f-ab7b5cfd7334)
3. Workspace Settings → Manage access
4. Click: + Add people or groups
5. Search: prviewacc (or paste Principal ID)
6. Role: Contributor
7. Click: Add
```

### Step 3: Test OneLake Connection
```powershell
cd c:\code\healthcare-analytics\healthcare-analytics
python scripts/test_onelake_connection.py
```

**Expected result:**
```
Before: ❌ Access denied (403)
After:  ✅ Success - Found X items
```

### Step 4: Update Purview Configuration
```
1. Open: https://web.purview.azure.com
2. Select account: prviewacc
3. Unified Catalog → Solution integrations → Self-serve analytics
4. Edit storage configuration:
   - Location URL: https://onelake.dfs.fabric.microsoft.com/afda4639-34ce-4ee9-a82f-ab7b5cfd7334/2960eef0-5de6-4117-80b1-6ee783cdaeec/Files/DEH
   - Authentication: System assigned managed identity
5. Click: Save
6. Click: Test connection
```

**Expected result:**
```
Before: ❌ Connection failed (red)
After:  ✅ Connection successful (green)
```

---

## 🔍 Verification Checklist

After completing all steps, verify:

### Azure Portal ✅
- [ ] prviewacc → Identity → System assigned: **ON**
- [ ] Principal ID visible and copied

### Fabric Portal ✅
- [ ] Workspace members show: prviewacc (Service Principal)
- [ ] Role: Contributor

### Terminal Test ✅
```powershell
python scripts/test_onelake_connection.py
# Should show: ✅ Success
```

### Purview Portal ✅
- [ ] Self-serve analytics → Storage status: **Connected**
- [ ] Test connection: **Successful** (green)
- [ ] Can run SQL query: `SELECT * FROM information_schema.tables LIMIT 5;`

---

## 🆘 Troubleshooting

### Error: "AuthorizationFailed" (Azure CLI)
**Cause:** You lack permissions to modify Purview account  
**Fix:** Use Azure Portal (Method 1) or contact admin (Method 2)

### Error: "Principal ID not found" (Fabric)
**Cause:** MI not fully propagated in Azure AD  
**Fix:** Wait 2-3 minutes, refresh Fabric portal, search again

### Error: "403 Access Denied" (OneLake test)
**Cause:** MI not added to Fabric workspace  
**Fix:** Complete Step 2 (add MI to workspace with Contributor role)

### Error: "Connection failed" (Purview Portal)
**Cause:** MI not enabled OR no Fabric permissions OR wrong URL  
**Fix:** Complete all 4 steps in order, verify each one

---

## 📚 Related Documentation

- **Complete Fix Guide:** `FABRIC_CONNECTION_FIX.md` (500+ lines)
- **Quick Summary:** `README_FABRIC_FIX.md`
- **Diagnostic Tool:** `scripts/test_onelake_connection.py`
- **Automation Script:** `scripts/fix_fabric_automated.ps1`

---

## 🔗 Quick Links

| Resource | URL |
|----------|-----|
| Azure Portal | https://portal.azure.com |
| Purview Account | https://portal.azure.com/#resource/subscriptions/5b44c9f3-bbe7-464c-aa3e-562726a12004/resourceGroups/purview/providers/Microsoft.Purview/accounts/prviewacc |
| Fabric Portal | https://app.fabric.microsoft.com |
| Purview Portal | https://web.purview.azure.com |
| Microsoft Docs | https://learn.microsoft.com/en-us/purview/unified-catalog-self-serve-analytics |

---

## 📊 Configuration Summary

| Setting | Current Value | Target Value |
|---------|--------------|--------------|
| Managed Identity | ❌ Not enabled | ✅ Enabled (SystemAssigned) |
| Principal ID | N/A | (from Azure Portal) |
| Fabric Permission | ❌ Not granted | ✅ Contributor |
| OneLake Test | ❌ 403 Failed | ✅ 200 Success |
| Self-Serve Analytics | ❌ Connection failed | ✅ Connected |

---

**START HERE:** Open Azure Portal and enable Managed Identity (Method 1 above) 🚀

**Created:** 2026-04-22 18:50  
**For:** prviewacc Managed Identity setup
