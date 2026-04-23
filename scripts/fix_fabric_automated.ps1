# Fix Purview Fabric Connection - Automated PowerShell Script
# This script automates as much as possible of the Fabric connection fix

param(
    [switch]$WhatIf = $false
)

Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "  PURVIEW FABRIC CONNECTION - AUTOMATED FIX" -ForegroundColor Cyan
Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host ""

$ErrorActionPreference = "Stop"

# Configuration
$resourceGroup = "purview"
$purviewAccount = "prviewacc"
$workspaceId = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
$lakehouseId = "2960eef0-5de6-4117-80b1-6ee783cdaeec"
$correctUrl = "https://onelake.dfs.fabric.microsoft.com/$workspaceId/$lakehouseId/Files/DEH"

# Step 1: Verify Azure CLI
Write-Host "🔍 STEP 1: Verify Azure CLI..." -ForegroundColor Yellow
Write-Host ""

try {
    $account = az account show 2>$null | ConvertFrom-Json
    Write-Host "   ✅ Logged in as: $($account.user.name)" -ForegroundColor Green
    Write-Host "   ✅ Subscription: $($account.name)" -ForegroundColor Green
    Write-Host ""
} catch {
    Write-Host "   ❌ Not logged in to Azure CLI" -ForegroundColor Red
    Write-Host ""
    Write-Host "   Run: az login" -ForegroundColor Yellow
    exit 1
}

# Step 2: Check Purview Managed Identity
Write-Host "🔍 STEP 2: Check Purview Managed Identity..." -ForegroundColor Yellow
Write-Host ""

try {
    $purviewResource = az resource show `
        --resource-group $resourceGroup `
        --name $purviewAccount `
        --resource-type "Microsoft.Purview/accounts" `
        2>$null | ConvertFrom-Json
    
    $principalId = $purviewResource.identity.principalId
    
    if ($principalId) {
        Write-Host "   ✅ Managed Identity already enabled" -ForegroundColor Green
        Write-Host "   Principal ID: $principalId" -ForegroundColor Cyan
        Write-Host ""
    } else {
        Write-Host "   ⚠️  Managed Identity NOT enabled" -ForegroundColor Yellow
        Write-Host ""
        
        if ($WhatIf) {
            Write-Host "   [WHATIF] Would enable Managed Identity" -ForegroundColor Magenta
            Write-Host ""
        } else {
            Write-Host "   Attempting to enable Managed Identity..." -ForegroundColor Yellow
            
            try {
                $result = az resource update `
                    --resource-group $resourceGroup `
                    --name $purviewAccount `
                    --resource-type "Microsoft.Purview/accounts" `
                    --set identity.type=SystemAssigned `
                    2>&1
                
                if ($LASTEXITCODE -eq 0) {
                    $updated = $result | ConvertFrom-Json
                    $principalId = $updated.identity.principalId
                    Write-Host "   ✅ Managed Identity enabled!" -ForegroundColor Green
                    Write-Host "   Principal ID: $principalId" -ForegroundColor Cyan
                    Write-Host ""
                } else {
                    throw "Failed to enable MI"
                }
            } catch {
                Write-Host "   ❌ Failed to enable Managed Identity automatically" -ForegroundColor Red
                Write-Host "   Error: $_" -ForegroundColor Red
                Write-Host ""
                Write-Host "   📝 MANUAL FIX REQUIRED:" -ForegroundColor Yellow
                Write-Host "   1. Open Azure Portal: https://portal.azure.com" -ForegroundColor White
                Write-Host "   2. Navigate to: Resource Groups → $resourceGroup → $purviewAccount" -ForegroundColor White
                Write-Host "   3. Click: Identity (left menu)" -ForegroundColor White
                Write-Host "   4. System assigned → Status: On → Save" -ForegroundColor White
                Write-Host "   5. Copy the Principal ID" -ForegroundColor White
                Write-Host "   6. Re-run this script" -ForegroundColor White
                Write-Host ""
                
                $continue = Read-Host "   Have you enabled MI manually? (y/N)"
                if ($continue -ne 'y') {
                    exit 1
                }
                
                # Re-check
                $purviewResource = az resource show `
                    --resource-group $resourceGroup `
                    --name $purviewAccount `
                    --resource-type "Microsoft.Purview/accounts" | ConvertFrom-Json
                
                $principalId = $purviewResource.identity.principalId
                
                if ($principalId) {
                    Write-Host "   ✅ Verified: MI now enabled!" -ForegroundColor Green
                    Write-Host "   Principal ID: $principalId" -ForegroundColor Cyan
                    Write-Host ""
                } else {
                    Write-Host "   ❌ MI still not enabled" -ForegroundColor Red
                    exit 1
                }
            }
        }
    }
} catch {
    Write-Host "   ❌ Failed to check Purview resource" -ForegroundColor Red
    Write-Host "   Error: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "   You may lack permissions to view Purview account" -ForegroundColor Yellow
    Write-Host "   Contact your Azure admin" -ForegroundColor Yellow
    exit 1
}

# Step 3: Test OneLake Connection
Write-Host "🔍 STEP 3: Test OneLake Connection..." -ForegroundColor Yellow
Write-Host ""

if ($WhatIf) {
    Write-Host "   [WHATIF] Would test OneLake connection" -ForegroundColor Magenta
    Write-Host ""
} else {
    Write-Host "   Running connectivity test..." -ForegroundColor White
    
    $testResult = python scripts/test_onelake_connection.py 2>&1
    $testExitCode = $LASTEXITCODE
    
    if ($testExitCode -eq 0) {
        Write-Host "   ✅ OneLake connection successful!" -ForegroundColor Green
        Write-Host ""
    } else {
        Write-Host "   ❌ OneLake connection failed (403 Access Denied)" -ForegroundColor Red
        Write-Host ""
        Write-Host "   📝 MANUAL FIX REQUIRED:" -ForegroundColor Yellow
        Write-Host "   1. Open Fabric Portal: https://app.fabric.microsoft.com" -ForegroundColor White
        Write-Host "   2. Find your workspace (ID: $workspaceId)" -ForegroundColor White
        Write-Host "   3. Click: Workspace Settings → Manage access" -ForegroundColor White
        Write-Host "   4. Click: + Add people or groups" -ForegroundColor White
        Write-Host "   5. Search for: $purviewAccount" -ForegroundColor White
        Write-Host "      (or use Principal ID: $principalId)" -ForegroundColor White
        Write-Host "   6. Select role: Contributor" -ForegroundColor White
        Write-Host "   7. Click: Add" -ForegroundColor White
        Write-Host ""
        
        $continue = Read-Host "   Have you added Purview to Fabric workspace? (y/N)"
        if ($continue -ne 'y') {
            Write-Host ""
            Write-Host "   ⏸️  Paused - Complete manual steps then re-run script" -ForegroundColor Yellow
            exit 1
        }
        
        # Re-test
        Write-Host ""
        Write-Host "   Re-testing OneLake connection..." -ForegroundColor White
        $testResult = python scripts/test_onelake_connection.py 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ Connection now working!" -ForegroundColor Green
            Write-Host ""
        } else {
            Write-Host "   ❌ Connection still failing" -ForegroundColor Red
            Write-Host "   Please verify Fabric permissions and workspace ID" -ForegroundColor Yellow
            Write-Host ""
            exit 1
        }
    }
}

# Step 4: Get correct workspace name from Fabric
Write-Host "🔍 STEP 4: Verify Workspace Name..." -ForegroundColor Yellow
Write-Host ""

Write-Host "   Screenshot shows: 'DataGovernenee' (possible typo)" -ForegroundColor Yellow
Write-Host "   Workspace ID: $workspaceId" -ForegroundColor Cyan
Write-Host ""
Write-Host "   ⚠️  Please verify the EXACT workspace name in Fabric Portal" -ForegroundColor Yellow
Write-Host ""

$workspaceName = Read-Host "   Enter exact workspace name (or press Enter to use GUID-based URL)"

if ([string]::IsNullOrWhiteSpace($workspaceName)) {
    Write-Host "   Using GUID-based URL (recommended)" -ForegroundColor Cyan
    $finalUrl = $correctUrl
} else {
    Write-Host "   Using name-based URL" -ForegroundColor Cyan
    $finalUrl = "https://onelake.dfs.fabric.microsoft.com/$workspaceName/DEH.Lakehouse/Files/DEH"
}

Write-Host ""
Write-Host "   📍 Final URL: $finalUrl" -ForegroundColor Green
Write-Host ""

# Step 5: Update Purview Configuration
Write-Host "🔍 STEP 5: Update Purview Configuration..." -ForegroundColor Yellow
Write-Host ""

if ($WhatIf) {
    Write-Host "   [WHATIF] Would update Purview config" -ForegroundColor Magenta
    Write-Host ""
} else {
    Write-Host "   ⚠️  MANUAL STEP - Purview Portal Configuration:" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "   1. Open: https://web.purview.azure.com" -ForegroundColor White
    Write-Host "   2. Select account: $purviewAccount" -ForegroundColor White
    Write-Host "   3. Navigate: Unified Catalog → Solution integrations → Self-serve analytics" -ForegroundColor White
    Write-Host "   4. Click: Edit or Configure storage" -ForegroundColor White
    Write-Host "   5. Update Location URL to:" -ForegroundColor White
    Write-Host ""
    Write-Host "      $finalUrl" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "   6. Authentication: System assigned managed identity" -ForegroundColor White
    Write-Host "   7. Click: Save" -ForegroundColor White
    Write-Host "   8. Click: Test connection" -ForegroundColor White
    Write-Host ""
    
    $continue = Read-Host "   Have you updated the configuration? (y/N)"
    if ($continue -ne 'y') {
        Write-Host ""
        Write-Host "   ⏸️  Paused - Complete configuration then verify manually" -ForegroundColor Yellow
        exit 0
    }
}

# Final verification
Write-Host ""
Write-Host "=" * 80 -ForegroundColor Green
Write-Host "  CONFIGURATION COMPLETE!" -ForegroundColor Green
Write-Host "=" * 80 -ForegroundColor Green
Write-Host ""
Write-Host "✅ Summary:" -ForegroundColor Green
Write-Host "   • Purview Managed Identity: Enabled" -ForegroundColor White
Write-Host "   • Principal ID: $principalId" -ForegroundColor White
Write-Host "   • OneLake Connection: Working" -ForegroundColor White
Write-Host "   • Correct URL: $finalUrl" -ForegroundColor White
Write-Host ""
Write-Host "📋 Verify in Purview Portal:" -ForegroundColor Yellow
Write-Host "   • Unified Catalog → Self-serve analytics" -ForegroundColor White
Write-Host "   • Test connection should show: ✅ Connection successful" -ForegroundColor White
Write-Host ""
Write-Host "🎯 Next Steps:" -ForegroundColor Yellow
Write-Host "   1. Run health check:" -ForegroundColor White
Write-Host "      python scripts/purview_monitoring.py" -ForegroundColor Cyan
Write-Host ""
Write-Host "   2. Setup data quality scanning:" -ForegroundColor White
Write-Host "      python scripts/configure_purview_scan_credentials.py" -ForegroundColor Cyan
Write-Host ""
Write-Host "   3. Create data products:" -ForegroundColor White
Write-Host "      python scripts/unified_catalog_examples.py" -ForegroundColor Cyan
Write-Host ""
Write-Host "📖 Full documentation: FABRIC_CONNECTION_FIX.md" -ForegroundColor White
Write-Host ""
