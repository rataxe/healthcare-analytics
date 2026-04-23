#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Enable Purview Lineage extraction for Azure SQL Database
    
.DESCRIPTION
    Configures SQL Database for Purview lineage extraction by:
    1. Creating Database Master Key (if not exists)
    2. Creating SQL users from External Provider for both Purview MSIs
    3. Granting db_owner role to both MSIs
#>

param(
    [string]$ServerName = "sql-hca-demo.database.windows.net",
    [string]$DatabaseName = "HealthcareAnalyticsDB"
)

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host ("="*80) -ForegroundColor Cyan
Write-Host "  ENABLE PURVIEW LINEAGE EXTRACTION FOR AZURE SQL DATABASE" -ForegroundColor Cyan
Write-Host ("="*80) -ForegroundColor Cyan
Write-Host ""
Write-Host "  Server:   $ServerName" -ForegroundColor White
Write-Host "  Database: $DatabaseName" -ForegroundColor White
Write-Host ""
Write-Host "  Managed Identities:" -ForegroundColor White
Write-Host "    1. prviewacc (Purview Account MSI)" -ForegroundColor White
Write-Host "       Object ID: 393d3d46-fd94-4128-a0b4-8ceb7ec371b1" -ForegroundColor White
Write-Host "    2. mi-purview (User-assigned MSI)" -ForegroundColor White
Write-Host "       Object ID: a1110d1d-6964-43c4-b171-13379215123a" -ForegroundColor White
Write-Host ""
Write-Host ("="*80) -ForegroundColor Cyan
Write-Host ""

# Get Azure AD token for authentication
Write-Host "🔐 Getting Azure AD authentication token..." -ForegroundColor Yellow
$token = az account get-access-token --resource https://database.windows.net/ --query accessToken -o tsv

if (-not $token) {
    Write-Host "❌ Failed to get Azure AD token. Please run: az login" -ForegroundColor Red
    exit 1
}

Write-Host "✅ Token obtained" -ForegroundColor Green

# SQL Commands to execute
$sqlCommands = @(
    @{
        Name = "Check Master Key"
        Query = @"
SELECT name, algorithm_desc, create_date
FROM sys.symmetric_keys
WHERE name = '##MS_DatabaseMasterKey##'
"@
        Required = $false
    },
    @{
        Name = "Create Master Key"
        Query = @"
IF NOT EXISTS (
    SELECT * FROM sys.symmetric_keys 
    WHERE name = '##MS_DatabaseMasterKey##'
)
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'PurviewLineage2026!Str0ngP@ssw0rd#Secure$'
    PRINT 'Master key created successfully'
END
ELSE
BEGIN
    PRINT 'Master key already exists'
END
"@
        Required = $false
    },
    @{
        Name = "Create User: prviewacc"
        Query = @"
IF NOT EXISTS (
    SELECT * FROM sys.database_principals 
    WHERE name = 'prviewacc' AND type = 'E'
)
BEGIN
    CREATE USER [prviewacc] FROM EXTERNAL PROVIDER
    PRINT 'User prviewacc created successfully'
END
ELSE
BEGIN
    PRINT 'User prviewacc already exists'
END
"@
        Required = $true
    },
    @{
        Name = "Grant db_owner to prviewacc"
        Query = @"
IF NOT EXISTS (
    SELECT 1 
    FROM sys.database_role_members rm
    JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
    JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
    WHERE rp.name = 'db_owner' AND mp.name = 'prviewacc'
)
BEGIN
    ALTER ROLE db_owner ADD MEMBER [prviewacc]
    PRINT 'Role db_owner granted to prviewacc'
END
ELSE
BEGIN
    PRINT 'prviewacc already has db_owner role'
END
"@
        Required = $true
    },
    @{
        Name = "Create User: mi-purview"
        Query = @"
IF NOT EXISTS (
    SELECT * FROM sys.database_principals 
    WHERE name = 'mi-purview' AND type = 'E'
)
BEGIN
    CREATE USER [mi-purview] FROM EXTERNAL PROVIDER
    PRINT 'User mi-purview created successfully'
END
ELSE
BEGIN
    PRINT 'User mi-purview already exists'
END
"@
        Required = $true
    },
    @{
        Name = "Grant db_owner to mi-purview"
        Query = @"
IF NOT EXISTS (
    SELECT 1 
    FROM sys.database_role_members rm
    JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
    JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
    WHERE rp.name = 'db_owner' AND mp.name = 'mi-purview'
)
BEGIN
    ALTER ROLE db_owner ADD MEMBER [mi-purview]
    PRINT 'Role db_owner granted to mi-purview'
END
ELSE
BEGIN
    PRINT 'mi-purview already has db_owner role'
END
"@
        Required = $true
    },
    @{
        Name = "Verify Configuration"
        Query = @"
-- Check users exist
SELECT 
    'User Check' as Check_Type,
    name as User_Name, 
    type_desc as User_Type,
    create_date as Created
FROM sys.database_principals
WHERE name IN ('prviewacc', 'mi-purview')

-- Check role membership
SELECT 
    'Role Check' as Check_Type,
    rp.name as Role_Name,
    mp.name as Member_Name,
    NULL as Created
FROM sys.database_role_members rm
JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
WHERE mp.name IN ('prviewacc', 'mi-purview') 
  AND rp.name = 'db_owner'
"@
        Required = $false
    }
)

# Execute each SQL command
$stepNum = 1
$failedSteps = @()

foreach ($cmd in $sqlCommands) {
    Write-Host ""
    Write-Host ("="*80) -ForegroundColor Cyan
    $stepLabel = "STEP {0}: {1}" -f $stepNum, $cmd.Name
    Write-Host "  $stepLabel" -ForegroundColor Cyan
    Write-Host ("="*80) -ForegroundColor Cyan
    Write-Host ""
    
    # Save query to temp file
    $tempFile = [System.IO.Path]::GetTempFileName()
    $cmd.Query | Out-File -FilePath $tempFile -Encoding UTF8
    
    try {
        Write-Host "Executing SQL query..." -ForegroundColor Yellow
        
        $result = az sql db query `
            --server $ServerName.Replace('.database.windows.net', '') `
            --database $DatabaseName `
            --file $tempFile `
            --auth-type Active-Directory-Default `
            2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "✅ SUCCESS" -ForegroundColor Green
            
            if ($result) {
                $result | ForEach-Object {
                    if ($_ -match 'already exists' -or $_ -match 'already has') {
                        Write-Host "  ℹ️  $_" -ForegroundColor Cyan
                    } elseif ($_ -match 'created successfully' -or $_ -match 'granted') {
                        Write-Host "  ✅ $_" -ForegroundColor Green
                    } else {
                        Write-Host "  $_" -ForegroundColor White
                    }
                }
            }
        } else {
            Write-Host "❌ FAILED" -ForegroundColor Red
            Write-Host "Error: $result" -ForegroundColor Red
            
            if ($cmd.Required) {
                $failedSteps += $cmd.Name
            }
        }
    }
    catch {
        Write-Host "❌ EXCEPTION: $_" -ForegroundColor Red
        if ($cmd.Required) {
            $failedSteps += $cmd.Name
        }
    }
    finally {
        Remove-Item $tempFile -ErrorAction SilentlyContinue
    }
    
    $stepNum++
}

# Final Summary
Write-Host ""
Write-Host ("="*80) -ForegroundColor Cyan
Write-Host "  FINAL SUMMARY" -ForegroundColor Cyan
Write-Host ("="*80) -ForegroundColor Cyan
Write-Host ""

if ($failedSteps.Count -eq 0) {
    Write-Host "✅✅✅ CONFIGURATION COMPLETE ✅✅✅" -ForegroundColor Green
    Write-Host ""
    Write-Host "Both Purview Managed Identities configured for lineage extraction:" -ForegroundColor Green
    Write-Host "  ✅ prviewacc (Purview Account MSI)" -ForegroundColor Green
    Write-Host "  ✅ mi-purview (User-assigned MSI)" -ForegroundColor Green
    Write-Host ""
    Write-Host "📋 Next steps:" -ForegroundColor Yellow
    Write-Host "  1. Azure Portal → Purview → Data Map → Sources" -ForegroundColor White
    Write-Host "  2. Find your Azure SQL Database data source" -ForegroundColor White
    Write-Host "  3. Edit data source → Enable 'Lineage extraction'" -ForegroundColor White
    Write-Host "  4. Select authentication: Managed Identity (prviewacc)" -ForegroundColor White
    Write-Host "  5. Run a new scan" -ForegroundColor White
    Write-Host "  6. Verify lineage appears in Data Catalog" -ForegroundColor White
} else {
    Write-Host "⚠️  CONFIGURATION INCOMPLETE" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Failed steps:" -ForegroundColor Red
    $failedSteps | ForEach-Object {
        Write-Host "  ❌ $_" -ForegroundColor Red
    }
    Write-Host ""
    Write-Host "📋 Manual steps required:" -ForegroundColor Yellow
    Write-Host "  1. Connect to SQL Server with Azure AD admin account" -ForegroundColor White
    Write-Host "  2. Run these commands in SSMS or Azure Data Studio:" -ForegroundColor White
    Write-Host ""
    Write-Host "     -- Create users" -ForegroundColor Gray
    Write-Host "     CREATE USER [prviewacc] FROM EXTERNAL PROVIDER;" -ForegroundColor Gray
    Write-Host "     CREATE USER [mi-purview] FROM EXTERNAL PROVIDER;" -ForegroundColor Gray
    Write-Host ""
    Write-Host "     -- Grant permissions" -ForegroundColor Gray
    Write-Host "     ALTER ROLE db_owner ADD MEMBER [prviewacc];" -ForegroundColor Gray
    Write-Host "     ALTER ROLE db_owner ADD MEMBER [mi-purview];" -ForegroundColor Gray
    Write-Host ""
}

Write-Host ("="*80) -ForegroundColor Cyan
Write-Host ""
