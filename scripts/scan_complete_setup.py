#!/usr/bin/env python3
"""
COMPLETE PURVIEW SCANNING SETUP
1. Create/verify credentials
2. Register data sources
3. Create scan configurations
4. Run scans

Official API: 2023-09-01
https://learn.microsoft.com/en-us/rest/api/purview/scanningdataplane
"""
import requests
import json
import time
from azure.identity import AzureCliCredential
from typing import Optional, Dict

# ============================================================================
# CONFIGURATION
# ============================================================================

PURVIEW_ACCOUNT = 'prviewacc'
SUBSCRIPTION_ID = '5b44c9f3-bbe7-464c-aa3e-562726a12004'
RESOURCE_GROUP = 'purview'

# Data Sources
SQL_SERVER = 'sql-hca-demo.database.windows.net'
SQL_DATABASE = 'sql-hca-demo'
FABRIC_WORKSPACE_ID = 'afda4639-34ce-4ee9-a82f-ab7b5cfd7334'
FABRIC_WORKSPACE_NAME = 'Healthcare Analytics'

# API Configuration
ACCOUNT_BASE = f'https://{PURVIEW_ACCOUNT}.purview.azure.com'
SCAN_BASE = f'{ACCOUNT_BASE}/scan'
API_VERSION = '2023-09-01'

# Authentication
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

# ============================================================================
# CREDENTIAL MANAGEMENT
# ============================================================================

def list_credentials():
    """List all existing credentials"""
    print("\n📋 Listing existing credentials...")
    
    r = requests.get(
        f'{SCAN_BASE}/credentials?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        creds = r.json().get('value', [])
        print(f"   Found {len(creds)} credentials")
        for cred in creds:
            print(f"      - {cred.get('name', '?')} ({cred.get('kind', '?')})")
        return creds
    elif r.status_code == 403:
        print(f"   ⚠️  403 Forbidden - Need 'Data Source Administrator' role")
        return []
    else:
        print(f"   Status: {r.status_code}")
        print(f"   {r.text[:300]}")
        return []

def create_managed_identity_credential(cred_name: str = 'purview-msi'):
    """Create credential for Purview Managed Identity"""
    print(f"\n🔑 Creating managed identity credential: {cred_name}")
    
    # Check if exists
    r = requests.get(
        f'{SCAN_BASE}/credentials/{cred_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        print(f"   ✅ Credential already exists")
        return cred_name
    
    # Create new credential
    body = {
        "kind": "ManagedIdentity",
        "properties": {
            "description": "Purview Managed Identity for data source scanning",
            "typeProperties": {}
        }
    }
    
    r = requests.put(
        f'{SCAN_BASE}/credentials/{cred_name}?api-version={API_VERSION}',
        headers=headers,
        json=body,
        timeout=30
    )
    
    if r.status_code in [200, 201]:
        print(f"   ✅ Credential created")
        return cred_name
    else:
        print(f"   ❌ Failed: {r.status_code}")
        print(f"   {r.text[:300]}")
        return None

# ============================================================================
# DATA SOURCE REGISTRATION
# ============================================================================

def register_sql_database(credential_name: str):
    """Register SQL Database as data source"""
    print("\n" + "="*80)
    print("  REGISTERING SQL DATABASE")
    print("="*80)
    
    datasource_name = 'AzureSqlDatabase-sql-hca-demo'
    
    # Check if exists
    r = requests.get(
        f'{SCAN_BASE}/datasources/{datasource_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        print(f"✅ SQL Database already registered")
        return datasource_name
    
    # Register new
    body = {
        "kind": "AzureSqlDatabase",
        "properties": {
            "serverEndpoint": SQL_SERVER,
            "database": SQL_DATABASE,
            "subscriptionId": SUBSCRIPTION_ID,
            "resourceGroup": RESOURCE_GROUP,
            "location": "swedencentral",
            "resourceName": SQL_DATABASE,
            "resourceId": f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Sql/servers/sql-hca-demo/databases/{SQL_DATABASE}"
        }
    }
    
    r = requests.put(
        f'{SCAN_BASE}/datasources/{datasource_name}?api-version={API_VERSION}',
        headers=headers,
        json=body,
        timeout=30
    )
    
    if r.status_code in [200, 201]:
        print(f"✅ SQL Database registered: {datasource_name}")
        return datasource_name
    else:
        print(f"❌ Failed: {r.status_code}")
        print(f"   {r.text[:500]}")
        return None

def register_fabric_workspace():
    """Register Fabric Workspace as data source"""
    print("\n" + "="*80)
    print("  REGISTERING FABRIC WORKSPACE")
    print("="*80)
    
    datasource_name = 'PowerBI-Healthcare-Analytics'
    
    # Check if exists
    r = requests.get(
        f'{SCAN_BASE}/datasources/{datasource_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        print(f"✅ Fabric workspace already registered")
        return datasource_name
    
    # Register new
    body = {
        "kind": "PowerBI",
        "properties": {
            "tenant": "71c4b6d5-0065-4c6c-a125-841a582754eb",
            "subscriptionId": SUBSCRIPTION_ID,
            "resourceGroup": RESOURCE_GROUP,
            "location": "swedencentral"
        }
    }
    
    r = requests.put(
        f'{SCAN_BASE}/datasources/{datasource_name}?api-version={API_VERSION}',
        headers=headers,
        json=body,
        timeout=30
    )
    
    if r.status_code in [200, 201]:
        print(f"✅ Fabric workspace registered: {datasource_name}")
        return datasource_name
    else:
        print(f"❌ Failed: {r.status_code}")
        print(f"   {r.text[:500]}")
        return None

# ============================================================================
# SCAN CONFIGURATION
# ============================================================================

def create_sql_scan(datasource_name: str, credential_name: str):
    """Create and configure SQL scan"""
    print("\n📊 Creating SQL scan configuration...")
    
    scan_name = 'Scan-Healthcare-SQL'
    
    # Check if exists
    r = requests.get(
        f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code != 200:
        # Create scan
        body = {
            "kind": "AzureSqlDatabaseMsi",
            "properties": {
                "scanRulesetName": "AzureSqlDatabase",
                "scanRulesetType": "System",
                "credential": {
                    "referenceName": credential_name,
                    "credentialType": "ManagedIdentity"
                }
            }
        }
        
        r = requests.put(
            f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}?api-version={API_VERSION}',
            headers=headers,
            json=body,
            timeout=30
        )
        
        if r.status_code in [200, 201]:
            print(f"   ✅ Scan created: {scan_name}")
        else:
            print(f"   ❌ Failed: {r.status_code}")
            print(f"   {r.text[:500]}")
            return None
    else:
        print(f"   ✅ Scan already exists: {scan_name}")
    
    return scan_name

def create_fabric_scan(datasource_name: str):
    """Create and configure Fabric workspace scan"""
    print("\n📊 Creating Fabric scan configuration...")
    
    scan_name = 'Scan-Healthcare-Fabric'
    
    # Check if exists
    r = requests.get(
        f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code != 200:
        # Create scan
        body = {
            "kind": "PowerBIDelegated",
            "properties": {
                "scanRulesetName": "PowerBI",
                "scanRulesetType": "System",
                "includePersonalWorkspaces": False
            }
        }
        
        r = requests.put(
            f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}?api-version={API_VERSION}',
            headers=headers,
            json=body,
            timeout=30
        )
        
        if r.status_code in [200, 201]:
            print(f"   ✅ Scan created: {scan_name}")
        else:
            print(f"   ❌ Failed: {r.status_code}")
            print(f"   {r.text[:500]}")
            return None
    else:
        print(f"   ✅ Scan already exists: {scan_name}")
    
    return scan_name

# ============================================================================
# RUN SCANS
# ============================================================================

def run_scan(datasource_name: str, scan_name: str):
    """Trigger a scan run"""
    print(f"\n🚀 Starting scan: {scan_name}")
    
    r = requests.post(
        f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}/run?api-version={API_VERSION}',
        headers=headers,
        json={},
        timeout=30
    )
    
    if r.status_code in [200, 202]:
        print(f"   ✅ Scan started")
        return True
    else:
        print(f"   ❌ Failed: {r.status_code}")
        print(f"   {r.text[:500]}")
        return False

def check_scan_status(datasource_name: str, scan_name: str):
    """Check latest scan run status"""
    print(f"\n⏳ Checking scan status: {scan_name}")
    
    r = requests.get(
        f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}/runs?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        runs = r.json().get('value', [])
        if runs:
            latest = runs[0]
            status = latest.get('status', 'Unknown')
            start_time = latest.get('startTime', '?')
            end_time = latest.get('endTime', 'In progress')
            
            print(f"   Status: {status}")
            print(f"   Started: {start_time}")
            print(f"   Ended: {end_time}")
            
            if status == 'Succeeded':
                scan_result = latest.get('scanResultId', '?')
                print(f"   ✅ Scan completed: {scan_result}")
            elif status == 'Failed':
                error = latest.get('error', {})
                print(f"   ❌ Error: {error}")
            
            return status
        else:
            print(f"   No runs found")
    else:
        print(f"   Status: {r.status_code}")
    
    return None

# ============================================================================
# LIST ALL DATA SOURCES
# ============================================================================

def list_all_data_sources():
    """List all registered data sources and their scans"""
    print("\n" + "="*80)
    print("  REGISTERED DATA SOURCES")
    print("="*80)
    
    r = requests.get(
        f'{SCAN_BASE}/datasources?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        sources = r.json().get('value', [])
        print(f"\nFound {len(sources)} data sources:\n")
        
        for source in sources:
            name = source.get('name', '?')
            kind = source.get('kind', '?')
            print(f"📁 {name}")
            print(f"   Type: {kind}")
            
            # List scans
            r_scans = requests.get(
                f'{SCAN_BASE}/datasources/{name}/scans?api-version={API_VERSION}',
                headers=headers,
                timeout=30
            )
            
            if r_scans.status_code == 200:
                scans = r_scans.json().get('value', [])
                if scans:
                    print(f"   Scans:")
                    for scan in scans:
                        scan_name = scan.get('name', '?')
                        print(f"      ↳ {scan_name}")
            print()
        
        return sources
    elif r.status_code == 403:
        print("\n⚠️  403 Forbidden")
        print("   You need 'Data Source Administrator' role in Purview")
        print("\n   To add this role:")
        print("   1. Go to: https://portal.azure.com/#view/Microsoft_Azure_Purview")
        print("   2. Open prviewacc → Data Map → Collections → Root Collection")
        print("   3. Click 'Role assignments'")
        print("   4. Add yourself as 'Data Source Administrator'")
        return []
    else:
        print(f"❌ Failed: {r.status_code}")
        print(f"   {r.text[:500]}")
        return []

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("="*80)
    print("  PURVIEW COMPLETE SCANNING SETUP")
    print("  Official API: 2023-09-01")
    print("="*80)
    
    # Check existing setup
    print("\n📋 PHASE 1: Check existing configuration")
    print("-"*80)
    
    creds = list_credentials()
    sources = list_all_data_sources()
    
    if not creds and not sources:
        print("\n⚠️  PERMISSION ISSUE DETECTED")
        print("="*80)
        print("""
You are getting 403 Forbidden errors, which means you need additional
permissions to scan data sources.

SOLUTION:
1. Go to Azure Portal: https://portal.azure.com
2. Navigate to: Microsoft Purview → prviewacc
3. Open: Data Map → Collections → Root Collection (prviewacc)
4. Click: Role assignments
5. Add your user to: "Data Source Administrator" role
6. Wait 5-10 minutes for permissions to propagate
7. Re-run this script

ALTERNATIVE - Using Azure CLI:
az purview account add-root-collection-admin \\
    --account-name prviewacc \\
    --resource-group purview \\
    --object-id <your-object-id>

After adding permissions, run:
    python scripts/scan_complete_setup.py
""")
        return
    
    # Create credentials
    print("\n📋 PHASE 2: Create scanning credentials")
    print("-"*80)
    
    cred_name = create_managed_identity_credential()
    if not cred_name:
        print("\n❌ Could not create credential")
        return
    
    # Register data sources
    print("\n📋 PHASE 3: Register data sources")
    print("-"*80)
    
    sql_ds = register_sql_database(cred_name)
    fabric_ds = register_fabric_workspace()
    
    # Create scan configurations
    print("\n📋 PHASE 4: Create scan configurations")
    print("-"*80)
    
    scans_to_run = []
    
    if sql_ds:
        sql_scan = create_sql_scan(sql_ds, cred_name)
        if sql_scan:
            scans_to_run.append((sql_ds, sql_scan))
    
    if fabric_ds:
        fabric_scan = create_fabric_scan(fabric_ds)
        if fabric_scan:
            scans_to_run.append((fabric_ds, fabric_scan))
    
    # Run scans
    print("\n📋 PHASE 5: Run scans")
    print("-"*80)
    
    for ds_name, scan_name in scans_to_run:
        run_scan(ds_name, scan_name)
        time.sleep(2)
    
    # Check status
    print("\n📋 PHASE 6: Check scan status")
    print("-"*80)
    
    for ds_name, scan_name in scans_to_run:
        check_scan_status(ds_name, scan_name)
    
    # Summary
    print("\n" + "="*80)
    print("  SETUP COMPLETE")
    print("="*80)
    print(f"""
✅ Credentials created: {len(creds) + 1}
✅ Data sources registered: {len([sql_ds, fabric_ds])}
✅ Scans configured: {len(scans_to_run)}
✅ Scans started: {len(scans_to_run)}

📊 Scans will run in the background and may take 10-30 minutes.

View results at:
https://portal.azure.com/#view/Microsoft_Azure_Purview/MainMenuBlade/~/dataCatalog

To check scan status again later:
    python scripts/check_scan_status.py
""")

if __name__ == '__main__':
    main()
