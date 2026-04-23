#!/usr/bin/env python3
"""
PURVIEW MASTER SCANNING SCRIPT
Register and scan all data sources:
- SQL Server (sql-hca-demo)
- Fabric Workspace (Healthcare Analytics)
- Azure Storage (if applicable)
"""
import requests
import json
import time
from azure.identity import AzureCliCredential
from typing import Optional, Dict, List

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

# API Endpoints
SCAN_BASE = f'https://{PURVIEW_ACCOUNT}.purview.azure.com/scan'
API_VERSION = '2023-09-01'

# Authentication
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def wait_for_scan(datasource_name: str, scan_name: str, max_wait: int = 300) -> bool:
    """Wait for scan to complete"""
    print(f"   ⏳ Waiting for scan to complete (max {max_wait}s)...")
    
    start_time = time.time()
    while (time.time() - start_time) < max_wait:
        try:
            r = requests.get(
                f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}/runs?api-version={API_VERSION}',
                headers=headers,
                timeout=30
            )
            
            if r.status_code == 200:
                runs = r.json().get('value', [])
                if runs:
                    latest_run = runs[0]
                    status = latest_run.get('status', 'Unknown')
                    
                    if status == 'Succeeded':
                        print(f"   ✅ Scan completed successfully")
                        return True
                    elif status == 'Failed':
                        print(f"   ❌ Scan failed")
                        error = latest_run.get('error', {})
                        print(f"      Error: {error}")
                        return False
                    else:
                        print(f"   ⏳ Status: {status}...", end='\r')
                        time.sleep(10)
                        continue
        except Exception as e:
            print(f"   ⚠️  Error checking status: {e}")
            time.sleep(10)
            continue
    
    print(f"\n   ⚠️  Scan did not complete within {max_wait}s")
    return False

# ============================================================================
# SQL SERVER SCANNING
# ============================================================================

def register_sql_server():
    """Register SQL Server as data source"""
    print("\n" + "="*80)
    print("  REGISTERING SQL SERVER")
    print("="*80)
    
    datasource_name = 'sql-hca-demo'
    
    # Check if already registered
    r = requests.get(
        f'{SCAN_BASE}/datasources/{datasource_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        print(f"✅ SQL Server already registered: {datasource_name}")
        return datasource_name
    
    # Register new data source
    body = {
        "kind": "AzureSqlDatabase",
        "properties": {
            "serverEndpoint": SQL_SERVER,
            "database": SQL_DATABASE,
            "location": "swedencentral",
            "resourceGroup": f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}",
            "subscriptionId": SUBSCRIPTION_ID
        }
    }
    
    r = requests.put(
        f'{SCAN_BASE}/datasources/{datasource_name}?api-version={API_VERSION}',
        headers=headers,
        json=body,
        timeout=30
    )
    
    if r.status_code in [200, 201]:
        print(f"✅ SQL Server registered: {datasource_name}")
        return datasource_name
    else:
        print(f"❌ Failed to register SQL Server: {r.status_code}")
        print(f"   {r.text[:500]}")
        return None

def create_sql_scan(datasource_name: str):
    """Create and run SQL Server scan"""
    print("\n📊 Creating SQL scan configuration...")
    
    scan_name = 'sql-hca-demo-scan'
    
    # Check if scan exists
    r = requests.get(
        f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code != 200:
        # Create scan configuration
        body = {
            "kind": "AzureSqlDatabaseCredential",
            "properties": {
                "scanRulesetName": "AzureSqlDatabase",
                "scanRulesetType": "System",
                "database": SQL_DATABASE,
                "serverEndpoint": SQL_SERVER
            }
        }
        
        r = requests.put(
            f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}?api-version={API_VERSION}',
            headers=headers,
            json=body,
            timeout=30
        )
        
        if r.status_code not in [200, 201]:
            print(f"❌ Failed to create scan: {r.status_code}")
            print(f"   {r.text[:500]}")
            return False
        
        print(f"✅ Scan configuration created: {scan_name}")
    else:
        print(f"✅ Scan configuration already exists: {scan_name}")
    
    # Run scan
    print("🚀 Starting SQL scan...")
    
    r = requests.post(
        f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}/run?api-version={API_VERSION}',
        headers=headers,
        json={},
        timeout=30
    )
    
    if r.status_code in [200, 202]:
        print(f"✅ Scan started")
        return wait_for_scan(datasource_name, scan_name)
    else:
        print(f"❌ Failed to start scan: {r.status_code}")
        print(f"   {r.text[:500]}")
        return False

# ============================================================================
# FABRIC WORKSPACE SCANNING
# ============================================================================

def register_fabric_workspace():
    """Register Fabric Workspace as data source"""
    print("\n" + "="*80)
    print("  REGISTERING FABRIC WORKSPACE")
    print("="*80)
    
    datasource_name = 'fabric-healthcare-analytics'
    
    # Check if already registered
    r = requests.get(
        f'{SCAN_BASE}/datasources/{datasource_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        print(f"✅ Fabric workspace already registered: {datasource_name}")
        return datasource_name
    
    # Register new data source
    body = {
        "kind": "Fabric",
        "properties": {
            "workspaceId": FABRIC_WORKSPACE_ID,
            "workspaceName": FABRIC_WORKSPACE_NAME,
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
        print(f"❌ Failed to register Fabric workspace: {r.status_code}")
        print(f"   {r.text[:500]}")
        return None

def create_fabric_scan(datasource_name: str):
    """Create and run Fabric workspace scan"""
    print("\n📊 Creating Fabric scan configuration...")
    
    scan_name = 'fabric-healthcare-scan'
    
    # Check if scan exists
    r = requests.get(
        f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code != 200:
        # Create scan configuration
        body = {
            "kind": "FabricMsi",
            "properties": {
                "scanRulesetName": "Fabric",
                "scanRulesetType": "System",
                "workspaceId": FABRIC_WORKSPACE_ID
            }
        }
        
        r = requests.put(
            f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}?api-version={API_VERSION}',
            headers=headers,
            json=body,
            timeout=30
        )
        
        if r.status_code not in [200, 201]:
            print(f"❌ Failed to create scan: {r.status_code}")
            print(f"   {r.text[:500]}")
            return False
        
        print(f"✅ Scan configuration created: {scan_name}")
    else:
        print(f"✅ Scan configuration already exists: {scan_name}")
    
    # Run scan
    print("🚀 Starting Fabric scan...")
    
    r = requests.post(
        f'{SCAN_BASE}/datasources/{datasource_name}/scans/{scan_name}/run?api-version={API_VERSION}',
        headers=headers,
        json={},
        timeout=30
    )
    
    if r.status_code in [200, 202]:
        print(f"✅ Scan started")
        return wait_for_scan(datasource_name, scan_name, max_wait=600)
    else:
        print(f"❌ Failed to start scan: {r.status_code}")
        print(f"   {r.text[:500]}")
        return False

# ============================================================================
# LIST EXISTING DATA SOURCES AND SCANS
# ============================================================================

def list_all_data_sources():
    """List all registered data sources"""
    print("\n" + "="*80)
    print("  EXISTING DATA SOURCES")
    print("="*80)
    
    r = requests.get(
        f'{SCAN_BASE}/datasources?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        sources = r.json().get('value', [])
        print(f"Found {len(sources)} data sources:\n")
        
        for source in sources:
            name = source.get('name', '?')
            kind = source.get('kind', '?')
            print(f"  📁 {name} ({kind})")
            
            # List scans for this data source
            r_scans = requests.get(
                f'{SCAN_BASE}/datasources/{name}/scans?api-version={API_VERSION}',
                headers=headers,
                timeout=30
            )
            
            if r_scans.status_code == 200:
                scans = r_scans.json().get('value', [])
                for scan in scans:
                    scan_name = scan.get('name', '?')
                    print(f"     ↳ Scan: {scan_name}")
        
        return sources
    else:
        print(f"❌ Failed to list data sources: {r.status_code}")
        return []

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("="*80)
    print("  PURVIEW DATA SOURCE SCANNING")
    print("  Register and scan all data sources")
    print("="*80)
    
    # List existing data sources
    list_all_data_sources()
    
    stats = {
        'registered': 0,
        'scanned': 0,
        'failed': 0
    }
    
    # 1. SQL Server
    print("\n" + "="*80)
    print("  1/2: SQL SERVER")
    print("="*80)
    
    sql_ds = register_sql_server()
    if sql_ds:
        stats['registered'] += 1
        if create_sql_scan(sql_ds):
            stats['scanned'] += 1
        else:
            stats['failed'] += 1
    else:
        stats['failed'] += 1
    
    # 2. Fabric Workspace
    print("\n" + "="*80)
    print("  2/2: FABRIC WORKSPACE")
    print("="*80)
    
    fabric_ds = register_fabric_workspace()
    if fabric_ds:
        stats['registered'] += 1
        if create_fabric_scan(fabric_ds):
            stats['scanned'] += 1
        else:
            stats['failed'] += 1
    else:
        stats['failed'] += 1
    
    # Summary
    print("\n" + "="*80)
    print("  SCANNING COMPLETE")
    print("="*80)
    print(f"  Data Sources Registered: {stats['registered']}")
    print(f"  Scans Completed: {stats['scanned']}")
    print(f"  Failed: {stats['failed']}")
    print("="*80)
    
    if stats['scanned'] > 0:
        print("\n✅ Data sources scanned successfully!")
        print("   Assets will appear in Purview within 5-10 minutes")
        print("   Check: https://portal.azure.com/#view/Microsoft_Azure_Purview")
    
    if stats['failed'] > 0:
        print("\n⚠️  Some scans failed - check errors above")
        print("   Common issues:")
        print("   - Purview MSI needs permissions on data sources")
        print("   - SQL Server firewall rules")
        print("   - Fabric workspace access")

if __name__ == '__main__':
    main()
