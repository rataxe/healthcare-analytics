#!/usr/bin/env python3
"""
FIX PURVIEW CREDENTIALS
Diagnose and fix the purview-msi credential issue
"""
import requests
import json
from azure.identity import AzureCliCredential

# Configuration
PURVIEW_ACCOUNT = 'prviewacc.purview.azure.com'
ACCOUNT_BASE = f'https://{PURVIEW_ACCOUNT}'
SCAN_BASE = f'{ACCOUNT_BASE}/scan'
API_VERSION = '2023-09-01'

# Authentication
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

def check_scan_api_access():
    """Check if we have Scan API access"""
    print("="*80)
    print("  CHECKING SCAN API ACCESS")
    print("="*80)
    
    r = requests.get(
        f'{SCAN_BASE}/datasources?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code == 200:
        print("✅ Scan API access: GRANTED")
        return True
    elif r.status_code == 403:
        print("❌ Scan API access: FORBIDDEN")
        print("   Need 'Data Source Administrator' role")
        return False
    else:
        print(f"⚠️  Scan API access: {r.status_code}")
        return False

def list_credentials():
    """List all credentials"""
    print("\n" + "="*80)
    print("  CURRENT CREDENTIALS")
    print("="*80)
    
    r = requests.get(
        f'{SCAN_BASE}/credentials?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code != 200:
        print(f"❌ Could not list credentials: {r.status_code}")
        if r.status_code == 403:
            print("   Need 'Data Source Administrator' role")
        return []
    
    creds = r.json().get('value', [])
    print(f"Found {len(creds)} credential(s)\n")
    
    for c in creds:
        name = c.get('name', '?')
        kind = c.get('kind', '?')
        props = c.get('properties', {})
        
        print(f"📋 {name}")
        print(f"   Type: {kind}")
        print(f"   Properties: {json.dumps(props, indent=2)}")
        print()
    
    return creds

def get_credential_details(cred_name: str):
    """Get detailed info about a credential"""
    print(f"\n{'='*80}")
    print(f"  CREDENTIAL DETAILS: {cred_name}")
    print("="*80)
    
    r = requests.get(
        f'{SCAN_BASE}/credentials/{cred_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code != 200:
        print(f"❌ Could not get credential: {r.status_code}")
        try:
            error = r.json()
            print(f"   Error: {json.dumps(error, indent=2)}")
        except:
            print(f"   Response: {r.text}")
        return None
    
    cred = r.json()
    print("✅ Credential found:")
    print(json.dumps(cred, indent=2))
    return cred

def delete_credential(cred_name: str):
    """Delete a credential"""
    print(f"\n🗑️  Deleting credential: {cred_name}")
    
    r = requests.delete(
        f'{SCAN_BASE}/credentials/{cred_name}?api-version={API_VERSION}',
        headers=headers,
        timeout=30
    )
    
    if r.status_code in [200, 204]:
        print(f"   ✅ Deleted successfully")
        return True
    else:
        print(f"   ❌ Delete failed: {r.status_code}")
        return False

def create_managed_identity_credential(cred_name: str = 'purview-msi'):
    """Create or recreate managed identity credential"""
    print(f"\n{'='*80}")
    print(f"  CREATING MANAGED IDENTITY CREDENTIAL: {cred_name}")
    print("="*80)
    
    # Get Purview managed identity info
    # The managed identity is the Purview account's system-assigned identity
    body = {
        "kind": "ManagedIdentity",
        "properties": {
            "description": "Purview managed identity for data source scanning",
            "typeProperties": {}
        }
    }
    
    print(f"Creating credential with body:")
    print(json.dumps(body, indent=2))
    
    r = requests.put(
        f'{SCAN_BASE}/credentials/{cred_name}?api-version={API_VERSION}',
        headers=headers,
        json=body,
        timeout=30
    )
    
    if r.status_code in [200, 201]:
        print(f"\n✅ Credential created successfully")
        result = r.json()
        print(json.dumps(result, indent=2))
        return True
    else:
        print(f"\n❌ Failed to create credential: {r.status_code}")
        try:
            error = r.json()
            print(f"Error: {json.dumps(error, indent=2)}")
        except:
            print(f"Response: {r.text}")
        return False

def main():
    print("="*80)
    print("  PURVIEW CREDENTIAL DIAGNOSTIC & FIX")
    print("="*80)
    
    # Step 1: Check API access
    has_access = check_scan_api_access()
    
    if not has_access:
        print("\n" + "="*80)
        print("  ⚠️  NO SCAN API ACCESS")
        print("="*80)
        print("""
To fix this, you need to add the 'Data Source Administrator' role:

1. Go to Azure Portal: portal.azure.com
2. Navigate to: Purview → prviewacc → Data Map
3. Click: Collections → Root Collection
4. Click: Role assignments
5. Add role: Data Source Administrator
6. Add your user account
7. Wait 5-10 minutes for permissions to propagate
8. Run this script again
""")
        return
    
    # Step 2: List existing credentials
    creds = list_credentials()
    
    # Step 3: Check purview-msi specifically
    if any(c.get('name') == 'purview-msi' for c in creds):
        print("\n⚠️  Found existing purview-msi credential with potential issues")
        print("   Option 1: Delete and recreate")
        print("   Option 2: Check details first")
        
        # Get details
        details = get_credential_details('purview-msi')
        
        if details:
            # Check if it's misconfigured
            kind = details.get('kind')
            props = details.get('properties', {})
            
            if kind != 'ManagedIdentity':
                print(f"\n❌ Wrong credential type: {kind} (expected ManagedIdentity)")
                print("   Recommending recreation...")
                delete_credential('purview-msi')
                create_managed_identity_credential('purview-msi')
            else:
                print("\n✅ Credential type is correct (ManagedIdentity)")
                print("   The warning icon may be a portal UI issue")
                print("   Credential should work for scanning")
    else:
        print("\n✅ No existing purview-msi credential found")
        print("   Creating new managed identity credential...")
        create_managed_identity_credential('purview-msi')
    
    # Step 4: Final verification
    print("\n" + "="*80)
    print("  FINAL VERIFICATION")
    print("="*80)
    
    final_creds = list_credentials()
    
    if any(c.get('name') == 'purview-msi' and c.get('kind') == 'ManagedIdentity' for c in final_creds):
        print("\n✅ SUCCESS: purview-msi credential is properly configured")
        print("\nNext steps:")
        print("1. Register data sources (SQL Server, Fabric workspace)")
        print("2. Create scans using the purview-msi credential")
        print("3. Run scans to populate the catalog")
    else:
        print("\n⚠️  Credential setup incomplete")
        print("   Check the error messages above for details")

if __name__ == '__main__':
    main()
