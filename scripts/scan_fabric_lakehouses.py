#!/usr/bin/env python3
"""
Scan Fabric Lakehouses into Purview Catalog
Registers and scans Bronze, Silver, Gold lakehouses from HCA workspace
"""
import requests
import time
from azure.identity import AzureCliCredential

# ══════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════
PURVIEW_ACCOUNT = "prviewacc"
TENANT_ID = "71c4b6d5-0065-4c6c-a125-841a582754eb"
SUBSCRIPTION_ID = "5b44c9f3-bbe7-464c-aa3e-562726a12004"
RESOURCE_GROUP = "purview"

# Fabric Workspace
FABRIC_WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
FABRIC_WORKSPACE_NAME = "Healthcare Analytics"

# Lakehouses in workspace
LAKEHOUSES = [
    {"id": "d1d45d67-3e1c-456d-8dca-95a23dd64e59", "name": "Bronze", "description": "Raw ingested data from FHIR, DICOM, GMS"},
    {"id": "0effc6f5-f26d-4e02-b2f8-1a4d4d2f8257", "name": "Silver", "description": "Cleaned and transformed healthcare data"},
    {"id": "2960eef0-5de6-4117-80b1-6ee783cdaeec", "name": "Gold", "description": "Business-ready analytics tables and ML features"},
]

# API Endpoints
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
ATLAS_API = f"{PURVIEW_ENDPOINT}/catalog/api/atlas/v2"
SCAN_API = f"{PURVIEW_ENDPOINT}/scan"

# ══════════════════════════════════════════════════════════
# AUTHENTICATION
# ══════════════════════════════════════════════════════════
print("🔐 Authenticating with Azure...")
credential = AzureCliCredential(process_timeout=30)
token = credential.get_token("https://purview.azure.net/.default").token
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════
def sep(title=""):
    """Print section separator"""
    print(f"\n{'═'*70}")
    if title:
        print(f"  {title}")
        print('═'*70)

def check_lakehouse_entities():
    """Check if lakehouse entities already exist in Purview"""
    sep("CHECKING EXISTING LAKEHOUSE ENTITIES")
    
    body = {
        "keywords": "lakehouse",
        "limit": 50,
        "filter": {}
    }
    
    try:
        r = requests.post(
            f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
            headers=headers,
            json=body,
            timeout=30
        )
        
        if r.status_code == 200:
            results = r.json().get("value", [])
            print(f"✅ Found {len(results)} lakehouse-related entities")
            
            for entity in results[:10]:
                name = entity.get("name", "?")
                entity_type = entity.get("entityType", "?")
                print(f"   - {name} ({entity_type})")
            
            return results
        else:
            print(f"⚠️  Search returned: {r.status_code}")
            return []
            
    except Exception as e:
        print(f"❌ Error searching: {e}")
        return []

def register_fabric_data_source():
    """Register Fabric workspace as a data source in Purview"""
    sep("REGISTERING FABRIC DATA SOURCE")
    
    # Check if already registered
    try:
        r = requests.get(
            f"{SCAN_API}/datasources?api-version=2022-07-01-preview",
            headers=headers,
            timeout=30
        )
        
        if r.status_code == 200:
            sources = r.json().get("value", [])
            for source in sources:
                if "fabric" in source.get("name", "").lower():
                    print(f"✅ Fabric data source already registered: {source['name']}")
                    return source.get("name")
        
        # Register new data source
        data_source_name = f"FabricHCA"
        
        payload = {
            "kind": "Fabric",
            "name": data_source_name,
            "properties": {
                "workspaceId": FABRIC_WORKSPACE_ID,
                "workspaceName": FABRIC_WORKSPACE_NAME,
                "endpoint": f"https://msit.powerbi.com/groups/{FABRIC_WORKSPACE_ID}",
                "subscriptionId": SUBSCRIPTION_ID,
                "resourceGroup": RESOURCE_GROUP,
                "location": "westeurope"
            }
        }
        
        r = requests.put(
            f"{SCAN_API}/datasources/{data_source_name}?api-version=2022-07-01-preview",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if r.status_code in [200, 201]:
            print(f"✅ Registered Fabric data source: {data_source_name}")
            return data_source_name
        else:
            print(f"⚠️  Could not register (HTTP {r.status_code})")
            print(f"   Response: {r.text[:200]}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def create_scan_for_lakehouses(data_source_name):
    """Create scan configuration for Fabric lakehouses"""
    sep("CREATING SCAN CONFIGURATION")
    
    if not data_source_name:
        print("❌ No data source name provided")
        return False
    
    scan_name = f"FabricLakehouseScan"
    
    payload = {
        "kind": "Fabric",
        "name": scan_name,
        "properties": {
            "scanRulesetName": "FabricLakehouse",
            "scanRulesetType": "System",
            "resourceTypes": ["PowerBIDataflow", "PowerBIDataset", "Lakehouse"],
            "collection": {
                "referenceName": PURVIEW_ACCOUNT,
                "type": "CollectionReference"
            }
        }
    }
    
    try:
        r = requests.put(
            f"{SCAN_API}/datasources/{data_source_name}/scans/{scan_name}?api-version=2022-07-01-preview",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if r.status_code in [200, 201]:
            print(f"✅ Created scan configuration: {scan_name}")
            return scan_name
        else:
            print(f"⚠️  Could not create scan (HTTP {r.status_code})")
            print(f"   Response: {r.text[:200]}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def trigger_scan(data_source_name, scan_name):
    """Trigger the scan to run"""
    sep("TRIGGERING SCAN")
    
    if not data_source_name or not scan_name:
        print("❌ Missing data source or scan name")
        return False
    
    run_id = f"manual_{int(time.time())}"
    
    try:
        r = requests.put(
            f"{SCAN_API}/datasources/{data_source_name}/scans/{scan_name}/runs/{run_id}?api-version=2022-07-01-preview",
            headers=headers,
            json={"scanLevel": "Full"},
            timeout=30
        )
        
        if r.status_code in [200, 201, 202]:
            print(f"✅ Scan triggered successfully")
            print(f"   Run ID: {run_id}")
            print(f"\n⏳ Scan is running... This may take 5-15 minutes.")
            print(f"\n📊 Check status in Purview Portal:")
            print(f"   https://web.purview.azure.com/resource/{PURVIEW_ACCOUNT}")
            print(f"   → Data Map → Sources → {data_source_name} → Scans")
            return True
        else:
            print(f"⚠️  Could not trigger scan (HTTP {r.status_code})")
            print(f"   Response: {r.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def create_lakehouse_entities_via_atlas():
    """Create lakehouse entities directly via Atlas API as fallback"""
    sep("CREATING LAKEHOUSE ENTITIES VIA ATLAS API (FALLBACK)")
    
    created = 0
    
    for lakehouse in LAKEHOUSES:
        entity = {
            "typeName": "fabric_lakehouse",
            "attributes": {
                "name": lakehouse["name"],
                "qualifiedName": f"lakehouse:{FABRIC_WORKSPACE_ID}:{lakehouse['id']}",
                "description": lakehouse["description"],
                "workspaceId": FABRIC_WORKSPACE_ID,
                "lakehouseId": lakehouse["id"]
            }
        }
        
        try:
            r = requests.post(
                f"{ATLAS_API}/entity",
                headers=headers,
                json={"entity": entity},
                timeout=30
            )
            
            if r.status_code in [200, 201]:
                guid = r.json().get("guidAssignments", {}).get("-1", "?")
                print(f"✅ Created {lakehouse['name']} (GUID: {guid})")
                created += 1
            elif r.status_code == 409:
                print(f"ℹ️  {lakehouse['name']} already exists")
            else:
                print(f"⚠️  Could not create {lakehouse['name']} (HTTP {r.status_code})")
                
        except Exception as e:
            print(f"❌ Error creating {lakehouse['name']}: {e}")
    
    return created

# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    sep("🔍 FABRIC LAKEHOUSE SCANNING FOR PURVIEW")
    
    # Step 1: Check existing
    existing = check_lakehouse_entities()
    
    # Step 2: Register Fabric data source
    data_source_name = register_fabric_data_source()
    
    if data_source_name:
        # Step 3: Create scan configuration
        scan_name = create_scan_for_lakehouses(data_source_name)
        
        if scan_name:
            # Step 4: Trigger scan
            trigger_scan(data_source_name, scan_name)
        else:
            print("\n⚠️  Could not create scan configuration")
            print("   Trying fallback: Create entities directly via Atlas API")
            create_lakehouse_entities_via_atlas()
    else:
        print("\n⚠️  Could not register Fabric data source")
        print("   Trying fallback: Create entities directly via Atlas API")
        create_lakehouse_entities_via_atlas()
    
    sep("✅ SCAN PROCESS COMPLETE")
    print("\n📝 Next steps:")
    print("   1. Wait 5-15 minutes for scan to complete")
    print("   2. Check Purview Portal → Data Map → Sources")
    print("   3. Run verify_all_purview.py to confirm lakehouse assets")
    print(f"   4. View in portal: https://web.purview.azure.com/resource/{PURVIEW_ACCOUNT}")

if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
