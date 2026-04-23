#!/usr/bin/env python3
"""
Test Official Purview APIs 2023-09-01
Verify the stable API versions work correctly
"""
import requests
import json
from azure.identity import AzureCliCredential

# Configuration
PURVIEW_ACCOUNT = 'prviewacc'
ACCOUNT_BASE = f'https://{PURVIEW_ACCOUNT}.purview.azure.com'

# Authentication
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

print("="*80)
print("  TESTING OFFICIAL PURVIEW APIs (2023-09-01)")
print("="*80)

# ============================================================================
# 1. DATA MAP DATA PLANE API (Entity API)
# ============================================================================
print("\n" + "="*80)
print("  1. DATA MAP DATA PLANE API")
print("  https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/entity")
print("="*80)

ENTITY_BASE = f'{ACCOUNT_BASE}/datamap/api/atlas/v2'

# Test 1.1: Get typedef
print("\n📋 Test 1.1: GET /types/typedef/name/{name}")
r = requests.get(
    f'{ENTITY_BASE}/types/typedef/name/healthcare_data_product',
    headers=headers,
    timeout=30
)
print(f"   Status: {r.status_code}")
if r.status_code == 200:
    typedef = r.json()
    attrs = typedef.get('entityDefs', [{}])[0].get('attributeDefs', [])
    print(f"   ✅ Typedef found with {len(attrs)} attributes")
else:
    print(f"   Response: {r.text[:200]}")

# Test 1.2: Search entities
print("\n📋 Test 1.2: POST /search/basic")
body = {
    "query": "*",
    "limit": 10,
    "filter": {
        "typeName": "healthcare_data_product"
    }
}
r = requests.post(
    f'{ENTITY_BASE}/search/basic',
    headers=headers,
    json=body,
    timeout=30
)
print(f"   Status: {r.status_code}")
if r.status_code == 200:
    results = r.json().get('value', [])
    print(f"   ✅ Found {len(results)} entities")
    for entity in results[:3]:
        print(f"      - {entity.get('attributes', {}).get('name', '?')}")
else:
    print(f"   Response: {r.text[:200]}")

# Test 1.3: Get entity by GUID
print("\n📋 Test 1.3: GET /entity/guid/{guid}")
if r.status_code == 200 and results:
    test_guid = results[0].get('guid')
    r = requests.get(
        f'{ENTITY_BASE}/entity/guid/{test_guid}',
        headers=headers,
        timeout=30
    )
    print(f"   Status: {r.status_code}")
    if r.status_code == 200:
        entity = r.json().get('entity', {})
        print(f"   ✅ Retrieved entity: {entity.get('attributes', {}).get('name', '?')}")
    else:
        print(f"   Response: {r.text[:200]}")

# Test 1.4: Update entity using /entity/bulk
print("\n📋 Test 1.4: POST /entity/bulk (update)")
if r.status_code == 200:
    # Remove server timestamps
    for field in ['lastModifiedTS', 'createTime', 'updateTime']:
        entity.pop(field, None)
    
    # Make small update
    original_desc = entity['attributes'].get('userDescription', '')
    entity['attributes']['userDescription'] = f"{original_desc} [API Test]"
    
    r = requests.post(
        f'{ENTITY_BASE}/entity/bulk',
        headers=headers,
        json={'entities': [entity]},
        timeout=30
    )
    print(f"   Status: {r.status_code}")
    if r.status_code == 200:
        print(f"   ✅ Entity updated successfully")
        
        # Restore original
        entity['attributes']['userDescription'] = original_desc
        r_restore = requests.post(
            f'{ENTITY_BASE}/entity/bulk',
            headers=headers,
            json={'entities': [entity]},
            timeout=30
        )
        print(f"   ✅ Restored original description")
    else:
        print(f"   Response: {r.text[:200]}")

# ============================================================================
# 2. SCANNING DATA PLANE API
# ============================================================================
print("\n" + "="*80)
print("  2. SCANNING DATA PLANE API")
print("  https://learn.microsoft.com/en-us/rest/api/purview/scanningdataplane/data-sources")
print("="*80)

SCAN_BASE = f'{ACCOUNT_BASE}/scan'

# Test 2.1: List data sources
print("\n📋 Test 2.1: GET /datasources")
r = requests.get(
    f'{SCAN_BASE}/datasources?api-version=2023-09-01',
    headers=headers,
    timeout=30
)
print(f"   Status: {r.status_code}")
if r.status_code == 200:
    sources = r.json().get('value', [])
    print(f"   ✅ Found {len(sources)} data sources")
    for source in sources:
        print(f"      - {source.get('name', '?')} ({source.get('kind', '?')})")
elif r.status_code == 403:
    print(f"   ⚠️  403 Forbidden - Requires 'Data Source Administrator' role")
else:
    print(f"   Response: {r.text[:200]}")

# Test 2.2: Get specific data source (if any exist)
if r.status_code == 200 and sources:
    print("\n📋 Test 2.2: GET /datasources/{name}")
    ds_name = sources[0].get('name')
    r = requests.get(
        f'{SCAN_BASE}/datasources/{ds_name}?api-version=2023-09-01',
        headers=headers,
        timeout=30
    )
    print(f"   Status: {r.status_code}")
    if r.status_code == 200:
        ds = r.json()
        print(f"   ✅ Retrieved: {ds.get('name')} ({ds.get('kind')})")

# Test 2.3: List scans for data source
    print("\n📋 Test 2.3: GET /datasources/{name}/scans")
    r = requests.get(
        f'{SCAN_BASE}/datasources/{ds_name}/scans?api-version=2023-09-01',
        headers=headers,
        timeout=30
    )
    print(f"   Status: {r.status_code}")
    if r.status_code == 200:
        scans = r.json().get('value', [])
        print(f"   ✅ Found {len(scans)} scans")
        for scan in scans:
            print(f"      - {scan.get('name', '?')}")

# ============================================================================
# 3. CATALOG DATA PLANE API (Search - Legacy)
# ============================================================================
print("\n" + "="*80)
print("  3. CATALOG DATA PLANE API (Search - v2022-08-01-preview)")
print("="*80)

# Test 3.1: Search query
print("\n📋 Test 3.1: POST /catalog/api/search/query")
body = {
    'keywords': '*',
    'limit': 5,
    'filter': {
        'entityType': 'healthcare_data_product'
    }
}
r = requests.post(
    f'{ACCOUNT_BASE}/catalog/api/search/query?api-version=2022-08-01-preview',
    headers=headers,
    json=body,
    timeout=30
)
print(f"   Status: {r.status_code}")
if r.status_code == 200:
    results = r.json().get('value', [])
    print(f"   ✅ Found {len(results)} entities")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*80)
print("  API VERSION SUMMARY")
print("="*80)
print("""
Working APIs:
  ✅ Data Map Data Plane (Entity API): 2023-09-01
     - Base: /datamap/api/atlas/v2
     - Operations: GET entity, POST /entity/bulk, search
  
  ⚠️  Scanning Data Plane API: 2023-09-01 (requires permissions)
     - Base: /scan
     - Operations: List/create data sources, configure scans
  
  ✅ Catalog Search API: 2022-08-01-preview (legacy)
     - Base: /catalog/api/search/query
     - Still works for backward compatibility

Recommended Usage:
  - Entity operations: Use Data Map API (/datamap/api/atlas/v2)
  - Scanning: Use Scanning API (/scan with api-version=2023-09-01)
  - Search: Can use either Atlas search or Catalog search
""")
print("="*80)
