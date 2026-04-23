#!/usr/bin/env python3
"""
Test tenant-based Atlas endpoint for entity operations
"""
import requests
import json
from azure.identity import AzureCliCredential

# Auth
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Test both endpoints
ACCOUNT_ENDPOINT = 'https://prviewacc.purview.azure.com/catalog/api/atlas/v2'
TENANT_ENDPOINT = 'https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com/catalog/api/atlas/v2'

TEST_GUID = 'e7010e17-8987-4c31-af29-b06fcf4b2142'  # Klinisk Patientanalys

print("=" * 80)
print("Testing Atlas Endpoints - Account vs Tenant")
print("=" * 80)

for endpoint_name, endpoint in [('ACCOUNT', ACCOUNT_ENDPOINT), ('TENANT', TENANT_ENDPOINT)]:
    print(f"\n{'='*80}")
    print(f"🔍 Testing {endpoint_name} Endpoint")
    print(f"   {endpoint}")
    print('='*80)
    
    # Test 1: GET glossary
    try:
        r1 = requests.get(f'{endpoint}/glossary', headers=headers, timeout=30)
        print(f'\n1. GET /glossary: {r1.status_code}')
        if r1.status_code == 200:
            data = r1.json()
            glossaries = data if isinstance(data, list) else [data]
            print(f'   ✅ Found {len(glossaries)} glossary/glossaries')
        else:
            print(f'   ❌ Error: {r1.text[:200]}')
    except Exception as e:
        print(f'   ❌ Exception: {e}')
    
    # Test 2: GET entity by GUID
    try:
        r2 = requests.get(f'{endpoint}/entity/guid/{TEST_GUID}', headers=headers, timeout=30)
        print(f'\n2. GET /entity/guid/{TEST_GUID}: {r2.status_code}')
        if r2.status_code == 200:
            entity = r2.json().get('entity', {})
            print(f'   ✅ Entity type: {entity.get("typeName")}')
            print(f'   ✅ Entity name: {entity.get("attributes", {}).get("name")}')
        else:
            print(f'   ❌ Error: {r2.text[:200]}')
    except Exception as e:
        print(f'   ❌ Exception: {e}')
    
    # Test 3: POST /entity (update)
    try:
        # First get the entity
        r_get = requests.get(f'{endpoint}/entity/guid/{TEST_GUID}', headers=headers, timeout=30)
        if r_get.status_code == 200:
            entity = r_get.json().get('entity', {})
            
            # Clean entity for update
            for field in ['lastModifiedTS', 'createTime', 'updateTime', 'isIndexed', 
                         'version', 'relationshipAttributes']:
                entity.pop(field, None)
            
            # Update a simple attribute
            entity['attributes']['userDescription'] = 'TENANT TEST: Updated via tenant endpoint'
            
            r3 = requests.post(
                f'{endpoint}/entity',
                headers=headers,
                json={'entities': [entity]},
                timeout=30
            )
            print(f'\n3. POST /entity (update): {r3.status_code}')
            if r3.status_code in [200, 201]:
                print(f'   ✅ Update successful!')
                result = r3.json()
                if 'mutatedEntities' in result:
                    print(f'   ✅ Mutated: {result.get("mutatedEntities", {})}')
            else:
                print(f'   ❌ Error: {r3.text[:300]}')
        else:
            print(f'\n3. POST /entity: SKIPPED (could not GET entity)')
    except Exception as e:
        print(f'   ❌ Exception: {e}')
    
    # Test 4: Search API
    try:
        search_body = {
            'keywords': '*',
            'limit': 5,
            'filter': {'entityType': 'healthcare_data_product'}
        }
        # Use account URL for search API (no tenant endpoint)
        search_url = 'https://prviewacc.purview.azure.com/catalog/api/search/query?api-version=2022-08-01-preview'
        r4 = requests.post(search_url, headers=headers, json=search_body, timeout=30)
        print(f'\n4. POST /search/query: {r4.status_code}')
        if r4.status_code == 200:
            results = r4.json().get('value', [])
            print(f'   ✅ Found {len(results)} data products')
        else:
            print(f'   ❌ Error: {r4.text[:200]}')
    except Exception as e:
        print(f'   ❌ Exception: {e}')

print("\n" + "="*80)
print("Test Complete")
print("="*80)
