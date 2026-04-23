#!/usr/bin/env python3
"""
Update by keeping entire entity structure
"""
import requests
import json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

ATLAS = 'https://prviewacc.purview.azure.com/catalog/api/atlas/v2'
GUID = 'e7010e17-8987-4c31-af29-b06fcf4b2142'

print("Testing update by preserving full entity structure...\n")

# Get full entity
r = requests.get(f'{ATLAS}/entity/guid/{GUID}', headers=h, timeout=30)
full_response = r.json()
entity = full_response.get('entity', {})

print(f"Original entity keys: {list(entity.keys())}")
print(f"Original attributes: {list(entity.get('attributes', {}).keys())}")

# Only remove timestamp fields that should be server-generated
fields_to_remove = ['lastModifiedTS', 'createTime', 'updateTime']
for field in fields_to_remove:
    entity.pop(field, None)

# Update ONE attribute
entity['attributes']['userDescription'] = 'TEST UPDATE: Preserved full structure'

print(f"\nEntity after modification:")
print(f"  Keys: {list(entity.keys())}")
print(f"  Status: {entity.get('status')}")
print(f"  Version: {entity.get('version')}")

print(f"\nSending update...")
r_update = requests.post(
    f'{ATLAS}/entity',
    headers=h,
    json={'entities': [entity]},
    timeout=30
)

print(f"Status: {r_update.status_code}")
print(f"Response: {r_update.text[:500]}")

# Try alternative: POST to /entity/bulk
print("\n" + "="*80)
print("Trying POST /entity/bulk/update")
print("="*80)

r_bulk = requests.post(
    f'{ATLAS}/entity/bulk',
    headers=h,
    json={'entities': [entity]},
    timeout=30
)
print(f"Status: {r_bulk.status_code}")
print(f"Response: {r_bulk.text[:500]}")

# Check what reference entities exist
print("\n" + "="*80)
print("Checking referredEntities in original response")
print("="*80)

referred = full_response.get('referredEntities', {})
print(f"Found {len(referred)} referred entities")
if referred:
    print("Referred entities:")
    for guid, ref_entity in list(referred.items())[:3]:
        print(f"  {guid}: {ref_entity.get('typeName')} - {ref_entity.get('attributes', {}).get('name', '?')}")
