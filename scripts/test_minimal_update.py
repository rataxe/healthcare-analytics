#!/usr/bin/env python3
"""
Test minimal entity update with only required fields
"""
import requests
import json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

ATLAS = 'https://prviewacc.purview.azure.com/catalog/api/atlas/v2'
GUID = 'e7010e17-8987-4c31-af29-b06fcf4b2142'

print("Testing minimal entity update...\n")

# Get entity to see its structure
r = requests.get(f'{ATLAS}/entity/guid/{GUID}', headers=h, timeout=30)
entity = r.json().get('entity', {})

print(f"Current entity attributes:")
for key, value in entity.get('attributes', {}).items():
    val_str = str(value)[:50] if value is not None else 'None'
    print(f"  {key}: {val_str}")

# Method 1: Minimal entity with only guid, typeName, and changed attributes
print("\n" + "="*80)
print("Method 1: Minimal entity (guid + typeName + attributes)")
print("="*80)

minimal = {
    'guid': GUID,
    'typeName': 'healthcare_data_product',
    'attributes': {
        'qualifiedName': entity['attributes']['qualifiedName'],
        'name': entity['attributes']['name'],
        'userDescription': 'TEST MINIMAL UPDATE',
        'criticalElements': json.dumps([
            {'name': 'Swedish Personnummer', 'type': 'PII', 'sensitivity': 'High'}
        ])
    }
}

r1 = requests.post(f'{ATLAS}/entity', headers=h, json={'entities': [minimal]}, timeout=30)
print(f"Status: {r1.status_code}")
if r1.status_code in [200, 201]:
    print("✅ Success!")
    print(json.dumps(r1.json(), indent=2)[:500])
else:
    print(f"❌ Error: {r1.text}")

# Method 2: Update using PUT to specific attribute
print("\n" + "="*80)
print("Method 2: Try different API paths")
print("="*80)

# Try partial update endpoint
r2 = requests.put(
    f'{ATLAS}/entity/guid/{GUID}',
    headers=h,
    json={
        'attributes': {
            'userDescription': 'TEST PUT UPDATE'
        }
    },
    timeout=30
)
print(f"PUT /entity/guid/{{guid}}: {r2.status_code}")
if r2.status_code not in [200, 201, 204]:
    print(f"  Error: {r2.text[:300]}")

# Method 3: Create relationship to update metadata
print("\n" + "="*80)
print("Method 3: Check if custom attributes are defined in typedef")
print("="*80)

r3 = requests.get(f'{ATLAS}/types/typedef/name/healthcare_data_product', headers=h, timeout=30)
if r3.status_code == 200:
    typedef = r3.json()
    print(f"Entity Type: {typedef.get('name')}")
    attrs = typedef.get('attributeDefs', [])
    print(f"\nDefined attributes ({len(attrs)}):")
    for attr in attrs[:15]:
        print(f"  - {attr.get('name')} ({attr.get('typeName')}) {'[REQUIRED]' if not attr.get('isOptional', True) else ''}")
    
    # Check if criticalElements and okrs exist
    custom_attrs = ['criticalElements', 'okrs', 'use_cases']
    print(f"\nChecking for custom attributes:")
    for ca in custom_attrs:
        exists = any(a.get('name') == ca for a in attrs)
        print(f"  {ca}: {'✅ EXISTS' if exists else '❌ NOT DEFINED'}")
