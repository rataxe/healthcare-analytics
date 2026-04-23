#!/usr/bin/env python3
import requests, json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

ACCT = 'https://prviewacc.purview.azure.com'
ATLAS = f'{ACCT}/catalog/api/atlas/v2'

# Get one data product to see its structure
guid = 'e7010e17-8987-4c31-af29-b06fcf4b2142'

print('Fetching data product entity...')
r = requests.get(f'{ATLAS}/entity/guid/{guid}', headers=h, timeout=30)
print(f'GET status: {r.status_code}')

if r.status_code == 200:
    entity = r.json().get('entity', {})
    attrs = entity.get('attributes', {})
    
    print(f"\nEntity type: {entity.get('typeName')}")
    print(f'Attributes:')
    for k, v in list(attrs.items())[:10]:
        val = str(v)[:50] if v else 'None'
        print(f'  {k}: {val}')
    
    # Try to update with POST /entity (standard Atlas API method)
    print('\n--- Testing UPDATE ---')
    
    # Atlas uses POST /entity for both create and update
    entity['attributes']['userDescription'] = 'TEST UPDATE - Critical Elements and OKRs'
    
    r_update = requests.post(f'{ATLAS}/entity', headers=h, json={'entities': [entity]}, timeout=30)
    print(f'POST /entity: {r_update.status_code}')
    
    if r_update.status_code in [200, 201]:
        print('  ✅ Update successful!')
        updated = r_update.json()
        print(f'  Updated GUID: {updated.get("guidAssignments", {})}')
    else:
        print(f'  ❌ Error: {r_update.text[:300]}')
