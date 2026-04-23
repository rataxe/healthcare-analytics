#!/usr/bin/env python3
import requests, json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

ACCT = 'https://prviewacc.purview.azure.com'
ATLAS = f'{ACCT}/catalog/api/atlas/v2'

guid = 'e7010e17-8987-4c31-af29-b06fcf4b2142'

print('Testing Atlas partial attribute update...')

# Method 1: PUT /entity/guid/{guid} with partial attributes
attrs_update = {
    'userDescription': 'UPDATED: Omfattar patientdemografi, diagnoser (ICD-10), läkemedel (ATC)',
    'criticalElements': '["Swedish Personnummer (PII)", "ICD-10 Codes (Clinical)", "ATC Codes (Clinical)"]',
    'okrs': '[{"objective": "Förbättra datakvalitet", "key_results": ["95% completeness", "2% error rate"]}]'
}

r1 = requests.put(
    f'{ATLAS}/entity/guid/{guid}',
    headers=h,
    json=attrs_update,
    timeout=30
)
print(f'\nMethod 1 - PUT /entity/guid/{{guid}}: {r1.status_code}')
if r1.status_code not in [200, 201, 204]:
    print(f'  Error: {r1.text[:300]}')
else:
    print('  ✅ Success!')

# Method 2: POST /entity/bulk with partial attrs
r2 = requests.post(
    f'{ATLAS}/entity/bulk',
    headers=h,
    json={'entities': [{
        'guid': guid,
        'typeName': 'healthcare_data_product',
        'attributes': attrs_update
    }]},
    timeout=30
)
print(f'\nMethod 2 - POST /entity/bulk: {r2.status_code}')
if r2.status_code not in [200, 201]:
    print(f'  Error: {r2.text[:300]}')
else:
    print('  ✅ Success!')

# Method 3: POST /entity/guid/{guid}/classifications (try business metadata)
r3 = requests.post(
    f'{ATLAS}/entity/guid/{guid}/businessmetadata',
    headers=h,
    json={'criticalElements': attrs_update['criticalElements']},
    timeout=30
)
print(f'\nMethod 3 - POST /businessmetadata: {r3.status_code}')
if r3.status_code not in [200, 201, 204]:
    print(f'  Error: {r3.text[:300]}')
else:
    print('  ✅ Success!')

# Method 4: Try using /entity/guid/{guid}/attributes endpoint if exists
for attr_name in ['userDescription', 'criticalElements']:
    r4 = requests.put(
        f'{ATLAS}/entity/guid/{guid}/attribute/{attr_name}',
        headers=h,
        json={'value': attrs_update[attr_name]},
        timeout=30
    )
    print(f'\nMethod 4 - PUT /attribute/{attr_name}: {r4.status_code}')
    if r4.status_code in [200, 201, 204]:
        print(f'  ✅ Success!')
        break
