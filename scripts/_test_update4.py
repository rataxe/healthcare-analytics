#!/usr/bin/env python3
import requests, json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

ACCT = 'https://prviewacc.purview.azure.com'
ATLAS = f'{ACCT}/catalog/api/atlas/v2'

guid = 'e7010e17-8987-4c31-af29-b06fcf4b2142'

print('Getting existing entity...')
r = requests.get(f'{ATLAS}/entity/guid/{guid}', headers=h, timeout=30)

if r.status_code == 200:
    entity_data = r.json()
    entity = entity_data.get('entity', {})
    
    # Update attributes while keeping all existing required fields
    entity['attributes']['userDescription'] = 'UPDATED: Omfattar patientdemografi, diagnoser (ICD-10), läkemedel (ATC), laboratorieprov (LOINC) och radiologiska studier (DICOM). Används för prediktiv analys.'
    entity['attributes']['criticalElements'] = json.dumps([
        {'name': 'Swedish Personnummer', 'type': 'PII', 'sensitivity': 'High', 'retention': '10 years'},
        {'name': 'ICD-10 Codes', 'type': 'Clinical', 'sensitivity': 'High'},
        {'name': 'ATC Codes', 'type': 'Clinical', 'sensitivity': 'High'}
    ])
    entity['attributes']['okrs'] = json.dumps([
        {
            'objective': 'Förbättra datakvalitet',
            'key_results': ['>=95% completeness', '<=2% error rate', 'Daily validation']
        }
    ])
    
    # Remove read-only fields that might cause issues
    for field in ['lastModifiedTS', 'createTime', 'updateTime', 'isIndexed', 'version']:
        entity.pop(field, None)
    
    # Remove relationshipAttributes which can't be updated directly
    entity.pop('relationshipAttributes', None)
    
    print('\nAttempting update...')
    print(f'Entity type: {entity.get("typeName")}')
    print(f'GUID: {entity.get("guid")}')
    print(f'Attributes: {list(entity.get("attributes", {}).keys())[:10]}')
    
    r2 = requests.post(
        f'{ATLAS}/entity',
        headers=h,
        json={'entities': [entity]},
        timeout=30
    )
    
    print(f'\nPOST /entity: {r2.status_code}')
    
    if r2.status_code in [200, 201]:
        print('✅ Update successful!')
        result = r2.json()
        print(f'Result: {json.dumps(result, indent=2)[:500]}')
    else:
        print(f'❌ Error: {r2.text}')
