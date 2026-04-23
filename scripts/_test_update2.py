#!/usr/bin/env python3
import requests, json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

ACCT = 'https://prviewacc.purview.azure.com'
ATLAS = f'{ACCT}/catalog/api/atlas/v2'

guid = 'e7010e17-8987-4c31-af29-b06fcf4b2142'

print('Fetching full entity structure...')
r = requests.get(f'{ATLAS}/entity/guid/{guid}', headers=h, timeout=30)

if r.status_code == 200:
    data = r.json()
    entity = data.get('entity', {})
    
    print(json.dumps(entity, indent=2)[:2000])
    
    # Try partial update using attributes only
    print('\n\n--- Testing PARTIAL UPDATE ---')
    
    partial_entity = {
        'guid': guid,
        'typeName': 'healthcare_data_product',
        'attributes': {
            'qualifiedName': entity['attributes']['qualifiedName'],
            'name': entity['attributes'].get('name', 'Klinisk Patientanalys'),
            'userDescription': 'UPDATED: Omfattar patientdemografi, diagnoser (ICD-10), läkemedel (ATC), laboratorieprov (LOINC) och radiologiska studier (DICOM)',
            'criticalElements': json.dumps([
                {'name': 'Swedish Personnummer', 'type': 'PII', 'sensitivity': 'High'},
                {'name': 'ICD-10 Diagnosis Codes', 'type': 'Clinical', 'sensitivity': 'High'}
            ])
        }
    }
    
    r2 = requests.post(f'{ATLAS}/entity', headers=h, json={'entities': [partial_entity]}, timeout=30)
    print(f'POST /entity (partial): {r2.status_code}')
    
    if r2.status_code in [200, 201]:
        print('  ✅ Update successful!')
    else:
        print(f'  ❌ Error: {r2.text[:500]}')
