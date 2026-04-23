#!/usr/bin/env python3
"""
Update data products using ONLY existing attributes
"""
import requests
import json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

ATLAS = 'https://prviewacc.purview.azure.com/catalog/api/atlas/v2'
GUID = 'e7010e17-8987-4c31-af29-b06fcf4b2142'

print("Testing update with EXISTING attributes only...\n")

# Get current entity
r = requests.get(f'{ATLAS}/entity/guid/{GUID}', headers=h, timeout=30)
entity = r.json().get('entity', {})

# Create update with ONLY existing typedef attributes
update_entity = {
    'guid': GUID,
    'typeName': 'healthcare_data_product',
    'attributes': {
        'qualifiedName': entity['attributes']['qualifiedName'],
        'name': entity['attributes']['name'],
        'description': entity['attributes']['description'],
        'userDescription': 'UPPDATERAT: Omfattar patientdemografi, diagnoser (ICD-10), läkemedel (ATC), laboratorieprov (LOINC) och radiologiska studier (DICOM). Innehåller data för 10,000 patienter med fokus på vårdkvalitet och prediktiv analys.',
        'product_type': 'Analytics',
        'product_status': 'Published',
        'product_owners': 'Healthcare Analytics Team | Clinical Data Stewards',
        'sla': 'Daglig uppdatering, <1h latens, 99.5% tillgänglighet, 10 års retention',
        'use_cases': 'LOS-prediktion | Återinläggningsrisk | Charlson Comorbidity | Avdelningsstatistik | GDPR-compliance | Kliniskt beslutsfattande',
        'tables': 'patients, encounters, diagnoses, medications, vitals_labs, observations, dicom_studies',
        'quality_score': 0.95
    }
}

print("Updating entity with existing attributes...")
print(f"  - userDescription: {update_entity['attributes']['userDescription'][:50]}...")
print(f"  - use_cases: {update_entity['attributes']['use_cases'][:60]}...")
print(f"  - product_owners: {update_entity['attributes']['product_owners']}")
print(f"  - quality_score: {update_entity['attributes']['quality_score']}")

r_update = requests.post(
    f'{ATLAS}/entity',
    headers=h,
    json={'entities': [update_entity]},
    timeout=30
)

print(f"\nPOST /entity: {r_update.status_code}")

if r_update.status_code in [200, 201]:
    print("✅ SUCCESS! Entity updated!")
    result = r_update.json()
    if 'mutatedEntities' in result:
        mutated = result['mutatedEntities']
        print(f"\nMutated entities:")
        for key, guids in mutated.items():
            print(f"  {key}: {len(guids)} entities")
    
    # Verify update
    r_verify = requests.get(f'{ATLAS}/entity/guid/{GUID}', headers=h, timeout=30)
    if r_verify.status_code == 200:
        updated = r_verify.json().get('entity', {})
        print(f"\nVerified updated attributes:")
        print(f"  userDescription: {updated['attributes'].get('userDescription', 'N/A')[:60]}...")
        print(f"  quality_score: {updated['attributes'].get('quality_score')}")
else:
    print(f"❌ FAILED")
    print(f"Error: {r_update.text}")
