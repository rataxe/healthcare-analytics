#!/usr/bin/env python3
import requests, json
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
token = cred.get_token('https://purview.azure.net/.default').token
H = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
BASE = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

# Check all products' domain field
r = requests.get(f'{BASE}/dataProducts?api-version={VER}', headers=H, timeout=30)
prods = r.json().get('value', [])
print("=== DATA PRODUCTS DOMAIN FIELDS ===")
for p in prods:
    name = p.get('name')
    dom = p.get('domain', 'NONE')
    print(f'  {name} | domain: {dom}')

# Try PUT on first product
print()
prod = prods[0]
pid = prod['id']
print(f'Testing PUT on: {prod["name"]}')
body = {k: v for k, v in prod.items() if k not in ('systemData', 'additionalProperties')}
body['description'] = 'Patientdemografi, diagnoser (ICD-10), lakemedel (ATC), labbprov (LOINC) och radiologiska studier (DICOM). 10 000 patienter.'
r2 = requests.put(f'{BASE}/dataProducts/{pid}?api-version={VER}', headers=H, json=body, timeout=30)
print(f'  PUT status: {r2.status_code}')
if r2.content:
    try:
        data = r2.json()
        print(json.dumps(data, indent=2)[:600])
    except Exception:
        print(r2.text[:300])

# Try terms endpoint with domain field
print()
print("=== TESTING TERM CREATION WITH DOMAIN ===")
domain_id = prods[0].get('domain', '')
print(f'Using domain_id: {domain_id}')
term_body = {
    'name': 'TestTerm_Delete',
    'description': 'Test term - please delete',
    'status': 'Draft',
    'domain': domain_id
}
r3 = requests.post(f'{BASE}/terms?api-version={VER}', headers=H, json=term_body, timeout=30)
print(f'  POST /terms status: {r3.status_code}')
if r3.content:
    try:
        print(json.dumps(r3.json(), indent=2)[:400])
    except Exception:
        print(r3.text[:300])
