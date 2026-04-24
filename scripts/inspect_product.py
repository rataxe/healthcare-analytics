"""Check product endpoint with possible expand params + single asset with product info."""
import requests, json
from azure.identity import AzureCliCredential

t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

# Find MLOps Modellregister
r = requests.get(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, timeout=60)
prods = r.json().get('value', [])
p = next(x for x in prods if x['name'] == 'MLOps Modellregister')
pid = p['id']
print('Full product record:')
print(json.dumps(p, indent=2, ensure_ascii=False))
print()

# Try: product details by id
r2 = requests.get(f'{UNIFIED}/dataProducts/{pid}?api-version={VER}', headers=h, timeout=60)
print(f'GET /dataProducts/{{id}} status: {r2.status_code}')
if r2.status_code == 200:
    print(json.dumps(r2.json(), indent=2, ensure_ascii=False))
print()

# Compare with a product that has assetCount (Klinisk Patientanalys)
p2 = next(x for x in prods if x['name'] == 'Klinisk Patientanalys')
print('Klinisk Patientanalys full record:')
print(json.dumps(p2, indent=2, ensure_ascii=False))
