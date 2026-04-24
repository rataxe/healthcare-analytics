"""Test POST to /dataAssets to link an asset to a data product."""
import requests, json
from azure.identity import AzureCliCredential

t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}', 'Content-Type': 'application/json'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

# Target: "MLOps Modellregister" (has 0 assets)
products = requests.get(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, timeout=60).json().get('value', [])
target = next(p for p in products if p['name'] == 'MLOps Modellregister')
tpid = target['id']
print(f'Target product: {target["name"]}  id={tpid}')

# Take an existing asset from Klinisk Patientanalys to reuse:  "ml_predictions"
source_pid = '334efee5-06c7-4ce3-81a7-1e8dd7570a56'
assets = requests.get(f'{UNIFIED}/dataAssets?dataProductId={source_pid}&api-version={VER}', headers=h, timeout=60).json().get('value', [])
sample = next((a for a in assets if a['name'] == 'ml_predictions'), None)
print(f'\nSample asset to copy:')
print(json.dumps(sample, indent=2, ensure_ascii=False)[:800])

# Try POST to attach to target product
payload = {
    "type": "General",
    "name": sample['name'],
    "description": sample.get('description', ''),
    "source": sample['source'],
    "dataProductId": tpid,
}
print(f'\n=== POST /dataAssets ===')
r = requests.post(f'{UNIFIED}/dataAssets?api-version={VER}', headers=h, json=payload, timeout=60)
print(f'Status: {r.status_code}')
print(f'Body: {r.text[:1000]}')
