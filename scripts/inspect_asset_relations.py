"""Discover correct relationship endpoint between product and asset."""
import requests, json
from azure.identity import AzureCliCredential

t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

# Inspect one asset record in full detail - look for dataProductId field
r = requests.get(f'{UNIFIED}/dataAssets?api-version={VER}', headers=h, timeout=60)
assets = r.json().get('value', [])
print(f'Total assets globally: {len(assets)}')
print()

# Find asset that was supposedly linked to "Akutflodesmonitorering"
# The POST used assetId d2754761-... (DW). Check if there's a dataProductId field
patients_assets = [a for a in assets if a['name'] == 'patients']
print(f'Assets named "patients": {len(patients_assets)}')
for a in patients_assets:
    print(json.dumps(a, indent=2, ensure_ascii=False))
    print('---')
