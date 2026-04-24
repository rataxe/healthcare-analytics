"""Inspect existing data-asset relationships and discover available assets."""
import requests, json
from azure.identity import AzureCliCredential

t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
SEARCH = 'https://prviewacc.purview.azure.com/catalog/api/search/query?api-version=2022-08-01-preview'
VER = '2025-09-15-preview'

# 1. Show existing assets on Klinisk Patientanalys
pid = '334efee5-06c7-4ce3-81a7-1e8dd7570a56'
r = requests.get(f'{UNIFIED}/dataAssets?dataProductId={pid}&api-version={VER}', headers=h, timeout=60)
print('=== Existing assets on "Klinisk Patientanalys" (9 assets) ===')
assets = r.json().get('value', [])
for a in assets[:3]:
    print(json.dumps(a, indent=2, ensure_ascii=False)[:1200])
    print('---')
print(f'Total assets returned: {len(assets)}')
print()

# 2. List asset names only
print('Asset names on that product:')
for a in assets:
    print(f'  - {a.get("name")}  (type={a.get("source",{}).get("type")})')
print()

# 3. Search catalog for all data assets (Atlas search)
print('=== Searching Purview catalog for Fabric/table assets ===')
body = {"keywords": "*", "limit": 25, "filter": {"entityType": "fabric_table"}}
r = requests.post(SEARCH, headers={**h, 'Content-Type': 'application/json'}, json=body, timeout=60)
print(f'fabric_table search: {r.status_code}')
if r.ok:
    val = r.json().get('value', [])
    print(f'  {len(val)} hits. Sample names:')
    for v in val[:10]:
        print(f'    - {v.get("name")}  ({v.get("entityType")})')
