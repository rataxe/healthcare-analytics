"""Check actual assets on a specific product."""
import requests
from azure.identity import AzureCliCredential

t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

r = requests.get(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, timeout=60)
prods = r.json().get('value', [])
p = next(x for x in prods if x['name'] == 'Akutflödesmonitorering')
pid = p['id']
print(f'Product: {p["name"]}  id={pid}')
print(f'additionalProperties: {p.get("additionalProperties", {})}')

r2 = requests.get(f'{UNIFIED}/dataAssets?dataProductId={pid}&api-version={VER}', headers=h, timeout=60)
print(f'\nGET /dataAssets?dataProductId=... status: {r2.status_code}')
assets = r2.json().get('value', [])
print(f'Assets returned: {len(assets)}')
for a in assets:
    print(f'  - {a["name"]}  (id={a["id"]}, state={a["systemData"].get("provisioningState")})')
