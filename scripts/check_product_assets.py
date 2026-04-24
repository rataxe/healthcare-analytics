"""Check assetCount for every data product in Purview Unified Catalog."""
import requests
from azure.identity import AzureCliCredential

t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

r = requests.get(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, timeout=60)
products = r.json().get('value', [])

# domains
dr = requests.get(f'{UNIFIED}/businessDomains?api-version={VER}', headers=h, timeout=60)
dmap = {d['id']: d['name'] for d in dr.json().get('value', [])}

print(f'{len(products)} products total\n')
zero = []
for p in sorted(products, key=lambda x: (dmap.get(x.get('domain', ''), ''), x.get('name', ''))):
    ac = p.get('additionalProperties', {}).get('assetCount', 0)
    dom = dmap.get(p.get('domain', ''), 'unknown')
    flag = ' <-- needs assets' if ac == 0 else ''
    print(f'  [{ac:>3}] {dom:<35} | {p["name"]}{flag}')
    if ac == 0:
        zero.append((p['id'], p['name'], dom))

print(f'\n{len(zero)} products have 0 assets')
