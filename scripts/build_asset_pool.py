"""Collect asset pool from all products that already have assets."""
import requests, json
from azure.identity import AzureCliCredential

t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

products = requests.get(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, timeout=60).json().get('value', [])

pool = {}  # assetId -> asset record
for p in products:
    ac = p.get('additionalProperties', {}).get('assetCount', 0)
    if ac == 0:
        continue
    r = requests.get(f'{UNIFIED}/dataAssets?dataProductId={p["id"]}&api-version={VER}', headers=h, timeout=60)
    for a in r.json().get('value', []):
        src = a.get('source', {})
        aid = src.get('assetId')
        if not aid:
            continue
        pool[aid] = {
            'name': a['name'],
            'description': a.get('description', ''),
            'source': src,
            'assetType': src.get('assetType', ''),
        }

print(f'Unique assets discovered: {len(pool)}')
print()
# Group by type for visibility
by_type = {}
for aid, a in pool.items():
    by_type.setdefault(a['assetType'], []).append(a['name'])
for t_name, names in sorted(by_type.items()):
    print(f'\n{t_name}  ({len(names)}):')
    for n in sorted(set(names)):
        print(f'  - {n}')

# Save pool
with open('scripts/asset_pool.json', 'w', encoding='utf-8') as f:
    json.dump(pool, f, indent=2, ensure_ascii=False)
print('\nSaved to scripts/asset_pool.json')
