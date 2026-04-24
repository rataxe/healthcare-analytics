"""Extract DATA_PRODUCTS list from purview_data_products.py and show tables for 0-asset products."""
import ast, sys, json, requests
from pathlib import Path
from azure.identity import AzureCliCredential

src = Path('scripts/purview_data_products.py').read_text(encoding='utf-8')
start = src.find('DATA_PRODUCTS = [')
# Find matching end: track brackets
i = src.find('[', start)
depth = 0
for j in range(i, len(src)):
    c = src[j]
    if c == '[':
        depth += 1
    elif c == ']':
        depth -= 1
        if depth == 0:
            end = j + 1
            break
list_src = src[i:end]
DATA_PRODUCTS = ast.literal_eval(list_src)
print(f'Parsed {len(DATA_PRODUCTS)} products from source')

# Get products with 0 assets
t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'
live = requests.get(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, timeout=60).json().get('value', [])
zero = {p['name'] for p in live if p.get('additionalProperties', {}).get('assetCount', 0) == 0}

print(f'\nProducts with 0 assets ({len(zero)}):\n')
for p in DATA_PRODUCTS:
    if p['name'] in zero:
        tables = p.get('tables', [])
        print(f'* {p["name"]}  [{p.get("domain_name")}]')
        print(f'    tables: {tables}')
        print()
