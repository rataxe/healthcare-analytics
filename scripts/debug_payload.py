import requests, json, ast
from azure.identity import AzureCliCredential as C
from pathlib import Path

t = C().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}', 'Content-Type': 'application/json'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

# Get domains
domains_r = requests.get(f'{UNIFIED}/businessDomains?api-version={VER}', headers=h, timeout=30)
domains = {d['name']: d['id'] for d in domains_r.json().get('value', [])}

# Load DATA_PRODUCTS
module = ast.parse(Path('scripts/purview_data_products.py').read_text(encoding='utf-8'))
products = None
for node in module.body:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if getattr(target, 'id', None) == 'DATA_PRODUCTS':
                products = ast.literal_eval(node.value)
                break
    if products is not None:
        break

# Test each product that failed (Akutflödesmonitorering is products[1])
for idx, dp in enumerate(products[1:5]):
    print(f'\n--- Testing {idx+2}: {dp["name"]} ---')
    
    payload = {
        'name': dp['name'],
        'description': dp['description'],
        'status': dp['status'],
        'type': dp['type'],
        'domain': domains.get(dp['domain_name']),
        'businessUse': dp.get('business_use', ''),
        'contacts': {'owner': [{'id': '9350a243-7bcf-4053-8f7e-996364f4de24', 'description': 'Creator'}]},
        'termsOfUse': [],
        'documentation': [],
        'endorsed': True,
        'audience': dp.get('audience', ['DataEngineer']),
    }
    
    r = requests.post(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, json=payload, timeout=30)
    print(f'Status: {r.status_code}')
    if r.status_code != 201:
        print(f'Error: {r.text[:500]}')
