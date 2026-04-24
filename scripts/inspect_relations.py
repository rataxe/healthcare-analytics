"""Check existing relationships on Klinisk Patientanalys to learn schema."""
import requests, json
from azure.identity import AzureCliCredential

t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

pid = '334efee5-06c7-4ce3-81a7-1e8dd7570a56'  # Klinisk Patientanalys (has 9 assets)
r = requests.get(f'{UNIFIED}/dataProducts/{pid}/relationships?api-version={VER}', headers=h, timeout=60)
print(f'status: {r.status_code}')
print(f'Total: {len(r.json().get("value", []))}')
# print first few of different entityTypes
data = r.json().get('value', [])
seen = set()
for rel in data:
    et = rel.get('entityType')
    if et not in seen:
        print(f'\n--- {et} ---')
        print(json.dumps(rel, indent=2, ensure_ascii=False))
        seen.add(et)

# Count by entityType
from collections import Counter
c = Counter(rel.get('entityType') for rel in data)
print(f'\nCounts: {dict(c)}')
