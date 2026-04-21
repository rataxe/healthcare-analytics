"""Test different OneLake URL formats to find the correct download path."""
import requests
from azure.identity import AzureCliCredential

WS = 'afda4639-34ce-4ee9-a82f-ab7b5cfd7334'
LH = '270a6614-2a07-463d-94de-0c55b26ec6de'
token = AzureCliCredential(process_timeout=30).get_token('https://storage.azure.com/.default').token
h = {'Authorization': f'Bearer {token}'}
pfile = 'part-00000-2827cb95-139d-4e4f-b70e-96fa727e8efa-c000.snappy.parquet'

# Try different path formats
attempts = [
    f'https://onelake.dfs.fabric.microsoft.com/{WS}/{LH}/Tables/diagnostic_log/{pfile}',
    f'https://onelake.dfs.fabric.microsoft.com/{WS}/{LH}/Tables/Tables/diagnostic_log/{pfile}',
]

for url in attempts:
    short = url.replace('https://onelake.dfs.fabric.microsoft.com/', '')
    r = requests.get(url, headers=h)
    print(f'{r.status_code} | ...{short[-80:]}')
    if r.status_code == 200:
        print(f'  SUCCESS! {len(r.content)} bytes')

# List the actual Tables directory with correct lakehouse prefix
print(f'\n--- Listing {LH}/Tables ---')
list_url = f'https://onelake.dfs.fabric.microsoft.com/{WS}'
params = {'resource': 'filesystem', 'recursive': 'true', 'directory': f'{LH}/Tables'}
r = requests.get(list_url, headers=h, params=params)
print(f'Status: {r.status_code}')
if r.status_code == 200:
    paths = r.json().get('paths', [])
    for p in paths[:30]:
        name = p.get('name', '')
        is_dir = p.get('isDirectory', 'false') == 'true'
        size = p.get('contentLength', '0')
        tag = '[D]' if is_dir else f'[{size}]'
        print(f'  {tag:>12s} {name}')
