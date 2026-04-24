"""Try PATCH on existing asset to add dataProductId."""
import requests, json
from azure.identity import AzureCliCredential

t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}', 'Content-Type': 'application/merge-patch+json'}
U = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
V = '2025-09-15-preview'

# ml_predictions asset
asset_id = 'e067c2d9-fee4-4aa8-84b2-29c4470e5469'
# MLOps Modellregister
pid = '6f6eafa4-ba29-486f-b5de-00b58a2c32af'

# Variants to try
attempts = [
    ('PATCH body dataProductId', 'PATCH', f'{U}/dataAssets/{asset_id}?api-version={V}', {'dataProductId': pid}),
    ('PATCH body dataProductIds list', 'PATCH', f'{U}/dataAssets/{asset_id}?api-version={V}', {'dataProductIds': [pid]}),
    ('PATCH body dataProducts list', 'PATCH', f'{U}/dataAssets/{asset_id}?api-version={V}', {'dataProducts': [{'id': pid}]}),
    # maybe product-side: POST /dataProducts/{id}/dataAssets
    ('POST product/dataAssets', 'POST', f'{U}/dataProducts/{pid}/dataAssets?api-version={V}', {'assetId': asset_id}),
    ('POST product/dataAssets with id', 'POST', f'{U}/dataProducts/{pid}/dataAssets?api-version={V}', {'id': asset_id}),
    ('POST relationships DATAASSET', 'POST', f'{U}/dataProducts/{pid}/relationships?api-version={V}&entityType=DATAASSET', {'entityId': asset_id, 'relationshipType': 'Related'}),
    ('POST relationships Asset', 'POST', f'{U}/dataProducts/{pid}/relationships?api-version={V}', {'entityType': 'Asset', 'entityId': asset_id, 'relationshipType': 'Related'}),
    ('POST relationships with DataAsset type', 'POST', f'{U}/dataProducts/{pid}/relationships?api-version={V}', {'entityType': 'DataAsset', 'entityId': asset_id, 'relationshipType': 'Related'}),
]

for name, method, url, body in attempts:
    h2 = dict(h)
    if method == 'POST':
        h2['Content-Type'] = 'application/json'
    r = requests.request(method, url, headers=h2, json=body, timeout=30)
    print(f'{name}: {r.status_code}')
    print(f'  {r.text[:300]}')
    print()
