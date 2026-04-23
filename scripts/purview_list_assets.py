"""List discovered assets in Purview and add metadata:
- Classifications on columns (personnummer, ICD10, etc.)
- Glossary terms linked to tables
- Descriptions on key assets
"""
import requests, json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
CATALOG_EP = "https://prviewacc.purview.azure.com"
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ── 1. Search for SQL assets ──
print("=" * 60)
print("1. SQL Assets (sql-hca-demo)")
print("=" * 60)

search_url = f"{CATALOG_EP}/catalog/api/search/query?api-version=2022-08-01-preview"
body = {
    "keywords": "*",
    "filter": {
        "and": [
            {"objectType": "Tables"},
            {"attributeName": "qualifiedName", "operator": "contains", "attributeValue": "sql-hca-demo"}
        ]
    },
    "limit": 50
}
r = requests.post(search_url, headers=h, json=body)
if r.status_code == 200:
    results = r.json().get("value", [])
    print(f"  Found {len(results)} table assets")
    for asset in results:
        name = asset.get("name", "?")
        qn = asset.get("qualifiedName", "?")
        guid = asset.get("id", "?")
        print(f"  - {name} (guid={guid[:12]}...)")
else:
    print(f"  Search failed: {r.status_code} {r.text[:200]}")

# ── 2. Search for all SQL columns ──
print(f"\n{'=' * 60}")
print("2. SQL Columns")
print("=" * 60)

body2 = {
    "keywords": "*",
    "filter": {
        "and": [
            {"objectType": "Columns"},
            {"attributeName": "qualifiedName", "operator": "contains", "attributeValue": "sql-hca-demo"}
        ]
    },
    "limit": 100
}
r2 = requests.post(search_url, headers=h, json=body2)
if r2.status_code == 200:
    cols = r2.json().get("value", [])
    print(f"  Found {len(cols)} column assets")
    # Group by table
    tables = {}
    for col in cols:
        qn = col.get("qualifiedName", "")
        parts = qn.rsplit("#", 1)
        tbl = parts[0].split("/")[-1] if "/" in parts[0] else parts[0]
        col_name = parts[1] if len(parts) > 1 else col.get("name", "?")
        if tbl not in tables:
            tables[tbl] = []
        tables[tbl].append({"name": col_name, "guid": col.get("id", ""), "qn": qn})
    for tbl, tcols in sorted(tables.items()):
        print(f"  {tbl}:")
        for c in sorted(tcols, key=lambda x: x["name"]):
            print(f"    - {c['name']}")
else:
    print(f"  Search failed: {r2.status_code} {r2.text[:200]}")

# ── 3. Search Fabric assets ──
print(f"\n{'=' * 60}")
print("3. Fabric Assets (sample)")
print("=" * 60)

for kw in ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse", "lh_brainchild"]:
    body3 = {"keywords": kw, "limit": 10}
    r3 = requests.post(search_url, headers=h, json=body3)
    if r3.status_code == 200:
        vals = r3.json().get("value", [])
        print(f"  '{kw}': {len(vals)} assets")
        for v in vals[:3]:
            print(f"    - {v.get('name', '?')} [{v.get('entityType', '?')}]")
