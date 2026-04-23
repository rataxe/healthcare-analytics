"""Check existing structure to recommend governance domains."""
import requests, sys, os
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
from azure.identity import AzureCliCredential
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
ACCT = "https://prviewacc.purview.azure.com"
ATLAS = ACCT + "/catalog/api/atlas/v2"
SEARCH = ACCT + "/catalog/api/search/query?api-version=2022-08-01-preview"

# 1. Glossary categories
r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=15)
glist = r.json() if isinstance(r.json(), list) else [r.json()]
g = glist[0]
gguid = g["guid"]
cats = g.get("categories", [])
print("GLOSSARY CATEGORIES:")
for c in cats:
    dt = c.get("displayText", "?")
    cg = c["categoryGuid"]
    print(f"  {dt} ({cg})")

# 2. Data products
r2 = requests.post(SEARCH, headers=h, json={"filter": {"entityType": "healthcare_data_product"}, "limit": 10}, timeout=15)
cnt = r2.json().get("@search.count", 0)
print(f"\nDATA PRODUCTS ({cnt}):")
for v in r2.json().get("value", []):
    nm = v.get("name", "?")
    coll = v.get("collectionId", "?")
    print(f"  {nm} | coll={coll}")

# 3. Collections
print("\nCOLLECTION STRUCTURE:")
r3 = requests.get(f"{ACCT}/account/collections?api-version=2019-11-01-preview", headers=h, timeout=15)
for c in r3.json().get("value", []):
    nm = c.get("friendlyName", c["name"])
    parent = c.get("parentCollection", {}).get("referenceName", "ROOT")
    print(f"  {nm} ({c['name']}) -> parent: {parent}")

# 4. Terms sample per category
print("\nTERMS PER CATEGORY (sample):")
r4 = requests.get(f"{ATLAS}/glossary/{gguid}/terms?limit=200&offset=0", headers=h, timeout=15)
if r4.status_code == 200:
    terms = r4.json()
    by_cat = {}
    for t in terms:
        cats_list = t.get("categories", [])
        cat_name = cats_list[0].get("displayText", "Uncategorized") if cats_list else "Uncategorized"
        if cat_name not in by_cat:
            by_cat[cat_name] = []
        by_cat[cat_name].append(t.get("name", "?"))
    for cat, tlist in sorted(by_cat.items()):
        print(f"  {cat} ({len(tlist)} terms):")
        for t in tlist[:3]:
            print(f"    - {t}")
        if len(tlist) > 3:
            print(f"    ... +{len(tlist)-3} more")

print("\nDone!")
