"""Verify category assignments."""
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
GG = "d939ea20-9c67-48af-98d9-b66965f7cde1"

# Get category name map
r = requests.get(f"{ATLAS}/glossary/{GG}", headers=h, timeout=15)
cat_map = {}
for c in r.json().get("categories", []):
    cg = c["categoryGuid"]
    dt = c.get("displayText", "?")
    cat_map[cg] = dt
    print(f"Category: {dt} = {cg}")

# Get all terms
print("\nFetching terms...")
terms = []
offset = 0
while True:
    r2 = requests.get(f"{ATLAS}/glossary/{GG}/terms?limit=100&offset={offset}",
                      headers=h, timeout=15)
    batch = r2.json()
    if not batch:
        break
    terms.extend(batch)
    offset += len(batch)
    if len(batch) < 100:
        break

# Count by category
by_cat = {}
nocats = []
for t in terms:
    cats = t.get("categories", [])
    if cats:
        cg = cats[0].get("categoryGuid", "?")
        cn = cat_map.get(cg, f"UNKNOWN({cg})")
        by_cat.setdefault(cn, []).append(t["name"])
    else:
        nocats.append(t["name"])

print(f"\nTERMS PER CATEGORY (total: {len(terms)}):")
for cn, tlist in sorted(by_cat.items()):
    print(f"  {cn}: {len(tlist)} terms")
    for n in tlist[:3]:
        print(f"    - {n}")
    if len(tlist) > 3:
        print(f"    ... +{len(tlist)-3} more")

if nocats:
    print(f"\n  UNCATEGORIZED: {len(nocats)}")
    for n in nocats[:10]:
        print(f"    - {n}")
else:
    print("\n  ALL 145 TERMS CATEGORIZED!")
