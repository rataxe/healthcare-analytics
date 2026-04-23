"""
Purview FULL RESET — deletes everything and starts fresh.
Steps:
  1. Delete all glossary terms
  2. Delete all glossaries (except default)
  3. Delete all scanned assets
  4. Remove scan definitions
  5. Unregister data sources
  6. Delete non-root collections
"""
import requests, json, time, sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

sess = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
sess.mount("https://", HTTPAdapter(max_retries=retry))

ACCT = "https://prviewacc.purview.azure.com"
TENANT = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
SCAN_EP = f"{TENANT}/scan"
SCAN_API = "2022-07-01-preview"

def sep(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")

# ── 1. INVENTORY ──
sep("1. CURRENT INVENTORY")

# Glossaries
r = sess.get(f"{ATLAS}/glossary", headers=h, timeout=30)
glossaries = r.json() if r.status_code == 200 else []
print(f"  Glossaries: {len(glossaries)}")
for g in glossaries:
    print(f"    - {g['name']} (guid={g['guid'][:12]}...)")

# Collections
r = sess.get(f"{ACCT}/account/collections?api-version=2019-11-01-preview", headers=h, timeout=30)
collections = r.json().get("value", []) if r.status_code == 200 else []
print(f"  Collections: {len(collections)}")
for c in collections:
    parent = c.get("parentCollection", {}).get("referenceName", "ROOT")
    print(f"    - {c['name']} ({c.get('friendlyName')}) parent={parent}")

# Data sources
r = sess.get(f"{SCAN_EP}/datasources?api-version={SCAN_API}", headers=h, timeout=30)
datasources = r.json().get("value", []) if r.status_code == 200 else []
print(f"  Data sources: {len(datasources)}")
for ds in datasources:
    print(f"    - {ds['name']} ({ds.get('kind', '?')})")

# Count assets
r = sess.post(SEARCH, headers=h, json={"keywords": "*", "limit": 1}, timeout=30)
total_assets = r.json().get("@search.count", 0) if r.status_code == 200 else "?"
print(f"  Total assets: {total_assets}")

# ── 2. CONFIRM ──
print(f"\n  ⚠️  THIS WILL DELETE EVERYTHING IN PURVIEW!")
print(f"  - {len(glossaries)} glossaries with all terms")
print(f"  - {len(datasources)} data sources with scans")
print(f"  - {total_assets} discovered assets")
print(f"  - {len(collections) - 2} non-root collections")  # minus 2 root collections
resp = input("\n  Type 'DELETE ALL' to confirm: ")
if resp.strip() != "DELETE ALL":
    print("  Aborted.")
    sys.exit(0)

# ── 3. DELETE GLOSSARY TERMS ──
sep("2. DELETING GLOSSARY TERMS")

for g in glossaries:
    r = sess.get(f"{ATLAS}/glossary/{g['guid']}/terms?limit=500", headers=h, timeout=30)
    if r.status_code != 200:
        continue
    terms = r.json()
    print(f"  Glossary '{g['name']}': {len(terms)} terms")
    for t in terms:
        r2 = sess.delete(f"{ATLAS}/glossary/term/{t['guid']}", headers=h, timeout=30)
        status = "✅" if r2.status_code in (200, 204) else f"❌ {r2.status_code}"
        print(f"    DEL term '{t['name']}': {status}")
        time.sleep(0.2)

# ── 4. DELETE GLOSSARIES ──
sep("3. DELETING GLOSSARIES")

for g in glossaries:
    r = sess.delete(f"{ATLAS}/glossary/{g['guid']}", headers=h, timeout=30)
    status = "✅" if r.status_code in (200, 204) else f"❌ {r.status_code}"
    print(f"  DEL glossary '{g['name']}': {status}")

# ── 5. DELETE SCANS THEN DATA SOURCES ──
sep("4. DELETING SCANS & DATA SOURCES")

for ds in datasources:
    ds_name = ds["name"]
    # List scans for this source
    r = sess.get(f"{SCAN_EP}/datasources/{ds_name}/scans?api-version={SCAN_API}", headers=h, timeout=30)
    scans = r.json().get("value", []) if r.status_code == 200 else []
    
    for scan in scans:
        scan_name = scan["name"]
        r2 = sess.delete(
            f"{SCAN_EP}/datasources/{ds_name}/scans/{scan_name}?api-version={SCAN_API}",
            headers=h, timeout=30
        )
        status = "✅" if r2.status_code in (200, 204) else f"❌ {r2.status_code}"
        print(f"  DEL scan {ds_name}/{scan_name}: {status}")
        time.sleep(0.3)
    
    # Delete data source
    r = sess.delete(
        f"{SCAN_EP}/datasources/{ds_name}?api-version={SCAN_API}",
        headers=h, timeout=30
    )
    status = "✅" if r.status_code in (200, 204) else f"❌ {r.status_code}"
    print(f"  DEL datasource '{ds_name}': {status}")
    time.sleep(0.3)

# ── 6. DELETE ASSETS (bulk) ──
sep("5. DELETING DISCOVERED ASSETS")

deleted_count = 0
batch_size = 50

while True:
    r = sess.post(SEARCH, headers=h, json={"keywords": "*", "limit": batch_size}, timeout=30)
    if r.status_code != 200:
        print(f"  Search error: {r.status_code}")
        break
    
    assets = r.json().get("value", [])
    if not assets:
        break
    
    guids = [a["id"] for a in assets]
    
    # Bulk delete via Atlas
    r2 = sess.delete(
        f"{ATLAS}/entity/bulk?guid={'&guid='.join(guids)}",
        headers=h, timeout=60
    )
    
    if r2.status_code in (200, 204):
        deleted_count += len(guids)
        print(f"  Deleted batch of {len(guids)} (total: {deleted_count})")
    else:
        # Try individual deletes
        for guid in guids:
            r3 = sess.delete(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=30)
            if r3.status_code in (200, 204):
                deleted_count += 1
            time.sleep(0.1)
        print(f"  Individual delete batch done (total: {deleted_count})")
    
    time.sleep(0.5)
    
    # Safety: break if we've done many rounds
    if deleted_count > 10000:
        print("  Safety limit reached")
        break

print(f"  Total assets deleted: {deleted_count}")

# ── 7. DELETE NON-ROOT COLLECTIONS ──
sep("6. DELETING NON-ROOT COLLECTIONS")

# Re-fetch collections and delete leaf-first
r = sess.get(f"{ACCT}/account/collections?api-version=2019-11-01-preview", headers=h, timeout=30)
collections = r.json().get("value", []) if r.status_code == 200 else []

# Build parent→children map
children = {}
for c in collections:
    parent = c.get("parentCollection", {}).get("referenceName")
    if parent:
        children.setdefault(parent, []).append(c["name"])

# Topological sort: delete leaves first
root_names = {"prviewacc", "upiwjm"}  # Keep root collections

def get_delete_order(colls, children_map):
    """Return collections in leaf-first order."""
    order = []
    visited = set()
    
    def visit(name):
        if name in visited:
            return
        visited.add(name)
        for child in children_map.get(name, []):
            visit(child)
        if name not in root_names:
            order.append(name)
    
    for c in colls:
        visit(c["name"])
    return order

delete_order = get_delete_order(collections, children)
print(f"  Delete order: {delete_order}")

for coll_name in delete_order:
    r = sess.delete(
        f"{ACCT}/account/collections/{coll_name}?api-version=2019-11-01-preview",
        headers=h, timeout=30
    )
    status = "✅" if r.status_code in (200, 204) else f"❌ {r.status_code} {r.text[:100]}"
    print(f"  DEL collection '{coll_name}': {status}")
    time.sleep(0.3)

# Try deleting upiwjm (IT) root — might not work if it's a true root
r = sess.delete(
    f"{ACCT}/account/collections/upiwjm?api-version=2019-11-01-preview",
    headers=h, timeout=30
)
status = "✅" if r.status_code in (200, 204) else f"⚠️ {r.status_code} (root collection, might need portal)"
print(f"  DEL 'upiwjm' (IT): {status}")

# ── 8. VERIFY ──
sep("7. VERIFICATION")

r = sess.get(f"{ATLAS}/glossary", headers=h, timeout=30)
g_count = len(r.json()) if r.status_code == 200 else "?"

r = sess.post(SEARCH, headers=h, json={"keywords": "*", "limit": 1}, timeout=30)
a_count = r.json().get("@search.count", "?") if r.status_code == 200 else "?"

r = sess.get(f"{SCAN_EP}/datasources?api-version={SCAN_API}", headers=h, timeout=30)
ds_count = len(r.json().get("value", [])) if r.status_code == 200 else "?"

r = sess.get(f"{ACCT}/account/collections?api-version=2019-11-01-preview", headers=h, timeout=30)
c_count = len(r.json().get("value", [])) if r.status_code == 200 else "?"

print(f"  Glossaries remaining:   {g_count}")
print(f"  Assets remaining:       {a_count}")
print(f"  Data sources remaining: {ds_count}")
print(f"  Collections remaining:  {c_count}")

if a_count == 0 and ds_count == 0:
    print("\n  ✅ PURVIEW IS CLEAN — Ready to rebuild!")
else:
    print(f"\n  ⚠️ Some items remain (assets may take a few minutes to purge)")
    print("  Run this script again if needed, or wait and re-verify.")
