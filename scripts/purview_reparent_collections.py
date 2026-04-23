"""
Attempt to reparent the Analysplattform collection (4b0vy9) from upiwjm to prviewacc,
so all Fabric assets end up in the same governance domain as the glossary.
"""
import requests, json, time
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
ACCT = "https://prviewacc.purview.azure.com"

# ── 1. Show current hierarchy ──
print("=" * 70)
print("1. Current collection hierarchy")
print("=" * 70)

r = requests.get(f"{ACCT}/account/collections?api-version=2019-11-01-preview", headers=h, timeout=30)
colls = {}
if r.status_code == 200:
    for c in r.json().get("value", []):
        name = c.get("name")
        friendly = c.get("friendlyName")
        parent = c.get("parentCollection", {}).get("referenceName", "ROOT")
        colls[name] = c
        print(f"  {name} ({friendly}) parent={parent}")

# ── 2. Try reparenting Analysplattform (4b0vy9) to prviewacc ──
print(f"\n{'=' * 70}")
print("2. Reparenting '4b0vy9' (Analysplattform) from upiwjm -> prviewacc")
print("=" * 70)

body = {
    "friendlyName": "Analysplattform",
    "parentCollection": {"referenceName": "prviewacc"},
}
r = requests.put(
    f"{ACCT}/account/collections/4b0vy9?api-version=2019-11-01-preview",
    headers=h, json=body, timeout=30,
)
print(f"  Status: {r.status_code}")
if r.status_code in (200, 201):
    new = r.json()
    print(f"  ✅ Moved! New parent: {new.get('parentCollection', {}).get('referenceName')}")
else:
    print(f"  Error: {r.text[:400]}")
    
    # Try moving upiwjm itself under prviewacc
    print(f"\n  Trying: reparent 'upiwjm' (IT) under prviewacc...")
    body2 = {
        "friendlyName": "IT",
        "parentCollection": {"referenceName": "prviewacc"},
    }
    r2 = requests.put(
        f"{ACCT}/account/collections/upiwjm?api-version=2019-11-01-preview",
        headers=h, json=body2, timeout=30,
    )
    print(f"  Status: {r2.status_code}")
    if r2.status_code in (200, 201):
        print(f"  ✅ Moved IT under prviewacc!")
    else:
        print(f"  Error: {r2.text[:400]}")

# ── 3. Verify new hierarchy ──
print(f"\n{'=' * 70}")
print("3. Updated collection hierarchy")
print("=" * 70)

time.sleep(2)
r = requests.get(f"{ACCT}/account/collections?api-version=2019-11-01-preview", headers=h, timeout=30)
if r.status_code == 200:
    for c in r.json().get("value", []):
        name = c.get("name")
        friendly = c.get("friendlyName")
        parent = c.get("parentCollection", {}).get("referenceName", "ROOT")
        domain = c.get("domain", "?")
        print(f"  {name} ({friendly}) parent={parent} domain={domain}")

# ── 4. Quick test: assign a glossary term to a Fabric asset ──
print(f"\n{'=' * 70}")
print("4. Testing glossary assignment to Fabric asset")
print("=" * 70)

ATLAS = f"{ACCT}/catalog/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"

# Get Bronze Layer term guid
r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=30)
glossary_guid = None
for g in r.json():
    if g.get("name") == "Kund":
        glossary_guid = g["guid"]
        break

r = requests.get(f"{ATLAS}/glossary/{glossary_guid}/terms?limit=100", headers=h, timeout=30)
bronze_term_guid = None
for t in r.json():
    if t.get("name") == "Bronze Layer":
        bronze_term_guid = t["guid"]
        break

# Get bronze_lakehouse guid
body = {"keywords": "bronze_lakehouse", "limit": 3}
r = requests.post(SEARCH, headers=h, json=body, timeout=30)
bronze_lh_guid = None
for a in r.json().get("value", []):
    if a.get("entityType") == "fabric_lake_warehouse" and "bronze" in a.get("name", "").lower():
        bronze_lh_guid = a["id"]
        break

if bronze_term_guid and bronze_lh_guid:
    print(f"  Bronze Layer term: {bronze_term_guid}")
    print(f"  bronze_lakehouse:  {bronze_lh_guid}")
    
    r = requests.post(
        f"{ATLAS}/glossary/terms/{bronze_term_guid}/assignedEntities",
        headers=h, json=[{"guid": bronze_lh_guid}], timeout=30,
    )
    print(f"  Assignment result: {r.status_code}")
    if r.status_code in (200, 204):
        print(f"  ✅ SUCCESS! Glossary term linked to Fabric lakehouse!")
    else:
        print(f"  ❌ {r.text[:300]}")
else:
    print(f"  Missing: term={bronze_term_guid}, lakehouse={bronze_lh_guid}")
