"""
Try Purview Account API to move entities to collections.
Tests multiple API patterns to find the one that works.
"""
import requests, sys, os, time, json
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

# Get one Process entity GUID from root
r = requests.post(SEARCH, headers=h, json={
    "filter": {"and": [{"entityType": "Process"}, {"collectionId": "prviewacc"}]},
    "limit": 1
}, timeout=30)
vals = r.json().get("value", [])
if not vals:
    print("No Process entity found in root")
    sys.exit(1)

test_guid = vals[0]["id"]
test_name = vals[0].get("name", "?")
print(f"Test entity: {test_name} ({test_guid})")

# Try different API endpoints for collection assignment
TARGET = "fabric-analytics"
API_VERSION_OPTS = ["2019-11-01-preview", "2022-07-01-preview", "2023-09-01"]

endpoints = [
    # Pattern 1: Account API
    f"{ACCT}/account/collections/{TARGET}/entity/{test_guid}",
    # Pattern 2: Account API with move
    f"{ACCT}/account/collections/{TARGET}/entity",
    # Pattern 3: Catalog collections
    f"{ACCT}/catalog/api/collections/{TARGET}/entity/{test_guid}",
    # Pattern 4: Datamap entity with collectionId in body
    f"{ACCT}/datamap/api/entity?api-version=2023-09-01",
    # Pattern 5: Datamap collections move  
    f"{ACCT}/datamap/api/collections/{TARGET}/entity/{test_guid}",
]

bodies = [
    None,  # Pattern 1: no body needed
    {"entityGuid": test_guid},  # Pattern 2: guid in body
    None,  # Pattern 3: no body needed
    None,  # Pattern 4: handled separately
    None,  # Pattern 5: no body needed
]

for i, url in enumerate(endpoints):
    print(f"\nPattern {i+1}: POST {url}")
    
    if i == 3:
        # Pattern 4: Full entity update via datamap
        r2 = requests.get(f"{ATLAS}/entity/guid/{test_guid}", headers=h, timeout=30)
        ent = r2.json().get("entity", {})
        ent["collectionId"] = TARGET
        ent.pop("relationshipAttributes", None)
        body = {"entity": ent}
        for ver in API_VERSION_OPTS:
            time.sleep(1)
            u = f"{ACCT}/datamap/api/entity?api-version={ver}"
            r3 = requests.post(u, headers=h, json=body, timeout=30)
            print(f"  api-version={ver}: {r3.status_code}")
            if r3.status_code < 300:
                print(f"  Response: {r3.text[:200]}")
                break
            elif r3.status_code != 404:
                print(f"  Error: {r3.text[:200]}")
        continue
    
    body = bodies[i]
    for ver in API_VERSION_OPTS:
        time.sleep(1)
        sep = "&" if "?" in url else "?"
        full_url = f"{url}{sep}api-version={ver}"
        
        if body:
            r3 = requests.post(full_url, headers=h, json=body, timeout=30)
        else:
            r3 = requests.post(full_url, headers=h, timeout=30)
        
        print(f"  api-version={ver}: {r3.status_code}")
        if r3.status_code < 300:
            print(f"  SUCCESS: {r3.text[:200]}")
            break
        elif r3.status_code == 404:
            pass  # try next version
        else:
            print(f"  Error: {r3.text[:150]}")
            break

# Also try PUT on same patterns
print("\n--- Also trying PUT ---")
for url in [
    f"{ACCT}/account/collections/{TARGET}/entity/{test_guid}",
    f"{ACCT}/catalog/api/collections/{TARGET}/entity/{test_guid}",
    f"{ACCT}/datamap/api/collections/{TARGET}/entity/{test_guid}",
]:
    print(f"\nPUT {url}")
    for ver in ["2019-11-01-preview", "2023-09-01"]:
        time.sleep(1)
        sep = "&" if "?" in url else "?"
        r3 = requests.put(f"{url}{sep}api-version={ver}", headers=h, timeout=30)
        print(f"  api-version={ver}: {r3.status_code}")
        if r3.status_code < 300:
            print(f"  SUCCESS: {r3.text[:200]}")
            break
        elif r3.status_code != 404:
            print(f"  Error: {r3.text[:150]}")
            break

# Try the move-specific endpoints
print("\n--- Trying move endpoints ---")
for base in [f"{ACCT}/account", f"{ACCT}/datamap/api", f"{ACCT}/catalog/api"]:
    url = f"{base}/entity/moveTo?api-version=2023-09-01"
    body = {"entityGuids": [test_guid], "collectionId": TARGET}
    time.sleep(1)
    r3 = requests.post(url, headers=h, json=body, timeout=30)
    print(f"POST {url}: {r3.status_code}")
    if r3.status_code < 300:
        print(f"  SUCCESS!")
    elif r3.status_code != 404:
        print(f"  {r3.text[:150]}")

# Final check: did the entity's collection change?
time.sleep(2)
print("\n--- Final check ---")
r = requests.get(f"{ATLAS}/entity/guid/{test_guid}", headers=h, timeout=30)
ent = r.json().get("entity", {})
cid = ent.get("collectionId", "NOT SET")
print(f"Entity collectionId: {cid}")

# Also check entity's custom attributes
attrs = ent.get("attributes", {})
print(f"Entity attributes keys: {list(attrs.keys())[:10]}")

print("\nDone!")
