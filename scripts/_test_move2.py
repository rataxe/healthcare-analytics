"""Test moveTo API with collectionId as query param."""
import requests, sys, os, time
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
SEARCH = ACCT + "/catalog/api/search/query?api-version=2022-08-01-preview"
ATLAS = ACCT + "/catalog/api/atlas/v2"

# Get test guid
r = requests.post(SEARCH, headers=h, json={
    "filter": {"and": [{"entityType": "Process"}, {"collectionId": "prviewacc"}]},
    "limit": 1
}, timeout=30)
guid = r.json()["value"][0]["id"]
name = r.json()["value"][0].get("name", "?")
print(f"Test: {name} ({guid})")

# A) moveTo with collectionId as query param
for ver in ["2023-09-01", "2022-07-01-preview", "2023-10-01-preview"]:
    time.sleep(1)
    url = f"{ACCT}/datamap/api/entity/moveTo?collectionId=fabric-analytics&api-version={ver}"
    body = {"entityGuids": [guid]}
    r2 = requests.post(url, headers=h, json=body, timeout=30)
    print(f"moveTo v={ver}: {r2.status_code}")
    if r2.status_code < 300:
        print(f"  SUCCESS: {r2.text[:200]}")
        break
    else:
        print(f"  {r2.text[:200]}")

# B) moveEntitiesToCollection
for ver in ["2023-09-01", "2023-10-01-preview"]:
    time.sleep(1)
    url = f"{ACCT}/datamap/api/entity/moveEntitiesToCollection?collectionId=fabric-analytics&api-version={ver}"
    body = {"entityGuids": [guid]}
    r2 = requests.post(url, headers=h, json=body, timeout=30)
    print(f"moveEntitiesToCollection v={ver}: {r2.status_code}")
    if r2.status_code < 300:
        print(f"  SUCCESS!")
        break
    else:
        print(f"  {r2.text[:200]}")

# C) createOrUpdate via datamap with collection as nested object
time.sleep(1)
r3 = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=30)
ent = r3.json().get("entity", {})
ent.pop("relationshipAttributes", None)

# Try nested CollectionReference format
ent2 = dict(ent)
ent2["collectionId"] = "fabric-analytics"
body = {"entity": ent2, "referredEntities": {}}
time.sleep(1)
url = f"{ACCT}/datamap/api/atlas/v2/entity"
r4 = requests.post(url, headers=h, json=body, timeout=30)
print(f"\nDatamap createOrUpdate (string): {r4.status_code}")
if r4.status_code < 300:
    print(f"  SUCCESS: {r4.text[:200]}")
else:
    print(f"  {r4.text[:200]}")

# Try with collection as part of entity body
ent3 = dict(ent)
ent3["collection"] = {"referenceName": "fabric-analytics", "type": "CollectionReference"}
body = {"entity": ent3, "referredEntities": {}}
time.sleep(1)
r5 = requests.post(url, headers=h, json=body, timeout=30)
print(f"Datamap createOrUpdate (CollectionReference): {r5.status_code}")
if r5.status_code < 300:
    print(f"  SUCCESS: {r5.text[:200]}")
else:
    print(f"  {r5.text[:200]}")

# Verify
time.sleep(2)
r6 = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=30)
ent = r6.json().get("entity", {})
cid = ent.get("collectionId", "NOT SET")
col = ent.get("collection", "NOT SET")
print(f"\nFinal - collectionId: {cid}, collection: {col}")
print("Done!")
