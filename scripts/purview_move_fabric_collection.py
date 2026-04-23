"""
Move the Fabric scan source to the prviewacc collection so all assets 
share the same governance domain as the glossary.
Then re-scan and assign glossary terms.
"""
import requests, json, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

ACCT_EP = "https://prviewacc.purview.azure.com"
SCAN_EP = f"{ACCT_EP}/scan"

# ── 1. Get current Fabric scan source details ──
print("=" * 70)
print("1. Current Fabric scan source")
print("=" * 70)

r = sess.get(f"{SCAN_EP}/datasources/Fabric?api-version=2022-02-01-preview", headers=h, timeout=30)
if r.status_code == 200:
    ds = r.json()
    print(f"  Name: {ds.get('name')}")
    print(f"  Kind: {ds.get('kind')}")
    props = ds.get("properties", {})
    print(f"  Collection: {props.get('collection', {})}")
    print(f"  Full body: {json.dumps(ds, indent=2)[:800]}")
else:
    print(f"  Error: {r.status_code} {r.text[:300]}")
    
# ── 2. Check collections in prviewacc ──
print(f"\n{'=' * 70}")
print("2. Collections in prviewacc")
print("=" * 70)

r = sess.get(f"{ACCT_EP}/account/collections?api-version=2019-11-01-preview", headers=h, timeout=30)
if r.status_code == 200:
    for c in r.json().get("value", []):
        name = c.get("name")
        friendly = c.get("friendlyName")
        parent = c.get("parentCollection", {}).get("referenceName", "root")
        print(f"  {name} ({friendly}) parent={parent}")
else:
    # Try another version
    r = sess.get(f"{ACCT_EP}/account/collections?api-version=2023-10-01-preview", headers=h, timeout=30)
    if r.status_code == 200:
        for c in r.json().get("value", []):
            name = c.get("name")
            friendly = c.get("friendlyName")
            parent = c.get("parentCollection", {}).get("referenceName", "root")
            print(f"  {name} ({friendly}) parent={parent}")

# ── 3. Create a Fabric-Assets collection under prviewacc root ──
print(f"\n{'=' * 70}")
print("3. Creating 'fabric-assets' collection under prviewacc")
print("=" * 70)

coll_body = {
    "friendlyName": "Fabric Assets",
    "parentCollection": {"referenceName": "prviewacc"},
    "description": "Fabric lakehouses, notebooks och pipelines"
}

r = sess.put(
    f"{ACCT_EP}/account/collections/fabric-assets?api-version=2019-11-01-preview",
    headers=h, json=coll_body, timeout=30,
)
print(f"  Create collection: {r.status_code}")
if r.status_code in (200, 201):
    print(f"  ✅ Created 'fabric-assets' collection")
    print(f"  {json.dumps(r.json(), indent=2)[:400]}")
else:
    print(f"  {r.text[:300]}")

# ── 4. Update Fabric scan source to use fabric-assets collection ──
print(f"\n{'=' * 70}")
print("4. Updating Fabric scan source collection")
print("=" * 70)

# First get current full body
r = sess.get(f"{SCAN_EP}/datasources/Fabric?api-version=2022-02-01-preview", headers=h, timeout=30)
if r.status_code == 200:
    ds = r.json()
    ds["properties"]["collection"] = {
        "type": "CollectionReference",
        "referenceName": "fabric-assets"
    }
    # Remove read-only fields
    for key in ["id", "systemData"]:
        ds.pop(key, None)
    ds.get("properties", {}).pop("createdAt", None)
    ds.get("properties", {}).pop("lastModifiedAt", None)
    ds.get("properties", {}).pop("dataSourceCollectionMovingState", None)
    
    r2 = sess.put(
        f"{SCAN_EP}/datasources/Fabric?api-version=2022-02-01-preview",
        headers=h, json=ds, timeout=30,
    )
    print(f"  Update datasource: {r2.status_code}")
    if r2.status_code in (200, 201):
        print(f"  ✅ Fabric scan source moved to 'fabric-assets' collection")
        print(f"  New collection: {r2.json().get('properties', {}).get('collection')}")
    else:
        print(f"  Error: {r2.text[:500]}")
