"""Quick check: verify actual collectionId vs search index."""
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
ATLAS = ACCT + "/catalog/api/atlas/v2"
SEARCH = ACCT + "/catalog/api/search/query?api-version=2022-08-01-preview"

# 1. Check a Process entity
r = requests.post(SEARCH, headers=h, json={"filter": {"and": [{"entityType": "Process"}, {"collectionId": "prviewacc"}]}, "limit": 1}, timeout=30)
vals = r.json().get("value", [])
if vals:
    pguid = vals[0]["id"]
    pname = vals[0].get("name", "?")
    print(f"Process in root (search): {pname}")
    time.sleep(1)
    r2 = requests.get(f"{ATLAS}/entity/guid/{pguid}", headers=h, timeout=30)
    ent = r2.json().get("entity", {})
    cid = ent.get("collectionId", "N/A")
    print(f"  Actual collectionId from entity API: {cid}")

# 2. Check unknown entities
time.sleep(1)
r = requests.post(SEARCH, headers=h, json={"filter": {"collectionId": "prviewacc"}, "limit": 10}, timeout=30)
for v in r.json().get("value", []):
    if v.get("entityType") in ("unknown", None, ""):
        uguid = v["id"]
        uname = v.get("name", "?")
        print(f"\nUnknown in search: '{uname}' guid={uguid}")
        time.sleep(0.5)
        r3 = requests.get(f"{ATLAS}/entity/guid/{uguid}", headers=h, timeout=30)
        if r3.status_code == 200:
            ent = r3.json().get("entity", {})
            tn = ent.get("typeName", "?")
            cid = ent.get("collectionId", "?")
            print(f"  Actual type: {tn}, collectionId: {cid}")
        else:
            print(f"  Fetch failed: {r3.status_code}")
        break

# 3. Check healthcare_data_product
time.sleep(1)
r = requests.post(SEARCH, headers=h, json={"filter": {"entityType": "healthcare_data_product"}, "limit": 5}, timeout=30)
vals = r.json().get("value", [])
if vals:
    dp = vals[0]
    dpguid = dp["id"]
    dpname = dp.get("name", "?")
    dpcoll = dp.get("collectionId", "?")
    print(f"\nData product '{dpname}' search collectionId: {dpcoll}")
    time.sleep(0.5)
    r2 = requests.get(f"{ATLAS}/entity/guid/{dpguid}", headers=h, timeout=30)
    ent = r2.json().get("entity", {})
    print(f"  Actual collectionId: {ent.get('collectionId', '?')}")

print("\nDone!")
