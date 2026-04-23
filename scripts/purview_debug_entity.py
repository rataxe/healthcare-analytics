import requests, json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
CATALOG_EP = "https://prviewacc.purview.azure.com"
ATLAS_EP = f"{CATALOG_EP}/catalog/api/atlas/v2"
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
s = requests.Session()
s.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

# Get patients table entity
guid = "e7945161-bd9b-4edc-8ff7-00f5ff8f6f7b"
r = s.get(f"{ATLAS_EP}/entity/guid/{guid}", headers=h, timeout=30)
if r.status_code == 200:
    ent = r.json()
    e = ent.get("entity", {})
    print("Type:", e.get("typeName"))
    attrs = e.get("attributes", {})
    print("Attributes:", list(attrs.keys()))
    print("userDescription:", attrs.get("userDescription", "N/A"))
    print("description:", attrs.get("description", "N/A"))
    
    # Check columns via relationship
    ra = e.get("relationshipAttributes", {})
    cols = ra.get("columns", [])
    print(f"\nColumns ({len(cols)}):")
    for c in cols[:10]:
        disp = c.get("displayText", "?")
        cguid = c.get("guid", "?")[:12]
        print(f"  {disp} ({cguid}...)")
else:
    print(f"GET entity failed: {r.status_code}")
    
# Also try updating description with the right field
print("\n--- Testing description update ---")
r2 = s.put(
    f"{ATLAS_EP}/entity/guid/{guid}",
    headers=h,
    json={"entity": {"guid": guid, "typeName": "azure_sql_table", "attributes": {"userDescription": "Patient demographics - 10,000 synthetic Swedish patients."}}},
    timeout=30,
)
print(f"PUT description: {r2.status_code}")
if r2.status_code not in (200, 204):
    print(r2.text[:300])
else:
    print(r2.json() if r2.text else "OK")
