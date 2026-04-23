"""
Explore Purview governance domains and Data Map API to resolve cross-domain
glossary assignment issue for Fabric assets.
"""
import requests, json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

TENANT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ACCT_EP = "https://prviewacc.purview.azure.com"

# ── Check Data Map API endpoints ──
print("=" * 70)
print("Exploring Data Map API endpoints")
print("=" * 70)

api_versions = ["2023-09-01", "2023-10-01-preview", "2024-03-01-preview", "2024-07-01"]
data_map_paths = [
    "/datamap/api/domains",
    "/datamap/api/glossary",
    "/datamap/api/collections",
    "/account/collections",
]

for path in data_map_paths:
    for ver in api_versions[:2]:
        url = f"{TENANT_EP}{path}?api-version={ver}"
        r = sess.get(url, headers=h, timeout=15)
        if r.status_code in (200, 201):
            print(f"\n  ✅ {path} (api-version={ver}): {r.status_code}")
            data = r.json()
            if isinstance(data, list):
                for item in data[:3]:
                    print(f"    {json.dumps(item, indent=2)[:200]}")
            elif isinstance(data, dict):
                if "value" in data:
                    for item in data["value"][:3]:
                        print(f"    {json.dumps(item, indent=2)[:200]}")
                else:
                    print(f"    {json.dumps(data, indent=2)[:400]}")
            break
        elif r.status_code != 404:
            print(f"\n  ⚠️ {path} ({ver}): {r.status_code} {r.text[:150]}")

# ── Try account-level collections ──
print(f"\n{'=' * 70}")
print("Checking account-level collections")
print("=" * 70)

for ver in api_versions:
    url = f"{ACCT_EP}/account/collections?api-version={ver}"
    r = sess.get(url, headers=h, timeout=15)
    if r.status_code == 200:
        print(f"  ✅ Account collections (api-version={ver})")
        for item in r.json().get("value", [])[:10]:
            name = item.get("name", "?")
            friendly = item.get("friendlyName", "?")
            parent = item.get("parentCollection", {}).get("referenceName", "root")
            print(f"    {name} ({friendly}) parent={parent}")
        break

# ── Check governance domains via Data Map API ──
print(f"\n{'=' * 70}")
print("Checking governance domains")
print("=" * 70)

# Try different domain APIs
domain_paths = [
    f"{TENANT_EP}/datamap/api/governanceDomains",
    f"{TENANT_EP}/catalog/api/businessAssets/domains",
    f"{ACCT_EP}/datamap/api/governanceDomains",
    f"{ACCT_EP}/account/metadata/domains",
]

for path in domain_paths:
    for ver in ["2023-09-01", "2023-10-01-preview"]:
        url = f"{path}?api-version={ver}"
        r = sess.get(url, headers=h, timeout=15)
        if r.status_code in (200, 201):
            print(f"\n  ✅ {path.split('.com')[1]}?api-version={ver}")
            data = r.json()
            if isinstance(data, dict) and "value" in data:
                for item in data["value"][:5]:
                    print(f"    {json.dumps(item, indent=2)[:300]}")
            else:
                print(f"    {json.dumps(data, indent=2)[:500]}")
            break

# ── Try to register Fabric workspace in Purview account collection ──
print(f"\n{'=' * 70}")
print("Checking Fabric workspace scan sources")
print("=" * 70)

scan_ep = f"{ACCT_EP}/scan/datasources?api-version=2022-02-01-preview"
r = sess.get(scan_ep, headers=h, timeout=15)
if r.status_code == 200:
    for ds in r.json().get("value", []):
        name = ds.get("name", "?")
        kind = ds.get("kind", "?")
        coll = ds.get("properties", {}).get("collection", {}).get("referenceName", "?")
        print(f"  {name} [{kind}] collection={coll}")

# ── Check entity with meanings to see what works ──
print(f"\n{'=' * 70}")
print("Checking SQL table entity for meanings relationship structure")
print("=" * 70)

# Get patients table (which has Person OMOP mapped)
qn = "mssql://sql-hca-demo.database.windows.net/HealthcareAnalyticsDB/hca/patients"
r = sess.get(f"{TENANT_EP}/catalog/api/atlas/v2/entity/uniqueAttribute/type/azure_sql_table?attr:qualifiedName={qn}", headers=h, timeout=30)
if r.status_code == 200:
    ent = r.json().get("entity", {})
    meanings = ent.get("relationshipAttributes", {}).get("meanings", [])
    print(f"  patients entity domain: {ent.get('domainId', '?')}")
    print(f"  patients meanings: {json.dumps(meanings[:2], indent=2)[:500]}")
