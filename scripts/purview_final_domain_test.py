"""
Final attempt: Use Purview Data Map / governance domain APIs 
to create glossary terms in the upiwjm domain, 
or find another way to link terms to Fabric assets.
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

ACCT = "https://prviewacc.purview.azure.com"
TENANT = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"

# ── 1. Explore governance domain APIs ──
print("=" * 70)
print("1. Exploring governance domain APIs")
print("=" * 70)

apis_to_try = [
    # Data Map API - governance domains
    (ACCT, "/datamap/api/governance-domains", "2023-09-01"),
    (ACCT, "/datamap/api/governance-domains", "2024-03-01"),
    (ACCT, "/datamap/api/governancedomains", "2023-09-01"),
    # Tenant level
    (TENANT, "/datamap/api/governance-domains", "2023-09-01"),
    (TENANT, "/datamap/api/governance-domains", "2024-03-01"),
    # Policy/business domain endpoints
    (ACCT, "/catalog/api/atlas/v2/glossary", None),
    # Try account mgmt 
    (ACCT, "/account/collections/upiwjm?api-version=2019-11-01-preview", None),
]

for base, path, ver in apis_to_try:
    url = f"{base}{path}"
    if ver:
        url += f"?api-version={ver}"
    r = sess.get(url, headers=h, timeout=30)
    label = f"{path} ({ver})" if ver else path
    host = "ACCT" if base == ACCT else "TENANT"
    print(f"  {host} {label}: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        txt = json.dumps(data, indent=2)
        print(f"  {txt[:300]}")
    time.sleep(0.3)

# ── 2. Check the upiwjm collection details ──
print(f"\n{'=' * 70}")
print("2. upiwjm collection details")
print("=" * 70)

r = sess.get(f"{ACCT}/account/collections/upiwjm?api-version=2019-11-01-preview", headers=h, timeout=30)
if r.status_code == 200:
    c = r.json()
    print(json.dumps(c, indent=2))

# ── 3. Check the prviewacc root collection ──
print(f"\n{'=' * 70}")
print("3. prviewacc root collection")
print("=" * 70)

r = sess.get(f"{ACCT}/account/collections/prviewacc?api-version=2019-11-01-preview", headers=h, timeout=30)
if r.status_code == 200:
    c = r.json()
    print(json.dumps(c, indent=2))

# ── 4. Check what bronze_lakehouse entity looks like now ──
print(f"\n{'=' * 70}")
print("4. bronze_lakehouse entity domain details")
print("=" * 70)

body = {"keywords": "bronze_lakehouse", "limit": 3}
r = sess.post(SEARCH, headers=h, json=body, timeout=30)
for a in r.json().get("value", []):
    if a.get("entityType") == "fabric_lake_warehouse" and "bronze" in a.get("name", "").lower():
        guid = a["id"]
        # Get full entity
        r2 = sess.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=30)
        if r2.status_code == 200:
            ent = r2.json().get("entity", {})
            print(f"  Name: {ent.get('attributes', {}).get('name')}")
            print(f"  Domain: {ent.get('collectionId', '?')}")
            print(f"  CollectionId: {ent.get('collectionId')}")
            # Check if there's a domain field
            for k in sorted(ent.keys()):
                if k not in ("attributes", "relationshipAttributes"):
                    print(f"  {k}: {json.dumps(ent[k])[:100]}")
        break

# ── 5. Try creating glossary via tenant API with explicit domain hint ──
print(f"\n{'=' * 70}")
print("5. Creating glossary via tenant API with domain context")
print("=" * 70)

# Try to create a glossary term directly associated with upiwjm
# by using the tenant endpoint and specifying domain metadata

# First check if we can use a different Atlas endpoint on tenant
TENANT_ATLAS = f"{TENANT}/catalog/api/atlas/v2"

# Get glossary from tenant
r = sess.get(f"{TENANT_ATLAS}/glossary", headers=h, timeout=30)
print(f"  Tenant glossary list: {r.status_code}")
if r.status_code == 200:
    for g in r.json():
        print(f"  - {g.get('name')} (guid={g['guid'][:12]}...)")

# ── 6. Try assigning from tenant endpoint ──
print(f"\n{'=' * 70}")
print("6. Trying glossary assignment via tenant endpoint")
print("=" * 70)

# Get term guids from tenant
r = sess.get(f"{TENANT_ATLAS}/glossary", headers=h, timeout=30)
kund_guid = None
for g in r.json():
    if g.get("name") == "Kund":
        kund_guid = g["guid"]

r = sess.get(f"{TENANT_ATLAS}/glossary/{kund_guid}/terms?limit=100", headers=h, timeout=30)
bronze_term = None
for t in r.json():
    if t.get("name") == "Bronze Layer":
        bronze_term = t["guid"]

# Get bronze lakehouse guid 
body = {"keywords": "bronze_lakehouse", "limit": 3}
r = sess.post(f"{TENANT}/catalog/api/search/query?api-version=2022-08-01-preview", 
              headers=h, json=body, timeout=30)
bronze_lh = None
for a in r.json().get("value", []):
    if a.get("entityType") == "fabric_lake_warehouse" and "bronze" in a.get("name", "").lower():
        bronze_lh = a["id"]

if bronze_term and bronze_lh:
    print(f"  Term: {bronze_term}")
    print(f"  Lakehouse: {bronze_lh}")
    
    # Try assignment via tenant endpoint
    r = sess.post(
        f"{TENANT_ATLAS}/glossary/terms/{bronze_term}/assignedEntities",
        headers=h, json=[{"guid": bronze_lh}], timeout=30,
    )
    print(f"  Tenant assignment: {r.status_code}")
    if r.status_code in (200, 204):
        print("  ✅ SUCCESS via tenant!")
    else:
        print(f"  Error: {r.text[:300]}")
        
    # Try relationship API on tenant
    rel_body = {
        "typeName": "AtlasGlossarySemanticAssignment",
        "end1": {"guid": bronze_term, "typeName": "AtlasGlossaryTerm"},
        "end2": {"guid": bronze_lh, "typeName": "fabric_lake_warehouse"},
    }
    r = sess.post(f"{TENANT_ATLAS}/relationship", headers=h, json=rel_body, timeout=30)
    print(f"  Tenant relationship: {r.status_code}")
    if r.status_code in (200, 201):
        print("  ✅ SUCCESS via tenant relationship!")
    else:
        print(f"  Error: {r.text[:200]}")

# ── 7. Summary ──
print(f"\n{'=' * 70}")
print("7. Sammanfattning")
print("=" * 70)
print("""
  Purview-konfigurationen har TVÅ separata root-domäner:
  
  ROOT
  ├── prviewacc (Purview account root) ← Glossary "Kund" bor här
  └── upiwjm (IT)                       ← Alla Fabric-assets bor här
       ├── Analysplattform (4b0vy9)
       ├── Azure Storage (jwdnsw)
       └── SQL Databases (t2dezg)
            ├── Utbildning
            └── Hälsosjukvård
                 ├── Healthcare-Analytics
                 └── BrainChild-FHIR
  
  Purview tillåter INTE cross-domain-operationer via API:
  - Glossary-koppling: 403
  - Kollektion-flytt: 400
  - Scankälla-flytt: 400
  
  LÖSNING: Flytta 'IT' (upiwjm) under 'prviewacc' i Purview-portalen
  (Data Map → Collections → dra IT under prviewacc)
  eller omvänt: flytta scankällan manuellt i portalen.
""")
