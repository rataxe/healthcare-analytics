"""
Assign glossary terms to Fabric assets using the tenant-level Purview API.
Fabric assets live in domain 'upiwjm' so we need to create/assign glossary
terms from the Fabric Purview endpoint, not the account-level endpoint.
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

# Account-level (SQL assets + existing glossary)
ACCT_EP = "https://prviewacc.purview.azure.com/catalog/api/atlas/v2"

# Tenant-level Purview endpoint (where Fabric assets live)
TENANT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com/catalog/api/atlas/v2"
TENANT_SEARCH_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com/catalog/api/search/query?api-version=2022-08-01-preview"

# ── Step 1: Check if tenant endpoint has glossaries ──
print("=" * 70)
print("1. Checking tenant-level Purview for glossaries")
print("=" * 70)

r = sess.get(f"{TENANT_EP}/glossary", headers=h, timeout=30)
print(f"  GET /glossary: {r.status_code}")
if r.status_code == 200:
    glossaries = r.json()
    for g in glossaries:
        print(f"    {g.get('name')} ({g.get('guid', '?')[:12]}...)")
else:
    print(f"  Error: {r.text[:300]}")

# ── Step 2: Try to find Fabric assets via tenant endpoint ──
print(f"\n{'=' * 70}")
print("2. Searching for Fabric assets via tenant endpoint")
print("=" * 70)

fabric_assets = {}
for kw in ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse", "gold_omop", "lh_brainchild"]:
    body = {"keywords": kw, "limit": 5}
    r = sess.post(TENANT_SEARCH_EP, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        for a in r.json().get("value", []):
            if a.get("entityType") == "fabric_lake_warehouse" and kw.replace("_", "") in a["name"].replace("_", "").lower():
                fabric_assets[kw] = {"guid": a["id"], "name": a["name"]}
                print(f"  ✅ {a['name']} ({a['id'][:12]}...)")
                break
    time.sleep(0.3)

# ── Step 3: Check the account-level endpoint - does it see those same assets? ──
print(f"\n{'=' * 70}")
print("3. Checking which domain the Bronze Layer term is in")
print("=" * 70)

# Get glossary
r = sess.get(f"{ACCT_EP}/glossary", headers=h, timeout=30)
acct_glossary_guid = None
for g in r.json():
    if g.get("name") == "Kund":
        acct_glossary_guid = g["guid"]
        break

print(f"  Account glossary: {acct_glossary_guid[:12]}...")

# Check if tenant endpoint has the same glossary
r2 = sess.get(f"{TENANT_EP}/glossary/{acct_glossary_guid}", headers=h, timeout=30)
print(f"  Tenant GET glossary {acct_glossary_guid[:12]}: {r2.status_code}")
if r2.status_code == 200:
    print(f"  Same glossary accessible from tenant endpoint!")
    
    # Get terms
    r3 = sess.get(f"{TENANT_EP}/glossary/{acct_glossary_guid}/terms?limit=5", headers=h, timeout=30)
    print(f"  Terms visible: {r3.status_code}")
    if r3.status_code == 200:
        for t in r3.json()[:3]:
            print(f"    {t.get('name')} ({t.get('guid', '?')[:12]}...)")

# ── Step 4: Try assigning term via tenant endpoint ──
print(f"\n{'=' * 70}")
print("4. Trying term assignment via tenant endpoint")
print("=" * 70)

# Get Bronze Layer term from account endpoint
r4 = sess.get(f"{ACCT_EP}/glossary/{acct_glossary_guid}/terms?limit=100", headers=h, timeout=30)
bronze_term_guid = None
for t in r4.json():
    if t.get("name") == "Bronze Layer":
        bronze_term_guid = t["guid"]
        break

if bronze_term_guid and "bronze_lakehouse" in fabric_assets:
    lh_guid = fabric_assets["bronze_lakehouse"]["guid"]
    print(f"  Bronze Layer term: {bronze_term_guid[:12]}...")
    print(f"  bronze_lakehouse:  {lh_guid[:12]}...")
    
    # Try via tenant endpoint
    body = [{"guid": lh_guid}]
    r5 = sess.post(
        f"{TENANT_EP}/glossary/terms/{bronze_term_guid}/assignedEntities",
        headers=h, json=body, timeout=30,
    )
    print(f"\n  Tenant assign: {r5.status_code}")
    if r5.status_code not in (200, 204):
        print(f"  Error: {r5.text[:500]}")
    else:
        print(f"  ✅ SUCCESS!")

    # Also try relationship via tenant
    if r5.status_code >= 400:
        rel_body = {
            "typeName": "AtlasGlossarySemanticAssignment",
            "end1": {"guid": lh_guid, "typeName": "fabric_lake_warehouse"},
            "end2": {"guid": bronze_term_guid, "typeName": "AtlasGlossaryTerm"},
        }
        r6 = sess.post(f"{TENANT_EP}/relationship", headers=h, json=rel_body, timeout=30)
        print(f"\n  Tenant relationship: {r6.status_code}")
        if r6.status_code not in (200, 201):
            print(f"  Error: {r6.text[:500]}")
        else:
            print(f"  ✅ SUCCESS via relationship API!")
