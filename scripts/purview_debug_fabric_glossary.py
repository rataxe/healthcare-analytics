"""Debug 403 on Fabric lakehouse glossary term assignment."""
import requests, json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
CATALOG_EP = "https://prviewacc.purview.azure.com"
ATLAS_EP = f"{CATALOG_EP}/catalog/api/atlas/v2"
SEARCH_EP = f"{CATALOG_EP}/catalog/api/search/query?api-version=2022-08-01-preview"
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

# 1. Get a Fabric lakehouse GUID via search
body = {"keywords": "bronze_lakehouse", "limit": 3}
r = sess.post(SEARCH_EP, headers=h, json=body, timeout=30)
results = r.json().get("value", [])
lh = None
for a in results:
    if a.get("entityType") == "fabric_lake_warehouse":
        lh = a
        break

if not lh:
    print("No lakehouse found")
    exit(1)

lh_guid = lh["id"]
lh_name = lh["name"]
print(f"Lakehouse: {lh_name} ({lh_guid})")
print(f"  collectionId: {lh.get('collectionId')}")
print(f"  assetType: {lh.get('assetType')}")

# 2. Get entity via Atlas to see full details
r2 = sess.get(f"{ATLAS_EP}/entity/guid/{lh_guid}", headers=h, timeout=30)
print(f"\n  Atlas GET: {r2.status_code}")
if r2.status_code == 200:
    ent = r2.json().get("entity", {})
    print(f"  typeName: {ent.get('typeName')}")
    print(f"  collectionId: {ent.get('collectionId')}")
    # Check if meanings relationship exists
    meanings = ent.get("relationshipAttributes", {}).get("meanings", [])
    print(f"  Current meanings: {meanings}")
else:
    print(f"  Error: {r2.text[:300]}")

# 3. Get Bronze Layer term GUID
r3 = sess.get(f"{ATLAS_EP}/glossary", headers=h, timeout=30)
glossary_guid = None
for g in r3.json():
    if g.get("name") == "Kund":
        glossary_guid = g["guid"]
        break

r4 = sess.get(f"{ATLAS_EP}/glossary/{glossary_guid}/terms?limit=100", headers=h, timeout=30)
bronze_term = None
for t in r4.json():
    if t.get("name") == "Bronze Layer":
        bronze_term = t
        break

if bronze_term:
    print(f"\nBronze Layer term: {bronze_term['guid'][:12]}...")
    
    # 4. Try assign with full body including typeName
    body = [{"guid": lh_guid, "typeName": "fabric_lake_warehouse"}]
    r5 = sess.post(
        f"{ATLAS_EP}/glossary/terms/{bronze_term['guid']}/assignedEntities",
        headers=h, json=body, timeout=30,
    )
    print(f"\n  Assign with typeName: {r5.status_code}")
    if r5.status_code != 200:
        print(f"  Body: {r5.text[:500]}")
    
    # 5. Try alternative: PUT on term with assignedEntities
    # First get the full term
    r6 = sess.get(f"{ATLAS_EP}/glossary/term/{bronze_term['guid']}", headers=h, timeout=30)
    if r6.status_code == 200:
        term_full = r6.json()
        print(f"\n  Full term keys: {list(term_full.keys())}")
        # Check what assignedEntities looks like for terms that DO work
        # Let's look at Person OMOP which is mapped to patients
        for t in r4.json():
            if t.get("name") == "Person OMOP":
                r7 = sess.get(f"{ATLAS_EP}/glossary/term/{t['guid']}", headers=h, timeout=30)
                if r7.status_code == 200:
                    person_term = r7.json()
                    print(f"\n  Person OMOP assigned: {json.dumps(person_term.get('assignedEntities', [])[:1], indent=2)[:500]}")
                break
    
    # 6. Try using the relationship API directly
    rel_body = {
        "typeName": "AtlasGlossarySemanticAssignment",
        "end1": {
            "guid": lh_guid,
            "typeName": "fabric_lake_warehouse",
        },
        "end2": {
            "guid": bronze_term["guid"],
            "typeName": "AtlasGlossaryTerm",
        },
    }
    r8 = sess.post(f"{ATLAS_EP}/relationship", headers=h, json=rel_body, timeout=30)
    print(f"\n  Relationship API: {r8.status_code}")
    if r8.status_code not in (200, 201):
        print(f"  Error: {r8.text[:500]}")
    else:
        print(f"  Success: {r8.text[:300]}")
