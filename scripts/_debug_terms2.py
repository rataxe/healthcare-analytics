"""Debug: test various term GET APIs and entity collection move."""
import requests, json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
tok = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"

# Term GUID to test
TGUID = "83365b1e-990c-49e2-a37e-213b32beefdf"  # FHIR R4

# 1) Test various GET endpoints
endpoints = [
    (f"{ATLAS}/glossary/terms/{TGUID}", "ATLAS no api-ver"),
    (f"{DATAMAP}/glossary/terms/{TGUID}", "DATAMAP no api-ver"),
    (f"{ATLAS}/glossary/term/{TGUID}", "ATLAS /term/ (singular)"),
    (f"{DATAMAP}/glossary/term/{TGUID}", "DATAMAP /term/ (singular)"),
    (f"{ACCT}/catalog/api/atlas/v2/glossary/terms/{TGUID}?api-version=2023-09-01", "ATLAS 2023-09"),
    (f"{ACCT}/datamap/api/glossary/terms/{TGUID}?api-version=2023-09-01", "DATAMAP /glossary/terms 2023"),
]
for url, label in endpoints:
    r = requests.get(url, headers=h, timeout=10)
    status = r.status_code
    extra = ""
    if status == 200:
        data = r.json()
        extra = f" name={data.get('name','?')} assigned={len(data.get('assignedEntities',[]))}"
    elif status != 404:
        extra = f" {r.text[:100]}"
    print(f"{status:3d} {label:50s}{extra}")

# 2) Test bulk term details (POST with guids)
print("\n--- Bulk term details ---")
r = requests.post(f"{ATLAS}/glossary/terms", headers=h, json=[TGUID], timeout=10)
print(f"POST /glossary/terms: {r.status_code}")
if r.status_code != 200:
    # Try DATAMAP
    r = requests.post(f"{DATAMAP}/glossary/terms", headers=h, json=[TGUID], timeout=10)
    print(f"POST DATAMAP /glossary/terms: {r.status_code}")

# 3) Check how entities were originally created - get one entity details
print("\n--- Entity details (FHIR R4 server) ---")
ENTITY_GUID = "dca24c56-4aec-4453-b"  # truncated - need full GUID from search
# Get full GUID from search
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
body = {"keywords": "*", "filter": {"entityType": "healthcare_fhir_service"}, "limit": 5}
r = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r.status_code == 200:
    for ent in r.json().get("value", []):
        eid = ent.get("id", "")
        name = ent.get("name", "?")
        print(f"  Entity: {name} -> {eid}")
        
        # Get entity full detail via Atlas
        r2 = requests.get(f"{ATLAS}/entity/guid/{eid}", headers=h, timeout=10)
        print(f"  GET entity: {r2.status_code}")
        if r2.status_code == 200:
            entity = r2.json().get("entity", {})
            coll = entity.get("collectionId", entity.get("attributes", {}).get("collectionId", "?"))
            print(f"  collectionId in entity: {coll}")
            # Check if there's a collection reference
            rel = entity.get("relationshipAttributes", {})
            meanings = rel.get("meanings", [])
            print(f"  meanings (term links): {len(meanings)}")
            for m in meanings[:3]:
                print(f"    {m.get('displayText','?')} ({m.get('guid','?')[:20]})")
            # Print full entity for debugging
            print(f"  Entity keys: {list(entity.keys())}")
            attrs = entity.get("attributes", {})
            print(f"  Attributes: {json.dumps({k: str(v)[:50] for k, v in attrs.items()}, indent=2)}")
