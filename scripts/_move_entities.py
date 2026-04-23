"""
Move 59 non-glossary root entities using the dedicated collections/entity move API.
"""
import requests, sys, os, time, json
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from azure.identity import AzureCliCredential
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

ACCT = "https://prviewacc.purview.azure.com"
SEARCH = ACCT + "/catalog/api/search/query?api-version=2022-08-01-preview"
ATLAS = ACCT + "/catalog/api/atlas/v2"
DATAMAP = ACCT + "/datamap/api/atlas/v2"

session = requests.Session()
retries = Retry(total=5, backoff_factor=3, status_forcelist=[429, 500, 502, 503])
session.mount("https://", HTTPAdapter(max_retries=retries))

def api(method, url, **kw):
    kw.setdefault("timeout", 60)
    kw.setdefault("headers", h)
    for attempt in range(3):
        try:
            return getattr(session, method.lower())(url, **kw)
        except Exception as e:
            if attempt < 2:
                print(f"  Retry ({type(e).__name__})...")
                time.sleep(5 * (attempt + 1))
            else:
                raise

# Collect all root entities
root_entities = []
offset = 0
while True:
    body = {"filter": {"collectionId": "prviewacc"}, "limit": 50, "offset": offset}
    time.sleep(1)
    r = api("POST", SEARCH, json=body)
    vals = r.json().get("value", [])
    root_entities.extend(vals)
    if len(vals) < 50:
        break
    offset += 50

# Filter out glossary items
GLOSSARY_TYPES = {"AtlasGlossaryTerm", "AtlasGlossary", "AtlasGlossaryCategory"}
movable = [e for e in root_entities if e.get("entityType") not in GLOSSARY_TYPES]
print(f"Root entities: {len(root_entities)} (glossary: {len(root_entities)-len(movable)}, movable: {len(movable)})")

type_to_collection = {
    "azure_sql_server": "sql-databases",
    "azure_sql_db": "sql-databases",
    "azure_sql_schema": "sql-databases",
    "azure_sql_table": "sql-databases",
    "azure_sql_view": "sql-databases",
    "azure_sql_column": "sql-databases",
    "healthcare_fhir_service": "fabric-brainchild",
    "healthcare_fhir_resource_type": "fabric-brainchild",
    "healthcare_dicom_service": "fabric-brainchild",
    "healthcare_dicom_modality": "fabric-brainchild",
    "healthcare_data_product": "halsosjukvard",
}

# ===== APPROACH: Fetch full entity, set collectionId, POST back with GUID =====
print("\n--- Approach: Full entity update with GUID ---")
moved = 0
failed = 0

for e in movable:
    tn = e.get("entityType", "unknown")
    guid = e.get("id")
    name = e.get("name", "?")

    target = type_to_collection.get(tn)
    if not target:
        if tn == "Process":
            if "SQL" in name:
                target = "sql-databases"
            elif "FHIR" in name or "DICOM" in name:
                target = "fabric-brainchild"
            else:
                target = "fabric-analytics"
        else:
            target = "halsosjukvard"

    # Fetch full entity
    time.sleep(1.5)
    r = api("GET", f"{ATLAS}/entity/guid/{guid}")
    if r.status_code != 200:
        if failed < 3:
            print(f"  SKIP [{tn}] {name}: fetch failed {r.status_code}")
        failed += 1
        continue

    full_entity = r.json().get("entity", {})
    
    # Set collectionId on the entity
    full_entity["collectionId"] = target
    
    # Remove relationship attributes (they cause issues in createOrUpdate)
    full_entity.pop("relationshipAttributes", None)
    
    # POST the updated entity
    time.sleep(1.5)
    payload = {"entity": full_entity}
    r2 = api("POST", f"{DATAMAP}/entity", json=payload)
    
    if r2.status_code in (200, 201):
        moved += 1
    else:
        # Try catalog API instead
        time.sleep(1)
        r3 = api("POST", f"{ATLAS}/entity", json=payload)
        if r3.status_code in (200, 201):
            moved += 1
        else:
            if failed < 10:
                err = r2.text[:150] if r2.text else ""
                print(f"  FAIL [{tn}] {name} -> {target}: datamap={r2.status_code} catalog={r3.status_code}")
                print(f"       {err}")
            failed += 1

    if (moved + failed) % 10 == 0:
        print(f"  Progress: {moved + failed}/{len(movable)} (moved={moved}, fail={failed})")

print(f"\nResult: {moved} moved, {failed} failed")

# Verify
print("\nWaiting 5s for index update...")
time.sleep(5)

for col_id in ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "fabric-brainchild"]:
    time.sleep(0.5)
    body = {"filter": {"collectionId": col_id}, "limit": 1}
    r = api("POST", SEARCH, json=body)
    count = r.json().get("@search.count", "?")
    label = "ROOT" if col_id == "prviewacc" else col_id
    print(f"  {label}: {count}")

print("\nDone!")
