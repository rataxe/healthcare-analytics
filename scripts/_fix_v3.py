"""
Final fix: Apply classifications (correct names with SPACES) + move non-glossary root entities.
"""
import json, requests, sys, os, time
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
DELAY = 0.8


def api_call(method, url, **kwargs):
    """Wrapper with retry on connection errors."""
    kwargs.setdefault("timeout", 60)
    kwargs.setdefault("headers", h)
    for attempt in range(3):
        try:
            if method == "GET":
                return session.get(url, **kwargs)
            elif method == "POST":
                return session.post(url, **kwargs)
            elif method == "PUT":
                return session.put(url, **kwargs)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                wait = (attempt + 1) * 5
                print(f"  Connection error, retry in {wait}s...")
                time.sleep(wait)
            else:
                raise


# ===================================================
# STEP 1: Apply classifications using CORRECT NAMES (spaces)
# ===================================================
print("=" * 60)
print("STEP 1: Map SQL column GUIDs")
print("=" * 60)

SQL_TABLES = ["patients", "encounters", "diagnoses", "vitals_labs", "medications"]
col_guid_map = {}

for tname in SQL_TABLES:
    body = {
        "keywords": tname,
        "filter": {"and": [{"typeName": "azure_sql_table"}, {"collectionId": "sql-databases"}]},
        "limit": 1,
    }
    time.sleep(DELAY)
    r = api_call("POST", SEARCH, json=body)
    vals = r.json().get("value", [])
    if not vals:
        print(f"  {tname}: NOT FOUND")
        continue
    tguid = vals[0]["id"]
    time.sleep(DELAY)
    r2 = api_call("GET", f"{ATLAS}/entity/guid/{tguid}")
    ent = r2.json().get("entity", {})
    cols = ent.get("relationshipAttributes", {}).get("columns", [])
    for c in cols:
        cn = c.get("displayText", "?")
        cg = c.get("guid", "?")
        col_guid_map[f"{tname}.{cn}"] = cg
    print(f"  {tname}: {len(cols)} columns")

# View
body = {
    "keywords": "vw_ml_encounters",
    "filter": {"and": [{"typeName": "azure_sql_view"}, {"collectionId": "sql-databases"}]},
    "limit": 1,
}
time.sleep(DELAY)
r = api_call("POST", SEARCH, json=body)
vals = r.json().get("value", [])
if vals:
    vguid = vals[0]["id"]
    time.sleep(DELAY)
    r2 = api_call("GET", f"{ATLAS}/entity/guid/{vguid}")
    ent = r2.json().get("entity", {})
    cols = ent.get("relationshipAttributes", {}).get("columns", [])
    for c in cols:
        cn = c.get("displayText", "?")
        cg = c.get("guid", "?")
        col_guid_map[f"vw_ml_encounters.{cn}"] = cg
    print(f"  vw_ml_encounters: {len(cols)} columns")

print(f"  Total: {len(col_guid_map)} columns")

# ===================================================
# STEP 2: Apply classifications (SPACES in names!)
# ===================================================
print()
print("=" * 60)
print("STEP 2: Apply column-level classifications")
print("=" * 60)

# NOTE: classification typeDef names use SPACES not underscores!
COL_CLASSIFICATIONS = {
    "patients.patient_id": ["Swedish Personnummer", "Patient Name PHI"],
    "patients.birth_date": ["Patient Name PHI"],
    "encounters.patient_id": ["Swedish Personnummer"],
    "encounters.encounter_id": ["FHIR Resource ID"],
    "diagnoses.icd10_code": ["ICD10 Diagnosis Code"],
    "diagnoses.encounter_id": ["FHIR Resource ID"],
    "vitals_labs.encounter_id": ["FHIR Resource ID"],
    "medications.encounter_id": ["FHIR Resource ID"],
    "vw_ml_encounters.patient_id": ["Swedish Personnummer"],
    "vw_ml_encounters.primary_icd10": ["ICD10 Diagnosis Code"],
}

ok = 0
fail = 0
for col_key, cls_names in COL_CLASSIFICATIONS.items():
    col_guid = col_guid_map.get(col_key)
    if not col_guid:
        print(f"  SKIP {col_key}: column not found")
        fail += 1
        continue
    for cls_name in cls_names:
        time.sleep(DELAY)
        url = f"{ATLAS}/entity/guid/{col_guid}/classifications"
        payload = [{"typeName": cls_name}]
        r = api_call("POST", url, json=payload)
        if r.status_code in (200, 204):
            print(f"  OK  {col_key} <- {cls_name}")
            ok += 1
        elif r.status_code == 409:
            print(f"  OK  {col_key} <- {cls_name} (already applied)")
            ok += 1
        else:
            print(f"  FAIL {col_key} <- {cls_name}: {r.status_code} {r.text[:150]}")
            fail += 1

print(f"\nClassifications: {ok} OK, {fail} FAIL")

# ===================================================
# STEP 3: Move non-glossary root entities to collections
# ===================================================
print()
print("=" * 60)
print("STEP 3: Move non-glossary root entities")
print("=" * 60)

root_entities = []
offset = 0
while True:
    body = {"filter": {"collectionId": "prviewacc"}, "limit": 50, "offset": offset}
    time.sleep(DELAY)
    r = api_call("POST", SEARCH, json=body)
    d = r.json()
    vals = d.get("value", [])
    root_entities.extend(vals)
    if len(vals) < 50:
        break
    offset += 50

# Separate glossary terms from movable entities
glossary_terms = [e for e in root_entities if e.get("entityType") in ("AtlasGlossaryTerm", "AtlasGlossary", "AtlasGlossaryCategory")]
movable = [e for e in root_entities if e not in glossary_terms]

print(f"Root entities: {len(root_entities)}")
print(f"  Glossary terms (skip): {len(glossary_terms)}")
print(f"  Movable entities: {len(movable)}")

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

moved = 0
already = 0
failed = 0

for e in movable:
    tn = e.get("entityType", "unknown")
    guid = e.get("id")
    name = e.get("name", "?")
    qn = e.get("qualifiedName", "")

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

    time.sleep(DELAY)
    # Use createOrUpdate with collectionId
    payload = {
        "entity": {
            "typeName": tn,
            "attributes": {"qualifiedName": qn, "name": name},
            "collectionId": target,
        }
    }
    r = api_call("POST", f"{DATAMAP}/entity", json=payload)
    if r.status_code in (200, 201):
        moved += 1
    else:
        if failed < 5:
            print(f"  FAIL [{tn}] {name}: {r.status_code} {r.text[:100]}")
        failed += 1

    if (moved + already + failed) % 10 == 0:
        print(f"  Progress: {moved + already + failed}/{len(movable)} (moved={moved}, fail={failed})")

print(f"\nResult: {moved} moved, {failed} failed (+ {len(glossary_terms)} glossary terms left in root)")

# ===================================================
# STEP 4: Final verification
# ===================================================
print()
print("=" * 60)
print("STEP 4: Final state")
print("=" * 60)
time.sleep(3)

for col_id in ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "barncancer", "fabric-brainchild"]:
    body = {"filter": {"collectionId": col_id}, "limit": 1}
    r = api_call("POST", SEARCH, json=body)
    d = r.json()
    count = d.get("@search.count", "?")
    print(f"  {col_id}: {count}")

print("\nClassifications:")
for cn in ["Swedish Personnummer", "Patient Name PHI", "ICD10 Diagnosis Code", "SNOMED CT Code", "FHIR Resource ID", "OMOP Concept ID"]:
    body = {"filter": {"classification": cn}, "limit": 1}
    r = api_call("POST", SEARCH, json=body)
    cnt = r.json().get("@search.count", 0)
    print(f"  {cn}: {cnt}")

print()
print("=" * 60)
print("MANUAL STEPS")
print("=" * 60)
print("""
1. PURVIEW PORTAL ACCESS (why you can't see anything):
   Classic portal: https://web.purview.azure.com/resource/prviewacc
   -> Data Map -> Collections -> Root (prviewacc) -> Role assignments tab
   -> Add yourself to ALL roles: Collection Admin, Data Source Admin, Data Curator, Data Reader
   -> Repeat for each child collection (halsosjukvard, sql-databases, etc.)

   New portal (purview.microsoft.com) requires:
   -> Azure Portal -> Entra ID -> Roles and administrators
   -> Search "Data Governance Administrator" -> Assign to your user

2. MIP SENSITIVITY LABELS:
   Azure Portal -> prviewacc resource -> Settings -> Information protection -> Enable
   Requires Global Admin or Compliance Administrator role

3. GOVERNANCE DOMAINS:
   purview.microsoft.com -> Data Governance -> Domains -> Create
   Create: "Halsosjukvard", "Barncancerforskning"

4. SQL SCAN with classification:
   Classic portal -> Data Map -> Sources -> sql-hca-demo -> New scan
   Enable classification and select all custom rules

5. MEDICATIONS DATA (20000/60563 uploaded):
   python scripts/resume_upload.py

6. KEY VAULT:
   az keyvault secret set --vault-name kv-brainchild --name fhir-service-url --value "https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com"
""")
print("Done!")
