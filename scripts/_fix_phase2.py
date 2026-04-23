"""
Phase 2: Fix classifications + move root entities.
1. List existing classification typeDefs (find correct names)
2. Create any missing classification typeDefs
3. Apply classifications to SQL column entities by GUID
4. Move remaining 210 root entities to proper collections
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
retries = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503])
session.mount("https://", HTTPAdapter(max_retries=retries))
DELAY = 0.3

# ============================================
# STEP 1: List all classification typeDefs
# ============================================
print("=" * 60)
print("STEP 1: Discover existing classification typeDefs")
print("=" * 60)

r = session.get(f"{ATLAS}/types/typedefs", headers=h, timeout=30)
td = r.json()
cls_defs = td.get("classificationDefs", [])
print(f"Total classification typeDefs: {len(cls_defs)}")

# Filter to show custom ones (not system)
custom_cls = {}
for c in cls_defs:
    name = c.get("name", "?")
    cat = c.get("category", "?")
    creator = c.get("createdBy", "")
    # Custom if creator is not Microsoft/system
    if "microsoft" not in creator.lower() and creator != "ExpertClassifier":
        custom_cls[name] = c
        print(f"  Custom: {name} (by {creator})")

# Also show names containing our keywords
for c in cls_defs:
    name = c.get("name", "?")
    for kw in ["ICD", "Person", "FHIR", "SNOMED", "PHI", "OMOP", "Swedish"]:
        if kw.lower() in name.lower() and name not in custom_cls:
            print(f"  Match: {name}")
            custom_cls[name] = c
            break

# ============================================
# STEP 2: Create missing classification typeDefs
# ============================================
print()
print("=" * 60)
print("STEP 2: Ensure classification typeDefs exist")
print("=" * 60)

NEEDED_CLASSIFICATIONS = [
    "Swedish_Personnummer",
    "Patient_Name_PHI",
    "ICD10_Diagnosis_Code",
    "SNOMED_CT_Code",
    "FHIR_Resource_ID",
    "OMOP_Concept_ID",
]

for cls_name in NEEDED_CLASSIFICATIONS:
    if cls_name in custom_cls:
        print(f"  EXISTS: {cls_name}")
    else:
        # Check with spaces instead of underscores
        space_name = cls_name.replace("_", " ")
        found = False
        for existing_name in custom_cls:
            if existing_name.replace(" ", "_") == cls_name or existing_name == space_name:
                print(f"  EXISTS (as '{existing_name}'): {cls_name}")
                found = True
                break
        if not found:
            # Create it
            payload = {
                "classificationDefs": [
                    {
                        "name": cls_name,
                        "description": f"Custom classification: {cls_name}",
                        "category": "CLASSIFICATION",
                        "typeVersion": "1.0",
                        "attributeDefs": [],
                    }
                ]
            }
            time.sleep(DELAY)
            r = session.post(f"{ATLAS}/types/typedefs", headers=h, json=payload, timeout=30)
            if r.status_code in (200, 201):
                print(f"  CREATED: {cls_name}")
            elif r.status_code == 409:
                print(f"  EXISTS (conflict): {cls_name}")
            else:
                print(f"  FAIL creating {cls_name}: {r.status_code} {r.text[:200]}")

# Re-fetch to get final list
time.sleep(1)
r = session.get(f"{ATLAS}/types/typedefs", headers=h, timeout=30)
td = r.json()
all_cls_names = set(c["name"] for c in td.get("classificationDefs", []))
print(f"\nClassification typeDefs after creation: {len(all_cls_names)}")
for cn in NEEDED_CLASSIFICATIONS:
    status = "OK" if cn in all_cls_names else "MISSING"
    print(f"  {cn}: {status}")

# ============================================
# STEP 3: Map SQL column GUIDs
# ============================================
print()
print("=" * 60)
print("STEP 3: Map SQL column GUIDs")
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
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
    vals = r.json().get("value", [])
    if not vals:
        print(f"  {tname}: NOT FOUND")
        continue
    tguid = vals[0]["id"]
    time.sleep(DELAY)
    r2 = session.get(f"{ATLAS}/entity/guid/{tguid}", headers=h, timeout=30)
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
r = session.post(SEARCH, headers=h, json=body, timeout=30)
vals = r.json().get("value", [])
if vals:
    vguid = vals[0]["id"]
    time.sleep(DELAY)
    r2 = session.get(f"{ATLAS}/entity/guid/{vguid}", headers=h, timeout=30)
    ent = r2.json().get("entity", {})
    cols = ent.get("relationshipAttributes", {}).get("columns", [])
    for c in cols:
        cn = c.get("displayText", "?")
        cg = c.get("guid", "?")
        col_guid_map[f"vw_ml_encounters.{cn}"] = cg
    print(f"  vw_ml_encounters: {len(cols)} columns")

print(f"Total columns mapped: {len(col_guid_map)}")

# Print available column names for debugging
print("\nAvailable columns:")
for key in sorted(col_guid_map.keys()):
    print(f"  {key}")

# ============================================
# STEP 4: Apply classifications to SQL columns
# ============================================
print()
print("=" * 60)
print("STEP 4: Apply classifications to SQL columns")
print("=" * 60)

# Map: column_key -> [classification_names]
# Using actual column names from the mapped GUIDs
SQL_COL_CLASSIFICATIONS = {
    "patients.patient_id": ["Swedish_Personnummer", "Patient_Name_PHI"],
    "patients.birth_date": ["Patient_Name_PHI"],
    "encounters.patient_id": ["Swedish_Personnummer"],
    "encounters.encounter_id": ["FHIR_Resource_ID"],
    "diagnoses.icd10_code": ["ICD10_Diagnosis_Code"],
    "vitals_labs.encounter_id": ["FHIR_Resource_ID"],
    "medications.encounter_id": ["FHIR_Resource_ID"],
    "vw_ml_encounters.patient_id": ["Swedish_Personnummer"],
}

# Dynamically add icd10/snomed if they exist as columns
for tname in ["encounters", "diagnoses", "vw_ml_encounters"]:
    for col_suffix in ["icd10_code", "icd_code", "icd10"]:
        key = f"{tname}.{col_suffix}"
        if key in col_guid_map and key not in SQL_COL_CLASSIFICATIONS:
            SQL_COL_CLASSIFICATIONS[key] = ["ICD10_Diagnosis_Code"]
    for col_suffix in ["snomed_code", "snomed", "snomed_ct_code"]:
        key = f"{tname}.{col_suffix}"
        if key in col_guid_map and key not in SQL_COL_CLASSIFICATIONS:
            SQL_COL_CLASSIFICATIONS[key] = ["SNOMED_CT_Code"]

ok = 0
fail = 0
for col_key, cls_names in SQL_COL_CLASSIFICATIONS.items():
    col_guid = col_guid_map.get(col_key)
    if not col_guid:
        print(f"  SKIP {col_key}: column not found")
        fail += 1
        continue
    for cls_name in cls_names:
        if cls_name not in all_cls_names:
            print(f"  SKIP {col_key} <- {cls_name}: typedef not found")
            fail += 1
            continue
        time.sleep(DELAY)
        # Use the entity/guid/classifications endpoint
        url = f"{ATLAS}/entity/guid/{col_guid}/classifications"
        payload = [{"typeName": cls_name}]
        r = session.post(url, headers=h, json=payload, timeout=30)
        if r.status_code in (200, 204):
            print(f"  OK  {col_key} <- {cls_name}")
            ok += 1
        elif r.status_code == 409:
            print(f"  OK  {col_key} <- {cls_name} (already exists)")
            ok += 1
        else:
            # Try bulk API
            url2 = f"{ATLAS}/entity/bulk/classification"
            payload2 = {"classification": {"typeName": cls_name}, "entityGuids": [col_guid]}
            r2 = session.post(url2, headers=h, json=payload2, timeout=30)
            if r2.status_code in (200, 204):
                print(f"  OK  {col_key} <- {cls_name} (bulk API)")
                ok += 1
            elif r2.status_code == 409:
                print(f"  OK  {col_key} <- {cls_name} (already exists)")
                ok += 1
            else:
                print(f"  FAIL {col_key} <- {cls_name}: {r.status_code}/{r2.status_code}")
                err = r.text[:200] if r.text else r2.text[:200]
                print(f"       {err}")
                fail += 1

print(f"\nClassifications: {ok} OK, {fail} FAIL")

# ============================================
# STEP 5: Move remaining root entities
# ============================================
print()
print("=" * 60)
print("STEP 5: Move 210 root entities to proper collections")
print("=" * 60)

# Get ALL root entities
root_entities = []
offset = 0
while True:
    body = {"filter": {"collectionId": "prviewacc"}, "limit": 50, "offset": offset}
    time.sleep(DELAY)
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
    d = r.json()
    vals = d.get("value", [])
    root_entities.extend(vals)
    if len(vals) < 50:
        break
    offset += 50

print(f"Root entities found: {len(root_entities)}")

type_to_collection = {
    "azure_sql_server": "sql-databases",
    "azure_sql_db": "sql-databases",
    "azure_sql_schema": "sql-databases",
    "azure_sql_table": "sql-databases",
    "azure_sql_view": "sql-databases",
    "azure_sql_column": "sql-databases",
    "azure_sql_stored_procedure": "sql-databases",
    "healthcare_fhir_service": "fabric-brainchild",
    "healthcare_fhir_resource_type": "fabric-brainchild",
    "healthcare_dicom_service": "fabric-brainchild",
    "healthcare_dicom_modality": "fabric-brainchild",
    "healthcare_data_product": "halsosjukvard",
}

moved = 0
failed = 0

for e in root_entities:
    tn = e.get("entityType", "unknown")
    guid = e.get("id")
    name = e.get("name", "?")
    qn = e.get("qualifiedName", "")

    # Determine target collection
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
            # Default to halsosjukvard for custom/unknown types
            target = "halsosjukvard"

    time.sleep(DELAY)

    # First try: create/update entity with collectionId
    payload = {
        "entity": {
            "guid": guid,
            "typeName": tn if tn != "unknown" else None,
            "attributes": {"qualifiedName": qn},
            "collectionId": target,
        }
    }

    # We need the typeName - fetch it if unknown
    if not tn or tn in ("unknown", "?"):
        r_ent = session.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=30)
        if r_ent.status_code == 200:
            ent_data = r_ent.json().get("entity", {})
            tn = ent_data.get("typeName", "unknown")
            qn = ent_data.get("attributes", {}).get("qualifiedName", qn)
            payload["entity"]["typeName"] = tn
            payload["entity"]["attributes"]["qualifiedName"] = qn
        time.sleep(DELAY)

    # Use datamap API for collection assignment
    url = f"{DATAMAP}/entity"
    r = session.post(url, headers=h, json=payload, timeout=30)
    if r.status_code in (200, 201):
        moved += 1
    else:
        # Try createOrUpdate with full entity
        url2 = f"{DATAMAP}/entity"
        payload2 = {
            "entity": {
                "typeName": tn,
                "attributes": {
                    "qualifiedName": qn,
                    "name": name,
                },
                "collectionId": target,
            }
        }
        r2 = session.post(url2, headers=h, json=payload2, timeout=30)
        if r2.status_code in (200, 201):
            moved += 1
        else:
            if failed < 10:  # Only print first 10 failures
                print(f"  FAIL [{tn}] {name} -> {target}: {r.status_code}/{r2.status_code}")
            failed += 1

    if (moved + failed) % 20 == 0:
        print(f"  Progress: {moved + failed}/{len(root_entities)} (moved: {moved}, failed: {failed})")

print(f"\nResult: {moved} moved, {failed} failed")

# ============================================
# STEP 6: Verification
# ============================================
print()
print("=" * 60)
print("STEP 6: Final verification")
print("=" * 60)

time.sleep(3)
for col_id in ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "barncancer", "fabric-brainchild"]:
    body = {"filter": {"collectionId": col_id}, "limit": 1}
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
    d = r.json()
    count = d.get("@search.count", "?")
    label = "ROOT" if col_id == "prviewacc" else col_id
    print(f"  {label}: {count}")

print("\nClassifications applied:")
for cls_name in NEEDED_CLASSIFICATIONS:
    body = {"filter": {"classification": cls_name}, "limit": 1}
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
    cnt = r.json().get("@search.count", 0)
    print(f"  {cls_name}: {cnt}")

print()
print("=" * 60)
print("MANUAL STEPS REQUIRED")
print("=" * 60)
print("""
1. MIP SENSITIVITY LABELS:
   Azure Portal -> prviewacc -> Settings -> Information protection
   -> Enable sensitivity labeling -> Accept consent
   Requires: Global Admin or Compliance Administrator role
   Then create labels: "Confidential - PHI", "Internal - Healthcare", "Public"

2. PURVIEW PORTAL ACCESS (why you can't see anything):
   a) Classic portal: https://web.purview.azure.com/resource/prviewacc
      -> Collections -> Root -> Role assignments
      -> Add yourself as: Collection Admin, Data Source Admin, Data Curator, Data Reader
   b) New portal: https://purview.microsoft.com
      Requires: "Data Governance Administrator" role in Entra ID
      Azure Portal -> Entra ID -> Roles -> Data Governance Administrator -> Assign

3. GOVERNANCE DOMAINS (manual in new portal):
   purview.microsoft.com -> Data Governance -> Domains -> Create
   Domains: "Halsosjukvard", "Barncancerforskning"

4. SQL SCAN (to pick up column classifications automatically):
   Classic portal -> Data Map -> Data sources -> sql-hca-demo
   -> New scan -> Enable classification -> Select custom rules
   -> Enable lineage extraction -> Run

5. MEDICATIONS DATA (incomplete upload):
   python scripts/resume_upload.py (20000/60563 uploaded)

6. KEY VAULT SECRET:
   az keyvault secret set --vault-name kv-brainchild \\
     --name fhir-service-url \\
     --value "https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com"
""")

print("Done!")
