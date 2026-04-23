"""Check column entities and apply classifications + move remaining root entities."""
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

DELAY = 0.5

# ============================================
# PHASE 1: Understand column entity structure
# ============================================
print("=" * 60)
print("PHASE 1: Column entity discovery")
print("=" * 60)

# Find the patients table in sql-databases
body = {
    "keywords": "patients",
    "filter": {"and": [{"typeName": "azure_sql_table"}, {"collectionId": "sql-databases"}]},
    "limit": 1,
}
r = session.post(SEARCH, headers=h, json=body, timeout=30)
vals = r.json().get("value", [])
if vals:
    guid = vals[0]["id"]
    qn = vals[0].get("qualifiedName", "?")
    print(f"Table: {qn} [{guid}]")
    time.sleep(DELAY)

    r2 = session.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=30)
    ent = r2.json().get("entity", {})
    rel = ent.get("relationshipAttributes", {})
    cols = rel.get("columns", [])
    print(f"  Columns: {len(cols)}")
    for c in cols:
        cg = c.get("guid", "?")
        cn = c.get("displayText", "?")
        print(f"    {cn} [{cg}]")

    # Classifications on table
    cls_list = ent.get("classifications", [])
    print(f"  Table classifications: {len(cls_list)}")
    for cl in cls_list:
        print(f"    {cl.get('typeName', '?')}")

    # Check first column entity
    if cols:
        col0_guid = cols[0]["guid"]
        time.sleep(DELAY)
        r3 = session.get(f"{ATLAS}/entity/guid/{col0_guid}", headers=h, timeout=30)
        if r3.status_code == 200:
            ce = r3.json().get("entity", {})
            print(f"\n  Sample column entity:")
            print(f"    typeName: {ce.get('typeName', '?')}")
            print(f"    QN: {ce.get('attributes', {}).get('qualifiedName', '?')}")
            print(f"    name: {ce.get('attributes', {}).get('name', '?')}")
            cc = ce.get("classifications", [])
            print(f"    classifications: {len(cc)}")
            for cl in cc:
                print(f"      {cl.get('typeName', '?')}")
        else:
            print(f"  Column entity fetch failed: {r3.status_code}")
else:
    print("No patients table found in sql-databases")

# ============================================
# PHASE 2: Map all SQL column GUIDs
# ============================================
print()
print("=" * 60)
print("PHASE 2: Map all SQL column GUIDs")
print("=" * 60)

SQL_TABLES = ["patients", "encounters", "diagnoses", "vitals_labs", "medications"]
# { "table.column": guid }
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
    print(f"  {tname}: {len(cols)} columns")
    for c in cols:
        cn = c.get("displayText", "?")
        cg = c.get("guid", "?")
        key = f"{tname}.{cn}"
        col_guid_map[key] = cg

# Also check the view
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
    print(f"  vw_ml_encounters: {len(cols)} columns")
    for c in cols:
        cn = c.get("displayText", "?")
        cg = c.get("guid", "?")
        key = f"vw_ml_encounters.{cn}"
        col_guid_map[key] = cg

print(f"\nTotal column GUIDs mapped: {len(col_guid_map)}")

# ============================================
# PHASE 3: Apply custom classifications to SQL columns
# ============================================
print()
print("=" * 60)
print("PHASE 3: Apply custom classifications to SQL columns")
print("=" * 60)

SQL_COL_CLASSIFICATIONS = {
    "patients.patient_id": ["Swedish_Personnummer", "Patient_Name_PHI"],
    "patients.birth_date": ["Patient_Name_PHI"],
    "encounters.patient_id": ["Swedish_Personnummer"],
    "encounters.encounter_id": ["FHIR_Resource_ID"],
    "encounters.icd10_code": ["ICD10_Diagnosis_Code"],
    "encounters.snomed_code": ["SNOMED_CT_Code"],
    "diagnoses.icd10_code": ["ICD10_Diagnosis_Code"],
    "diagnoses.snomed_code": ["SNOMED_CT_Code"],
    "vitals_labs.encounter_id": ["FHIR_Resource_ID"],
    "medications.encounter_id": ["FHIR_Resource_ID"],
    "vw_ml_encounters.patient_id": ["Swedish_Personnummer"],
    "vw_ml_encounters.icd10_code": ["ICD10_Diagnosis_Code"],
}

ok = 0
fail = 0
for col_key, cls_names in SQL_COL_CLASSIFICATIONS.items():
    col_guid = col_guid_map.get(col_key)
    if not col_guid:
        print(f"  SKIP {col_key}: no GUID found")
        fail += 1
        continue
    for cls_name in cls_names:
        time.sleep(DELAY)
        payload = {"classification": {"typeName": cls_name}, "entityGuids": [col_guid]}
        url = f"{ATLAS}/entity/bulk/classification"
        r = session.post(url, headers=h, json=payload, timeout=30)
        if r.status_code in (200, 204):
            print(f"  OK  {col_key} <- {cls_name}")
            ok += 1
        elif r.status_code == 409:
            print(f"  SKIP {col_key} <- {cls_name} (already exists)")
            ok += 1
        else:
            # Try alternative API
            url2 = f"{ATLAS}/entity/guid/{col_guid}/classifications"
            payload2 = [{"typeName": cls_name}]
            r2 = session.post(url2, headers=h, json=payload2, timeout=30)
            if r2.status_code in (200, 204):
                print(f"  OK  {col_key} <- {cls_name} (alt API)")
                ok += 1
            elif r2.status_code == 409:
                print(f"  SKIP {col_key} <- {cls_name} (already exists)")
                ok += 1
            else:
                print(f"  FAIL {col_key} <- {cls_name}: {r.status_code} / {r2.status_code}")
                # Print small error for debugging
                txt = r2.text[:200] if len(r2.text) > 200 else r2.text
                print(f"       {txt}")
                fail += 1

print(f"\nSQL column classifications: {ok} OK, {fail} FAIL")

# ============================================
# PHASE 4: Move remaining root entities to proper collections
# ============================================
print()
print("=" * 60)
print("PHASE 4: Move remaining root entities to collections")
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

print(f"Root entities to process: {len(root_entities)}")

# Categorize entities
type_to_collection = {
    # SQL entities -> sql-databases
    "azure_sql_server": "sql-databases",
    "azure_sql_db": "sql-databases",
    "azure_sql_schema": "sql-databases",
    "azure_sql_table": "sql-databases",
    "azure_sql_view": "sql-databases",
    "azure_sql_column": "sql-databases",
    "azure_sql_stored_procedure": "sql-databases",
    # Healthcare custom types -> appropriate collections
    "healthcare_fhir_service": "fabric-brainchild",
    "healthcare_fhir_resource_type": "fabric-brainchild",
    "healthcare_dicom_service": "fabric-brainchild",
    "healthcare_dicom_modality": "fabric-brainchild",
    "healthcare_data_product": "halsosjukvard",
    # Process entities
    "Process": "fabric-analytics",
    # Data products
    "DataProduct": "halsosjukvard",
}

# Count by type
type_counts = {}
for e in root_entities:
    tn = e.get("entityType", "unknown")
    type_counts[tn] = type_counts.get(tn, 0) + 1

print("\nEntity types in root:")
for tn, cnt in sorted(type_counts.items()):
    target = type_to_collection.get(tn, "?")
    print(f"  {tn}: {cnt}  -> {target}")

# Move entities
moved = 0
skipped = 0
for e in root_entities:
    tn = e.get("entityType", "unknown")
    guid = e.get("id")
    name = e.get("name", "?")
    target = type_to_collection.get(tn)

    # For Process entities, determine by name
    if tn == "Process":
        if "SQL" in name:
            target = "sql-databases"
        elif "FHIR" in name or "DICOM" in name:
            target = "fabric-brainchild"
        else:
            target = "fabric-analytics"

    if not target or target == "?":
        # Default: put remaining in halsosjukvard (the main healthcare parent)
        target = "halsosjukvard"

    time.sleep(DELAY)
    payload = {
        "entity": {
            "guid": guid,
            "typeName": tn,
            "attributes": {"qualifiedName": e.get("qualifiedName", "")},
            "collectionId": target,
        }
    }
    url = f"{DATAMAP}/entity"
    r = session.post(url, headers=h, json=payload, timeout=30)
    if r.status_code in (200, 201):
        moved += 1
    else:
        # Try partial update with collection
        url2 = f"{DATAMAP}/entity/guid/{guid}"
        payload2 = {
            "entity": {
                "typeName": tn,
                "attributes": {"qualifiedName": e.get("qualifiedName", "")},
                "collectionId": target,
            }
        }
        r2 = session.put(url2, headers=h, json=payload2, timeout=30)
        if r2.status_code in (200, 201):
            moved += 1
        else:
            print(f"  FAIL [{tn}] {name} -> {target}: {r.status_code}/{r2.status_code}")
            skipped += 1

print(f"\nMoved: {moved}, Failed: {skipped}")

# ============================================
# PHASE 5: Verify final state
# ============================================
print()
print("=" * 60)
print("PHASE 5: Final verification")
print("=" * 60)

time.sleep(2)
for col_id in ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "barncancer", "fabric-brainchild"]:
    body = {"filter": {"collectionId": col_id}, "limit": 1}
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
    d = r.json()
    count = d.get("@search.count", "?")
    print(f"  {col_id}: {count}")

# Check classifications
print("\nCustom classifications applied:")
for cls_name in ["ICD10_Diagnosis_Code", "Swedish_Personnummer", "FHIR_Resource_ID", "SNOMED_CT_Code", "Patient_Name_PHI", "OMOP_Concept_ID"]:
    body = {"filter": {"classification": cls_name}, "limit": 1}
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
    cnt = r.json().get("@search.count", 0)
    status = "OK" if cnt > 0 else "NOT APPLIED"
    print(f"  {cls_name}: {cnt} ({status})")

# ============================================
# MANUAL STEPS
# ============================================
print()
print("=" * 60)
print("MANUAL STEPS REQUIRED")
print("=" * 60)
print("""
1. MIP SENSITIVITY LABELS:
   Azure Portal -> prviewacc -> Settings -> Information protection -> Enable
   Requires Global Admin or Compliance Administrator role
   Then: Microsoft Purview portal -> Information protection -> Labels -> Create
   Apply labels: "Confidential - PHI", "Internal", "Public"

2. PURVIEW PORTAL ACCESS:
   Classic portal: https://web.purview.azure.com/resource/prviewacc
   New portal: https://purview.microsoft.com
   Required roles:
   - Data Map: Collection Admin + Data Source Admin + Data Curator + Data Reader
   - Governance: Data Governance Administrator (tenant-level)
   Go to: Azure Portal -> prviewacc -> Access control -> Root collection -> Add roles

3. GOVERNANCE DOMAINS:
   New Purview portal -> Data Governance -> Domains -> Create
   Create: "Halsosjukvard", "Barncancerforskning"

4. SQL SCAN RE-TRIGGER (if classifications not visible):
   Classic portal -> Data Map -> Data sources -> sql-hca-demo -> New scan
   Enable: Classification (column-level), Lineage extraction
   Select classification rules: all custom + Swedish_Personnummer etc.

5. FABRIC SCAN LINEAGE:
   Classic portal -> Data Map -> Data sources -> Fabric workspace -> Edit scan
   Enable: Lineage extraction

6. MEDICATIONS DATA (incomplete):
   Re-run: cd c:\\code\\healthcare-analytics\\healthcare-analytics
           python scripts/resume_upload.py
   (Only 20000/60563 rows uploaded for medications table)

7. KEY VAULT SECRET:
   az keyvault secret set --vault-name kv-brainchild --name fhir-service-url --value "https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com"
""")

print("Script complete.")
