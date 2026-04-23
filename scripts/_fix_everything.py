"""
Comprehensive Purview Fix — moves entities to correct collections,
assigns Process entities, applies custom classifications to columns,
fixes VCF term link, and enables sensitivity labels.
"""
import json, sys, os, time, requests

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
SCAN = f"{ACCT}/scan"

ok = 0
fail = 0

def inc_ok():
    global ok; ok += 1
def inc_fail():
    global fail; fail += 1

# ═══════════════════════════════════════════════════════════════════
# STEP 1 — MOVE SQL ENTITIES FROM ROOT → sql-databases
# ═══════════════════════════════════════════════════════════════════
print("=" * 70)
print("  STEP 1: Move SQL entities to sql-databases collection")
print("=" * 70)
for etype in ["azure_sql_table", "azure_sql_view", "azure_sql_schema", "azure_sql_db"]:
    body = {"keywords": "*", "limit": 50, "filter": {"and": [{"entityType": etype}, {"collectionId": "prviewacc"}]}}
    r = requests.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code != 200:
        continue
    entities = r.json().get("value", [])
    for ent in entities:
        guid = ent.get("id", "")
        name = ent.get("name", "?")
        # Move to sql-databases collection
        move_body = {"entityGuids": [guid]}
        mr = requests.post(
            f"{ACCT}/catalog/api/collections/sql-databases/entity?api-version=2022-11-01-preview",
            headers=h, json=move_body, timeout=15,
        )
        if mr.status_code in (200, 204):
            print(f"  OK  moved {name} ({etype}) -> sql-databases")
            inc_ok()
        else:
            # Try alternate API
            mr2 = requests.post(
                f"{ATLAS}/entity/moveTo?collectionId=sql-databases&api-version=2022-03-01-preview",
                headers=h, json={"entityGuids": [guid]}, timeout=15,
            )
            if mr2.status_code in (200, 204):
                print(f"  OK  moved {name} ({etype}) -> sql-databases (alt)")
                inc_ok()
            else:
                # Direct entity update with collection
                r_ent = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
                if r_ent.status_code == 200:
                    entity = r_ent.json().get("entity", {})
                    entity["collectionId"] = "sql-databases"
                    up = requests.post(
                        f"{DATAMAP}/entity",
                        headers=h,
                        json={"entity": entity},
                        timeout=30,
                    )
                    if up.status_code in (200, 201):
                        print(f"  OK  moved {name} ({etype}) -> sql-databases (update)")
                        inc_ok()
                    else:
                        print(f"  FAIL  {name}: {up.status_code} {up.text[:100]}")
                        inc_fail()

# ═══════════════════════════════════════════════════════════════════
# STEP 2 — MOVE FHIR/DICOM ENTITIES FROM ROOT → fabric-brainchild
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 2: Move FHIR/DICOM entities to fabric-brainchild")
print("=" * 70)
for etype in ["healthcare_fhir_service", "healthcare_fhir_resource_type",
              "healthcare_dicom_service", "healthcare_dicom_modality"]:
    body = {"keywords": "*", "limit": 50, "filter": {"and": [{"entityType": etype}, {"collectionId": "prviewacc"}]}}
    r = requests.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code != 200:
        continue
    for ent in r.json().get("value", []):
        guid = ent.get("id", "")
        name = ent.get("name", "?")
        # Update entity via datamap API
        r_ent = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
        if r_ent.status_code == 200:
            entity = r_ent.json().get("entity", {})
            entity["collectionId"] = "fabric-brainchild"
            up = requests.post(
                f"{DATAMAP}/entity",
                headers=h,
                json={"entity": entity},
                timeout=30,
            )
            if up.status_code in (200, 201):
                print(f"  OK  moved {name} -> fabric-brainchild")
                inc_ok()
            else:
                print(f"  FAIL  {name}: {up.status_code} {up.text[:100]}")
                inc_fail()

# ═══════════════════════════════════════════════════════════════════
# STEP 3 — MOVE DATA PRODUCT ENTITIES FROM ROOT
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 3: Move healthcare_data_product to halsosjukvard")
print("=" * 70)
body = {"keywords": "*", "limit": 50, "filter": {"and": [{"entityType": "healthcare_data_product"}, {"collectionId": "prviewacc"}]}}
r = requests.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    for ent in r.json().get("value", []):
        guid = ent.get("id", "")
        name = ent.get("name", "?")
        r_ent = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
        if r_ent.status_code == 200:
            entity = r_ent.json().get("entity", {})
            entity["collectionId"] = "halsosjukvard"
            up = requests.post(
                f"{DATAMAP}/entity",
                headers=h,
                json={"entity": entity},
                timeout=30,
            )
            if up.status_code in (200, 201):
                print(f"  OK  moved {name} -> halsosjukvard")
                inc_ok()
            else:
                print(f"  FAIL  {name}: {up.status_code} {up.text[:100]}")
                inc_fail()

# ═══════════════════════════════════════════════════════════════════
# STEP 4 — ASSIGN PROCESS ENTITIES TO CORRECT COLLECTIONS
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 4: Assign Process entities to collections")
print("=" * 70)
# Map process names to collections
PROCESS_COLLECTION_MAP = {
    "SQL ETL": "sql-databases",
    "FHIR Ingest": "fabric-brainchild",
    "FHIR Transform": "fabric-brainchild",
    "OMOP Transform": "fabric-analytics",
    "ML Pipeline": "fabric-analytics",
    "DICOM": "fabric-brainchild",
    "KG": "fabric-analytics",
}

body = {"keywords": "*", "limit": 50, "filter": {"entityType": "Process"}}
r = requests.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    for ent in r.json().get("value", []):
        guid = ent.get("id", "")
        name = ent.get("name", "?")
        # Determine collection
        target_coll = "fabric-analytics"  # default
        for prefix, coll in PROCESS_COLLECTION_MAP.items():
            if name.startswith(prefix):
                target_coll = coll
                break

        r_ent = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
        if r_ent.status_code == 200:
            entity = r_ent.json().get("entity", {})
            entity["collectionId"] = target_coll
            up = requests.post(
                f"{DATAMAP}/entity",
                headers=h,
                json={"entity": entity},
                timeout=30,
            )
            if up.status_code in (200, 201):
                print(f"  OK  {name} -> {target_coll}")
                inc_ok()
            else:
                print(f"  FAIL  {name}: {up.status_code} {up.text[:100]}")
                inc_fail()
        time.sleep(0.15)

# ═══════════════════════════════════════════════════════════════════
# STEP 5 — APPLY CUSTOM CLASSIFICATIONS TO SQL COLUMNS
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 5: Apply custom classifications to SQL columns")
print("=" * 70)

SQL_COLUMN_CLASSIFICATIONS = {
    "patients": {
        "patient_id": ["Swedish_Personnummer", "Patient_Name_PHI"],
        "birth_date": ["Patient_Name_PHI"],
        "gender": [],  # already has MICROSOFT.PERSONAL.GENDER
        "region": [],
        "postal_code": [],
        "ses_level": [],
        "smoking_status": [],
    },
    "encounters": {
        "patient_id": ["Swedish_Personnummer"],
        "encounter_id": ["FHIR_Resource_ID"],
        "icd10_code": ["ICD10_Diagnosis_Code"],
        "snomed_code": ["SNOMED_CT_Code"],
    },
    "diagnoses": {
        "patient_id": ["Swedish_Personnummer"],
        "icd10_code": ["ICD10_Diagnosis_Code"],
        "snomed_code": ["SNOMED_CT_Code"],
    },
    "vitals_labs": {
        "patient_id": ["Swedish_Personnummer"],
        "encounter_id": ["FHIR_Resource_ID"],
    },
    "medications": {
        "patient_id": ["Swedish_Personnummer"],
        "encounter_id": ["FHIR_Resource_ID"],
    },
    "vw_ml_encounters": {
        "patient_id": ["Swedish_Personnummer"],
        "icd10_code": ["ICD10_Diagnosis_Code"],
    },
}

QN_BASE = "mssql://sql-hca-demo.database.windows.net/HealthcareAnalyticsDB/dbo/hca"

for table_name, col_map in SQL_COLUMN_CLASSIFICATIONS.items():
    # Find table
    body = {"keywords": table_name, "limit": 5, "filter": {"entityType": "azure_sql_table"}}
    if table_name == "vw_ml_encounters":
        body["filter"] = {"entityType": "azure_sql_view"}
    r = requests.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code != 200:
        continue
    table_guid = None
    for v in r.json().get("value", []):
        if v.get("name") == table_name:
            table_guid = v.get("id")
            break
    if not table_guid:
        print(f"  SKIP {table_name}: not found")
        continue

    # Get table with columns
    r_t = requests.get(f"{ATLAS}/entity/guid/{table_guid}", headers=h, timeout=15)
    if r_t.status_code != 200:
        continue
    columns = r_t.json().get("entity", {}).get("relationshipAttributes", {}).get("columns", [])

    for col_ref in columns:
        col_guid = col_ref.get("guid", "")
        col_name_display = col_ref.get("displayText", "")
        # Get actual column name from entity
        r_c = requests.get(f"{ATLAS}/entity/guid/{col_guid}", headers=h, timeout=15)
        if r_c.status_code != 200:
            continue
        col_ent = r_c.json().get("entity", {})
        col_name = col_ent.get("attributes", {}).get("name", col_name_display)
        existing_cls = [c.get("typeName", "") for c in col_ent.get("classifications", [])]

        if col_name not in col_map:
            continue
        new_cls = col_map[col_name]
        if not new_cls:
            continue

        # Add classifications that don't exist yet
        for cls_name in new_cls:
            if cls_name in existing_cls:
                continue
            cls_body = {"classification": {"typeName": cls_name}, "entityGuids": [col_guid]}
            cr = requests.post(
                f"{ATLAS}/entity/bulk/classification",
                headers=h,
                json=cls_body,
                timeout=15,
            )
            if cr.status_code in (200, 204):
                print(f"  OK  {table_name}.{col_name} <- {cls_name}")
                inc_ok()
            else:
                # Try single entity classification
                cr2 = requests.post(
                    f"{ATLAS}/entity/guid/{col_guid}/classifications",
                    headers=h,
                    json=[{"typeName": cls_name}],
                    timeout=15,
                )
                if cr2.status_code in (200, 204):
                    print(f"  OK  {table_name}.{col_name} <- {cls_name} (alt)")
                    inc_ok()
                else:
                    print(f"  FAIL {table_name}.{col_name} <- {cls_name}: {cr2.status_code} {cr2.text[:100]}")
                    inc_fail()
        time.sleep(0.1)

# ═══════════════════════════════════════════════════════════════════
# STEP 6 — APPLY CLASSIFICATIONS TO FABRIC COLUMNS
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 6: Apply classifications to Fabric columns")
print("=" * 70)

FABRIC_COLUMN_CLASSIFICATIONS = {
    "hca_patients": {
        "patient_id": ["Swedish_Personnummer", "Patient_Name_PHI"],
        "birth_date": ["Patient_Name_PHI"],
        "gender": [],
    },
    "hca_encounters": {
        "patient_id": ["Swedish_Personnummer"],
        "encounter_id": ["FHIR_Resource_ID"],
        "icd10_code": ["ICD10_Diagnosis_Code"],
    },
    "hca_diagnoses": {
        "patient_id": ["Swedish_Personnummer"],
        "icd10_code": ["ICD10_Diagnosis_Code"],
    },
    "hca_vitals_labs": {
        "patient_id": ["Swedish_Personnummer"],
    },
    "hca_medications": {
        "patient_id": ["Swedish_Personnummer"],
    },
    "person": {
        "person_source_value": ["Swedish_Personnummer"],
    },
    "condition_occurrence": {
        "condition_source_value": ["ICD10_Diagnosis_Code"],
        "condition_concept_id": ["OMOP_Concept_ID"],
    },
    "drug_exposure": {
        "drug_concept_id": ["OMOP_Concept_ID"],
    },
    "measurement": {
        "measurement_concept_id": ["OMOP_Concept_ID"],
    },
    "visit_occurrence": {
        "visit_concept_id": ["OMOP_Concept_ID"],
    },
    "silver_patient": {
        "patient_id": ["Swedish_Personnummer", "FHIR_Resource_ID"],
    },
    "fhir_bronze_patient": {
        "id": ["FHIR_Resource_ID"],
    },
    "fhir_bronze_observation": {
        "id": ["FHIR_Resource_ID"],
    },
    "ml_features": {
        "patient_id": ["Swedish_Personnummer"],
    },
    "ml_predictions": {
        "patient_id": ["Swedish_Personnummer"],
    },
}

for table_name, col_map in FABRIC_COLUMN_CLASSIFICATIONS.items():
    # Search for the table
    body = {"keywords": table_name, "limit": 5}
    r = requests.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code != 200:
        continue
    table_guid = None
    for v in r.json().get("value", []):
        vname = v.get("name", "")
        vtype = v.get("entityType", "")
        if vname == table_name and "table" in vtype.lower():
            table_guid = v.get("id")
            break
    if not table_guid:
        print(f"  SKIP {table_name}: not found")
        continue

    r_t = requests.get(f"{ATLAS}/entity/guid/{table_guid}", headers=h, timeout=15)
    if r_t.status_code != 200:
        continue
    columns = r_t.json().get("entity", {}).get("relationshipAttributes", {}).get("columns", [])

    for col_ref in columns:
        col_guid = col_ref.get("guid", "")
        r_c = requests.get(f"{ATLAS}/entity/guid/{col_guid}", headers=h, timeout=15)
        if r_c.status_code != 200:
            continue
        col_ent = r_c.json().get("entity", {})
        col_name = col_ent.get("attributes", {}).get("name", "")
        existing_cls = [c.get("typeName", "") for c in col_ent.get("classifications", [])]

        if col_name not in col_map:
            continue
        new_cls = col_map[col_name]
        if not new_cls:
            continue

        for cls_name in new_cls:
            if cls_name in existing_cls:
                continue
            cr = requests.post(
                f"{ATLAS}/entity/guid/{col_guid}/classifications",
                headers=h,
                json=[{"typeName": cls_name}],
                timeout=15,
            )
            if cr.status_code in (200, 204):
                print(f"  OK  {table_name}.{col_name} <- {cls_name}")
                inc_ok()
            else:
                print(f"  FAIL {table_name}.{col_name} <- {cls_name}: {cr.status_code} {cr.text[:80]}")
                inc_fail()
        time.sleep(0.1)

# ═══════════════════════════════════════════════════════════════════
# STEP 7 — FIX VCF TERM-ENTITY LINK
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 7: Fix VCF term-entity link")
print("=" * 70)
# Find VCF term
glossary_guid = "d939ea20-9c67-48af-98d9-b66965f7cde1"
r = requests.get(f"{ATLAS}/glossary/{glossary_guid}/terms?limit=200&offset=0", headers=h, timeout=15)
vcf_term_guid = None
if r.status_code == 200:
    for t in r.json():
        if "VCF" in t.get("name", ""):
            vcf_term_guid = t.get("guid")
            print(f"  VCF term: {t.get('name')} guid={vcf_term_guid}")
            break

# Find a suitable Fabric table to link (brainchild VCF-related)
vcf_targets = []
for kw in ["vcf", "specimen", "brainchild_bronze"]:
    body = {"keywords": kw, "limit": 10}
    r = requests.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        for v in r.json().get("value", []):
            if "table" in v.get("entityType", "").lower():
                vcf_targets.append(v)

if vcf_term_guid and vcf_targets:
    # Link VCF term to matching tables
    for target in vcf_targets[:3]:
        tguid = target.get("id", "")
        tname = target.get("name", "?")
        ttype = target.get("entityType", "?")
        link_body = [
            {
                "guid": tguid,
                "typeName": ttype,
                "displayText": tname,
                "relationshipAttributes": {
                    "typeName": "AtlasGlossarySemanticAssignment",
                    "attributes": {"confidence": 100},
                },
            }
        ]
        lr = requests.post(
            f"{ATLAS}/glossary/terms/{vcf_term_guid}/assignedEntities",
            headers=h,
            json=link_body,
            timeout=15,
        )
        if lr.status_code in (200, 204):
            print(f"  OK  VCF -> {tname}")
            inc_ok()
        else:
            print(f"  FAIL VCF -> {tname}: {lr.status_code} {lr.text[:100]}")
            inc_fail()

# ═══════════════════════════════════════════════════════════════════
# STEP 8 — CREATE GOVERNANCE DOMAINS
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 8: Create Governance Domains")
print("=" * 70)

TENANT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
DG_BASE = f"{TENANT_EP}/datagovernance/catalog"

DOMAINS = [
    {
        "name": "Hälsosjukvård",
        "description": "Domän för hälso- och sjukvårdsdata, inklusive patientregister, diagnoser, läkemedel och klinisk forskning.",
        "owners": [{"id": "admin@MngEnvMCAP522719.onmicrosoft.com", "type": "User"}],
    },
    {
        "name": "Barncancerforskning",
        "description": "Domän för barncancerforskningsdata, inklusive FHIR, DICOM, genomik och kliniska prövningar.",
        "owners": [{"id": "admin@MngEnvMCAP522719.onmicrosoft.com", "type": "User"}],
    },
    {
        "name": "OMOP CDM",
        "description": "Domän för OMOP Common Data Model — standardiserad klinisk data för forskningsanalys.",
        "owners": [{"id": "admin@MngEnvMCAP522719.onmicrosoft.com", "type": "User"}],
    },
]

for dom in DOMAINS:
    for api_v in ["2025-09-15-preview", "2024-03-01-preview"]:
        dr = requests.post(
            f"{DG_BASE}/governanceDomains?api-version={api_v}",
            headers=h,
            json=dom,
            timeout=15,
        )
        if dr.status_code in (200, 201):
            print(f"  OK  domain {dom['name']} ({api_v})")
            inc_ok()
            break
        else:
            print(f"  TRY domain {dom['name']} ({api_v}): {dr.status_code} {dr.text[:150]}")
    else:
        inc_fail()

# ═══════════════════════════════════════════════════════════════════
# STEP 9 — TRIGGER SENSITIVITY LABEL SYNC
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 9: Sensitivity labels / MIP")
print("=" * 70)

# Try to enable auto-labeling on the Purview account
for api_v in ["2022-07-01-preview", "2023-09-01"]:
    r = requests.put(
        f"{SCAN}/sensitivitylabels/default?api-version={api_v}",
        headers=h,
        json={"properties": {"enabled": True}},
        timeout=15,
    )
    print(f"  Enable MIP ({api_v}): {r.status_code} {r.text[:200]}")

# Try to get the current labels
r = requests.get(
    f"{SCAN}/sensitivitylabels?api-version=2022-07-01-preview",
    headers=h,
    timeout=15,
)
print(f"  List labels: {r.status_code} {r.text[:200]}")

# Try Graph API for MIP labels
try:
    graph_tok = cred.get_token("https://graph.microsoft.com/.default").token
    gh = {"Authorization": f"Bearer {graph_tok}", "Content-Type": "application/json"}
    r = requests.get(
        "https://graph.microsoft.com/v1.0/informationProtection/policy/labels",
        headers=gh,
        timeout=15,
    )
    print(f"  Graph MIP labels: {r.status_code}")
    if r.status_code == 200:
        labels = r.json().get("value", [])
        print(f"  Found {len(labels)} MIP labels:")
        for lbl in labels[:10]:
            print(f"    - {lbl.get('name','?')} (id={lbl.get('id','?')[:8]}...)")
except Exception as e:
    print(f"  Graph MIP: {str(e)[:200]}")

# ═══════════════════════════════════════════════════════════════════
# STEP 10 — RE-TRIGGER SQL SCAN WITH LINEAGE
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 10: Re-trigger SQL scan")
print("=" * 70)
# Use the latest successful scan
scan_name = "Scan-abl"
r = requests.put(
    f"{SCAN}/datasources/sql-hca-demo/scans/{scan_name}/runs/default?api-version=2022-07-01-preview",
    headers=h,
    json={"scanLevel": "Full"},
    timeout=15,
)
print(f"  Trigger scan: {r.status_code} {r.text[:200]}")

# ═══════════════════════════════════════════════════════════════════
# VERIFY — Check entity counts after moves
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  VERIFICATION: Entity counts by collection")
print("=" * 70)
time.sleep(5)  # Wait for indexing
for coll in ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "barncancer", "fabric-brainchild"]:
    body = {"keywords": "*", "limit": 1, "filter": {"collectionId": coll}}
    r = requests.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        cnt = r.json().get("@search.count", 0)
        print(f"  {coll}: {cnt} entities")

# Check Process entities collection
body = {"keywords": "*", "limit": 5, "filter": {"entityType": "Process"}}
r = requests.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    for v in r.json().get("value", [])[:3]:
        nm = v.get("name", "?")
        cid = v.get("collectionId", "?")
        print(f"  Process: {nm} -> {cid}")

# Check custom classifications
print("\n  Classification counts:")
for cls in ["ICD10_Diagnosis_Code", "Swedish_Personnummer", "FHIR_Resource_ID", "OMOP_Concept_ID", "SNOMED_CT_Code"]:
    body = {"keywords": "*", "limit": 1, "filter": {"classification": cls}}
    r = requests.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        cnt = r.json().get("@search.count", 0)
        print(f"    {cls}: {cnt}")

print("\n" + "=" * 70)
print(f"  COMPLETE: {ok} OK, {fail} FAIL")
print("=" * 70)

# ═══════════════════════════════════════════════════════════════════
# MANUAL STEPS (printed for user)
# ═══════════════════════════════════════════════════════════════════
print("""
╔══════════════════════════════════════════════════════════════════╗
║  MANUELLA STEG — MÅSTE GÖRAS I AZURE PORTAL / PURVIEW PORTAL  ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  1. MIP / SENSITIVITY LABELS                                    ║
║     → Azure Portal → Microsoft Purview (prviewacc)              ║
║     → Settings → Information Protection                         ║
║     → "Extend labeling to assets in Microsoft Purview"          ║
║     → Consent & Enable                                          ║
║     → Kräver: Global Admin eller Compliance Admin-roll          ║
║                                                                  ║
║  2. DATA GOVERNANCE ADMIN ROLL                                  ║
║     → purview.microsoft.com → Settings → Role assignments       ║
║     → Lägg till dig som "Governance Domain Administrator"       ║
║     → ELLER: Azure Portal → Microsoft Purview → Access control  ║
║                                                                  ║
║  3. GOVERNANCE DOMAINS (om API inte fungerade)                  ║
║     → purview.microsoft.com → Data Governance → Domains         ║
║     → Skapa: "Hälsosjukvård", "Barncancerforskning", "OMOP"    ║
║                                                                  ║
║  4. KLASSISK PORTAL vs NY PORTAL                                ║
║     → Klassisk: web.purview.azure.com (Data Map, Glossary)      ║
║     → Ny UX: purview.microsoft.com (Data Governance)            ║
║     → OM inget syns: prova KLASSISK portalen först!             ║
║     → Navigera: Data Map → Collections → Browse assets          ║
║                                                                  ║
║  5. FABRIC SCAN LINEAGE                                        ║
║     → Azure Portal → Purview → Data Map → Data sources          ║
║     → Fabric → Edit scan → Enable "Lineage extraction"         ║
║     → Re-run Fabric scan                                        ║
║                                                                  ║
║  6. COLLECTION-LEVEL ACCESS                                     ║
║     → Klassisk portal → Data Map → Collections                  ║
║     → Varje collection: Role assignments → verifiera att du     ║
║       har "Collection admin" + "Data reader" på ALLA            ║
║                                                                  ║
║  PORTAL-URLs:                                                   ║
║  Classic:  https://web.purview.azure.com/resource/prviewacc     ║
║  New UX:   https://purview.microsoft.com                        ║
║  Azure:    portal.azure.com → Microsoft Purview → prviewacc     ║
╚══════════════════════════════════════════════════════════════════╝
""")
