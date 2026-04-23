"""
Continuation script — picks up from where _fix_everything.py crashed.
Adds retry logic + delay between calls to avoid rate limiting.
Steps 1-3 already done. Continues from Step 4 remaining + Steps 5-10.
"""
import json, sys, os, time, requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
ATLAS = ACCT + "/catalog/api/atlas/v2"
DATAMAP = ACCT + "/datamap/api/atlas/v2"
SEARCH = ACCT + "/catalog/api/search/query?api-version=2022-08-01-preview"
SCAN = ACCT + "/scan"

# Session with retry
session = requests.Session()
retries = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503])
session.mount("https://", HTTPAdapter(max_retries=retries))

ok = 0
fail = 0
def inc_ok():
    global ok; ok += 1
def inc_fail():
    global fail; fail += 1

DELAY = 0.5  # seconds between API calls

PROCESS_COLLECTION_MAP = {
    "SQL ETL": "sql-databases",
    "FHIR Ingest": "fabric-brainchild",
    "FHIR Transform": "fabric-brainchild",
    "OMOP Transform": "fabric-analytics",
    "ML Pipeline": "fabric-analytics",
    "ML Feature": "fabric-analytics",
    "DICOM Ingest": "fabric-brainchild",
    "Transform: DICOM": "fabric-analytics",
    "Transform: FHIR": "fabric-analytics",
    "KG": "fabric-analytics",
}

# ═══════════════════════════════════════════════════════════════════
# STEP 4 (RETRY) — Assign remaining Process entities
# ═══════════════════════════════════════════════════════════════════
print("=" * 70)
print("  STEP 4: Assign remaining Process entities to collections")
print("=" * 70)
body = {"keywords": "*", "limit": 50, "filter": {"entityType": "Process"}}
r = session.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    for ent in r.json().get("value", []):
        guid = ent.get("id", "")
        name = ent.get("name", "?")
        target_coll = "fabric-analytics"
        for prefix, coll in PROCESS_COLLECTION_MAP.items():
            if name.startswith(prefix):
                target_coll = coll
                break

        # Check if already in correct collection
        time.sleep(DELAY)
        r_ent = session.get(ATLAS + "/entity/guid/" + guid, headers=h, timeout=30)
        if r_ent.status_code == 200:
            entity = r_ent.json().get("entity", {})
            current_coll = entity.get("collectionId", "")
            if current_coll == target_coll:
                print("  SKIP {} (already in {})".format(name, target_coll))
                continue
            entity["collectionId"] = target_coll
            time.sleep(DELAY)
            try:
                up = session.post(DATAMAP + "/entity", headers=h, json={"entity": entity}, timeout=60)
                if up.status_code in (200, 201):
                    print("  OK  {} -> {}".format(name, target_coll))
                    inc_ok()
                else:
                    print("  FAIL {} : {} {}".format(name, up.status_code, up.text[:80]))
                    inc_fail()
            except Exception as e:
                print("  ERR  {}: {}".format(name, str(e)[:80]))
                inc_fail()
                time.sleep(3)

# ═══════════════════════════════════════════════════════════════════
# STEP 5 — Apply custom classifications to SQL columns
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 5: Apply custom classifications to SQL columns")
print("=" * 70)

SQL_COL_CLS = {
    "patients": {"patient_id": ["Swedish_Personnummer", "Patient_Name_PHI"], "birth_date": ["Patient_Name_PHI"]},
    "encounters": {"patient_id": ["Swedish_Personnummer"], "encounter_id": ["FHIR_Resource_ID"], "icd10_code": ["ICD10_Diagnosis_Code"], "snomed_code": ["SNOMED_CT_Code"]},
    "diagnoses": {"patient_id": ["Swedish_Personnummer"], "icd10_code": ["ICD10_Diagnosis_Code"], "snomed_code": ["SNOMED_CT_Code"]},
    "vitals_labs": {"patient_id": ["Swedish_Personnummer"], "encounter_id": ["FHIR_Resource_ID"]},
    "medications": {"patient_id": ["Swedish_Personnummer"], "encounter_id": ["FHIR_Resource_ID"]},
    "vw_ml_encounters": {"patient_id": ["Swedish_Personnummer"], "icd10_code": ["ICD10_Diagnosis_Code"]},
}

for table_name, col_map in SQL_COL_CLS.items():
    etype = "azure_sql_view" if table_name == "vw_ml_encounters" else "azure_sql_table"
    body = {"keywords": table_name, "limit": 5, "filter": {"entityType": etype}}
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code != 200:
        continue
    table_guid = None
    for v in r.json().get("value", []):
        if v.get("name") == table_name:
            table_guid = v.get("id")
            break
    if not table_guid:
        print("  SKIP {}: not found".format(table_name))
        continue

    time.sleep(DELAY)
    r_t = session.get(ATLAS + "/entity/guid/" + table_guid, headers=h, timeout=30)
    if r_t.status_code != 200:
        continue
    columns = r_t.json().get("entity", {}).get("relationshipAttributes", {}).get("columns", [])

    for col_ref in columns:
        col_guid = col_ref.get("guid", "")
        time.sleep(DELAY)
        r_c = session.get(ATLAS + "/entity/guid/" + col_guid, headers=h, timeout=30)
        if r_c.status_code != 200:
            continue
        col_ent = r_c.json().get("entity", {})
        col_name = col_ent.get("attributes", {}).get("name", "")
        existing_cls = [c.get("typeName", "") for c in col_ent.get("classifications", [])]

        if col_name not in col_map:
            continue

        for cls_name in col_map[col_name]:
            if cls_name in existing_cls:
                print("  SKIP {}.{} already has {}".format(table_name, col_name, cls_name))
                continue
            time.sleep(DELAY)
            try:
                cr = session.post(
                    ATLAS + "/entity/guid/" + col_guid + "/classifications",
                    headers=h, json=[{"typeName": cls_name}], timeout=30,
                )
                if cr.status_code in (200, 204):
                    print("  OK  {}.{} <- {}".format(table_name, col_name, cls_name))
                    inc_ok()
                else:
                    print("  FAIL {}.{} <- {}: {} {}".format(table_name, col_name, cls_name, cr.status_code, cr.text[:80]))
                    inc_fail()
            except Exception as e:
                print("  ERR  {}.{}: {}".format(table_name, col_name, str(e)[:80]))
                inc_fail()
                time.sleep(3)

# ═══════════════════════════════════════════════════════════════════
# STEP 6 — Apply classifications to Fabric columns
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 6: Apply classifications to Fabric columns")
print("=" * 70)

FAB_COL_CLS = {
    "hca_patients": {"patient_id": ["Swedish_Personnummer", "Patient_Name_PHI"], "birth_date": ["Patient_Name_PHI"]},
    "hca_encounters": {"patient_id": ["Swedish_Personnummer"], "encounter_id": ["FHIR_Resource_ID"], "icd10_code": ["ICD10_Diagnosis_Code"]},
    "hca_diagnoses": {"patient_id": ["Swedish_Personnummer"], "icd10_code": ["ICD10_Diagnosis_Code"]},
    "hca_vitals_labs": {"patient_id": ["Swedish_Personnummer"]},
    "hca_medications": {"patient_id": ["Swedish_Personnummer"]},
    "person": {"person_source_value": ["Swedish_Personnummer"]},
    "condition_occurrence": {"condition_source_value": ["ICD10_Diagnosis_Code"], "condition_concept_id": ["OMOP_Concept_ID"]},
    "drug_exposure": {"drug_concept_id": ["OMOP_Concept_ID"]},
    "measurement": {"measurement_concept_id": ["OMOP_Concept_ID"]},
    "visit_occurrence": {"visit_concept_id": ["OMOP_Concept_ID"]},
    "silver_patient": {"patient_id": ["Swedish_Personnummer", "FHIR_Resource_ID"]},
    "fhir_bronze_patient": {"id": ["FHIR_Resource_ID"]},
    "ml_features": {"patient_id": ["Swedish_Personnummer"]},
    "ml_predictions": {"patient_id": ["Swedish_Personnummer"]},
}

for table_name, col_map in FAB_COL_CLS.items():
    body = {"keywords": table_name, "limit": 10}
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
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
        print("  SKIP {}: not found".format(table_name))
        continue

    time.sleep(DELAY)
    r_t = session.get(ATLAS + "/entity/guid/" + table_guid, headers=h, timeout=30)
    if r_t.status_code != 200:
        continue
    columns = r_t.json().get("entity", {}).get("relationshipAttributes", {}).get("columns", [])

    for col_ref in columns:
        col_guid = col_ref.get("guid", "")
        time.sleep(DELAY)
        r_c = session.get(ATLAS + "/entity/guid/" + col_guid, headers=h, timeout=30)
        if r_c.status_code != 200:
            continue
        col_ent = r_c.json().get("entity", {})
        col_name = col_ent.get("attributes", {}).get("name", "")
        existing_cls = [c.get("typeName", "") for c in col_ent.get("classifications", [])]

        if col_name not in col_map:
            continue

        for cls_name in col_map[col_name]:
            if cls_name in existing_cls:
                print("  SKIP {}.{} already has {}".format(table_name, col_name, cls_name))
                continue
            time.sleep(DELAY)
            try:
                cr = session.post(
                    ATLAS + "/entity/guid/" + col_guid + "/classifications",
                    headers=h, json=[{"typeName": cls_name}], timeout=30,
                )
                if cr.status_code in (200, 204):
                    print("  OK  {}.{} <- {}".format(table_name, col_name, cls_name))
                    inc_ok()
                else:
                    print("  FAIL {}.{} <- {}: {} {}".format(table_name, col_name, cls_name, cr.status_code, cr.text[:80]))
                    inc_fail()
            except Exception as e:
                print("  ERR  {}.{}: {}".format(table_name, col_name, str(e)[:80]))
                inc_fail()
                time.sleep(3)

# ═══════════════════════════════════════════════════════════════════
# STEP 7 — Fix VCF term-entity link
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 7: Fix VCF term-entity link")
print("=" * 70)
glossary_guid = "d939ea20-9c67-48af-98d9-b66965f7cde1"
r = session.get(ATLAS + "/glossary/" + glossary_guid + "/terms?limit=200&offset=0", headers=h, timeout=30)
vcf_guid = None
if r.status_code == 200:
    for t in r.json():
        if "VCF" in t.get("name", ""):
            vcf_guid = t.get("guid")
            print("  VCF term: {} guid={}".format(t.get("name"), vcf_guid))
            break

if vcf_guid:
    # Find specimen or brainchild table
    for kw in ["specimen", "brainchild"]:
        body = {"keywords": kw, "limit": 10}
        r = session.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            for v in r.json().get("value", []):
                if "table" in v.get("entityType", "").lower():
                    tguid = v.get("id", "")
                    tname = v.get("name", "?")
                    ttype = v.get("entityType", "?")
                    link_body = [{
                        "guid": tguid,
                        "typeName": ttype,
                        "displayText": tname,
                        "relationshipAttributes": {
                            "typeName": "AtlasGlossarySemanticAssignment",
                            "attributes": {"confidence": 100},
                        },
                    }]
                    time.sleep(DELAY)
                    lr = session.post(
                        ATLAS + "/glossary/terms/" + vcf_guid + "/assignedEntities",
                        headers=h, json=link_body, timeout=30,
                    )
                    if lr.status_code in (200, 204):
                        print("  OK  VCF -> {}".format(tname))
                        inc_ok()
                    elif lr.status_code == 400 and "already" in lr.text.lower():
                        print("  SKIP VCF -> {} (already linked)".format(tname))
                    else:
                        print("  FAIL VCF -> {}: {} {}".format(tname, lr.status_code, lr.text[:100]))
                        inc_fail()
                    break
            break

# ═══════════════════════════════════════════════════════════════════
# STEP 8 — Governance Domains
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 8: Governance Domains")
print("=" * 70)
TENANT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
DOMAINS = [
    {"name": "Halsosjukvard", "description": "Klinisk patientdata och sjukvard"},
    {"name": "Barncancerforskning", "description": "Barncancerforskningsdata inkl FHIR DICOM genomik"},
    {"name": "OMOP CDM", "description": "OMOP Common Data Model for forskning"},
]
for dom in DOMAINS:
    for api_v in ["2025-09-15-preview"]:
        time.sleep(DELAY)
        dr = session.post(
            TENANT_EP + "/datagovernance/catalog/governanceDomains?api-version=" + api_v,
            headers=h, json=dom, timeout=15,
        )
        print("  Domain {} ({}): {} {}".format(dom["name"], api_v, dr.status_code, dr.text[:150]))

# ═══════════════════════════════════════════════════════════════════
# STEP 9 — Sensitivity labels (MIP)
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 9: Sensitivity labels (MIP)")
print("=" * 70)

# Check Graph API for labels
try:
    graph_tok = cred.get_token("https://graph.microsoft.com/.default").token
    gh = {"Authorization": "Bearer " + graph_tok, "Content-Type": "application/json"}
    r = session.get("https://graph.microsoft.com/v1.0/informationProtection/policy/labels", headers=gh, timeout=15)
    print("  Graph MIP labels: {}".format(r.status_code))
    if r.status_code == 200:
        labels = r.json().get("value", [])
        print("  Found {} labels:".format(len(labels)))
        for lbl in labels[:10]:
            print("    - {} (id={})".format(lbl.get("name", "?"), lbl.get("id", "?")[:12]))
    elif r.status_code == 403:
        print("  403 — need InformationProtectionPolicy.Read permission")
    else:
        print("  {}".format(r.text[:200]))
except Exception as e:
    print("  Graph: {}".format(str(e)[:200]))

# Try beta API for sensitivity labels
try:
    r = session.get("https://graph.microsoft.com/beta/security/informationProtection/sensitivityLabels", headers=gh, timeout=15)
    print("  Beta sensitivity labels: {}".format(r.status_code))
    if r.status_code == 200:
        labels = r.json().get("value", [])
        print("  Found {} sensitivity labels".format(len(labels)))
        for lbl in labels[:10]:
            print("    - {} (id={})".format(lbl.get("name", "?"), lbl.get("id", "?")[:12]))
except Exception as e:
    print("  Beta: {}".format(str(e)[:200]))

# Try Purview sensitivity label extension
for api_v in ["2022-07-01-preview", "2023-09-01"]:
    r = session.put(
        SCAN + "/sensitivitylabels/default?api-version=" + api_v,
        headers=h, json={"properties": {"enabled": True}}, timeout=15,
    )
    print("  Enable auto-labeling ({}): {} {}".format(api_v, r.status_code, r.text[:150]))

# ═══════════════════════════════════════════════════════════════════
# STEP 10 — Re-trigger SQL scan
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  STEP 10: Re-trigger SQL scan")
print("=" * 70)
r = session.put(
    SCAN + "/datasources/sql-hca-demo/scans/Scan-abl/runs/default?api-version=2022-07-01-preview",
    headers=h, json={"scanLevel": "Full"}, timeout=30,
)
print("  Trigger: {} {}".format(r.status_code, r.text[:200]))

# ═══════════════════════════════════════════════════════════════════
# VERIFY
# ═══════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  VERIFICATION")
print("=" * 70)
time.sleep(5)

print("  Entity counts:")
for coll in ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "barncancer", "fabric-brainchild"]:
    body = {"keywords": "*", "limit": 1, "filter": {"collectionId": coll}}
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        cnt = r.json().get("@search.count", 0)
        print("    {}: {}".format(coll, cnt))
    time.sleep(0.2)

print("\n  Classification counts:")
for cls in ["ICD10_Diagnosis_Code", "Swedish_Personnummer", "FHIR_Resource_ID", "OMOP_Concept_ID", "SNOMED_CT_Code", "Patient_Name_PHI"]:
    body = {"keywords": "*", "limit": 1, "filter": {"classification": cls}}
    r = session.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        cnt = r.json().get("@search.count", 0)
        print("    {}: {}".format(cls, cnt))
    time.sleep(0.2)

print("\n  Process entities with collection:")
body = {"keywords": "*", "limit": 5, "filter": {"entityType": "Process"}}
r = session.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    for v in r.json().get("value", [])[:5]:
        nm = v.get("name", "?")
        cid = v.get("collectionId", "?")
        print("    {} -> {}".format(nm, cid))

print("\n" + "=" * 70)
print("  TOTAL: {} OK, {} FAIL".format(ok, fail))
print("=" * 70)

print("""
=== MANUELLA STEG (du maste gora i portalen) ===

1. MIP / SENSITIVITY LABELS:
   -> Azure Portal -> Microsoft Purview (prviewacc)
   -> Settings -> "Information protection"
   -> Klicka "Extend labeling to assets in Microsoft Purview Data Map"
   -> Consent & Enable
   -> Kraver: Global Admin eller Compliance Admin roll
   -> ALTERNATIVT: Microsoft 365 Compliance Center
      -> compliance.microsoft.com -> Information protection -> Labels
      -> Skapa labels: Konfidentiell, Intern, Offentlig, PHI-Skyddad

2. PURVIEW UI - VILKEN PORTAL?
   -> KLASSISK: https://web.purview.azure.com/resource/prviewacc
      -> Har du: Data Map, Glossary, Classifications, Lineage
      -> Ga till: Data Map -> Browse -> By collection
   -> NY UX: https://purview.microsoft.com
      -> Har du: Data Governance, Data Estate Health, Glossary
      -> KRAVER: Data Governance Administrator-roll
      -> Ga till: Settings -> Role assignments -> lagg till dig

3. GOVERNANCE DOMAINS (om de inte skapades automatiskt):
   -> purview.microsoft.com -> Data Governance -> Domains
   -> Skapa: Halsosjukvard, Barncancerforskning, OMOP CDM

4. FABRIC SCAN LINEAGE:
   -> Klassisk portal -> Data Map -> Data sources -> Fabric
   -> Edit scan -> Kryssa i "Lineage extraction"
   -> Re-run scan

5. COLLECTION PERMISSIONS (viktigast!):
   -> Klassisk portal -> Data Map -> Collections
   -> For VARJE collection: Role assignments
   -> Se till att du har ALLA roller:
      - Collection admin
      - Data source admin
      - Data curator
      - Data reader
   -> Propagera till child-collections

6. PURVIEW KONTO-ROLLER (Azure Portal):
   -> portal.azure.com -> prviewacc -> Access control (IAM)
   -> Lagg till: Purview Data Curator, Purview Data Reader
   -> pa SUBSCRIPTION-niva eller Resource Group-niva
""")
