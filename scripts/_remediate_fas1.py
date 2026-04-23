"""
Fas 1 Remediation: Fix all API-automatable issues.
  1.1 Apply SNOMED CT Code to diagnoses + OMOP Fabric tables
  1.2 Apply OMOP Concept ID to OMOP Fabric tables
  1.3 Move 3 entities to barncancer collection
  1.4 Link ~30+ glossary terms to entities
"""
import requests, json, sys, os, time
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8")
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
MOVE_API = f"{ACCT}/datamap/api/entity/moveTo"

ok = 0
fail = 0


def search_entities(filter_dict, limit=50, keywords=None):
    body = {"filter": filter_dict, "limit": limit}
    if keywords:
        body["keywords"] = keywords
    r = requests.post(SEARCH, headers=h, json=body, timeout=15)
    return r.json().get("value", []) if r.status_code == 200 else []


def apply_classification(guid, name, cls_name):
    """Apply a classification to an entity by guid."""
    global ok, fail
    url = f"{DATAMAP}/entity/guid/{guid}/classifications"
    body = [{"typeName": cls_name}]
    r = requests.put(url, headers=h, json=body, timeout=15)
    # Also try POST if PUT returns error (some entities need POST)
    if r.status_code not in (200, 204):
        r = requests.post(url, headers=h, json=body, timeout=15)
    if r.status_code in (200, 204):
        print(f"  ✓ {cls_name} → {name}")
        ok += 1
    else:
        print(f"  ✗ {cls_name} → {name}: {r.status_code} {r.text[:200]}")
        fail += 1


def move_entity(guid, name, target_collection):
    """Move entity to target collection."""
    global ok, fail
    url = f"{MOVE_API}?collectionId={target_collection}&api-version=2023-09-01"
    body = {"entityGuids": [guid]}
    r = requests.post(url, headers=h, json=body, timeout=15)
    if r.status_code in (200, 204):
        print(f"  ✓ Moved {name} → {target_collection}")
        ok += 1
    else:
        print(f"  ✗ Move {name}: {r.status_code} {r.text[:200]}")
        fail += 1


# ============================================================
# 1.1 SNOMED CT Code
# ============================================================
print("=" * 70)
print("1.1 APPLY SNOMED CT Code")
print("=" * 70)

# SQL diagnoses tables (2 instances)
diag_ents = search_entities({"entityType": "azure_sql_table"}, keywords="diagnoses")
for e in diag_ents:
    if "diagnoses" in e.get("name", "").lower():
        apply_classification(e["id"], f"SQL: {e['name']}", "SNOMED CT Code")

# Fabric OMOP tables with SNOMED-relevant data
for tbl_name in ["condition_occurrence", "measurement", "specimen"]:
    ents = search_entities({"entityType": "fabric_lakehouse_table"}, keywords=tbl_name)
    for e in ents:
        if tbl_name in e.get("name", "").lower():
            apply_classification(e["id"], f"Fabric: {e['name']}", "SNOMED CT Code")

# ============================================================
# 1.2 OMOP Concept ID
# ============================================================
print("\n" + "=" * 70)
print("1.2 APPLY OMOP Concept ID")
print("=" * 70)

for tbl_name in ["condition_occurrence", "drug_exposure", "measurement",
                  "person", "specimen", "visit_occurrence"]:
    ents = search_entities({"entityType": "fabric_lakehouse_table"}, keywords=tbl_name)
    for e in ents:
        if tbl_name in e.get("name", "").lower():
            apply_classification(e["id"], f"Fabric: {e['name']}", "OMOP Concept ID")

# Also apply to SQL OMOP-related tables if they exist
# (These are the same diagnoses/medications that have clinical codes mapping to OMOP)
for tbl_name in ["diagnoses", "medications"]:
    ents = search_entities({"entityType": "azure_sql_table"}, keywords=tbl_name)
    for e in ents:
        if tbl_name in e.get("name", "").lower():
            apply_classification(e["id"], f"SQL: {e['name']}", "OMOP Concept ID")

# ============================================================
# 1.3 Move entities to barncancer
# ============================================================
print("\n" + "=" * 70)
print("1.3 MOVE ENTITIES TO BARNCANCER")
print("=" * 70)

# BrainChild Barncancerforskning data product (currently in halsosjukvard)
dp_ents = search_entities({}, keywords="BrainChild Barncancerforskning")
for e in dp_ents:
    if e.get("entityType") == "healthcare_data_product" and "barncancer" in e.get("name", "").lower():
        move_entity(e["id"], e["name"], "barncancer")

# BrainChild FHIR Server
fhir_ents = search_entities({}, keywords="BrainChild FHIR Server")
for e in fhir_ents:
    if e.get("entityType") == "healthcare_fhir_service":
        move_entity(e["id"], e["name"], "barncancer")

# BrainChild DICOM Server
dicom_ents = search_entities({}, keywords="BrainChild DICOM Server")
for e in dicom_ents:
    if e.get("entityType") == "healthcare_dicom_service":
        move_entity(e["id"], e["name"], "barncancer")

# ============================================================
# 1.4 Link glossary terms to entities
# ============================================================
print("\n" + "=" * 70)
print("1.4 LINK GLOSSARY TERMS TO ENTITIES")
print("=" * 70)

GG = "d939ea20-9c67-48af-98d9-b66965f7cde1"

# Get all terms
all_terms = []
offset = 0
while True:
    r = requests.get(f"{ATLAS}/glossary/{GG}/terms?limit=100&offset={offset}",
                     headers=h, timeout=15)
    batch = r.json() if r.status_code == 200 else []
    if not batch:
        break
    all_terms.extend(batch)
    offset += len(batch)
    if len(batch) < 100:
        break

# Get full details for unassigned terms
unassigned_terms = []
for t in all_terms:
    tg = t["guid"]
    r = requests.get(f"{ATLAS}/glossary/term/{tg}", headers=h, timeout=15)
    if r.status_code == 200:
        ft = r.json()
        if not ft.get("assignedEntities"):
            cats = ft.get("categories", [])
            cat_name = cats[0].get("displayText", "?") if cats else "?"
            unassigned_terms.append({"guid": tg, "name": ft["name"], "category": cat_name})

print(f"  Found {len(unassigned_terms)} unassigned terms")

# Build entity lookup for linking
# Get all relevant entities we can link to
entity_pool = {}

# SQL tables
sql_ents = search_entities({"collectionId": "sql-databases"}, limit=30)
for e in sql_ents:
    if e.get("entityType") in ("azure_sql_table", "azure_sql_view"):
        entity_pool[e["name"].lower()] = {
            "guid": e["id"],
            "typeName": e["entityType"],
            "qualifiedName": e.get("qualifiedName", "")
        }

# Fabric BrainChild tables
bc_ents = search_entities({"collectionId": "fabric-brainchild"}, limit=50)
for e in bc_ents:
    entity_pool[e["name"].lower()] = {
        "guid": e["id"],
        "typeName": e["entityType"],
        "qualifiedName": e.get("qualifiedName", "")
    }

# Custom healthcare entities
for kw in ["healthcare_data_product", "healthcare_fhir", "healthcare_dicom"]:
    custom_ents = search_entities({}, keywords=kw, limit=20)
    for e in custom_ents:
        if e.get("entityType", "").startswith("healthcare_"):
            entity_pool[e["name"].lower()] = {
                "guid": e["id"],
                "typeName": e["entityType"],
                "qualifiedName": e.get("qualifiedName", "")
            }

# Lineage processes
proc_ents = search_entities({"entityType": "Process"}, limit=40)
for e in proc_ents:
    entity_pool[e["name"].lower()] = {
        "guid": e["id"],
        "typeName": e["entityType"],
        "qualifiedName": e.get("qualifiedName", "")
    }

# Fabric OMOP tables
for tbl in ["condition_occurrence", "drug_exposure", "measurement", "person",
            "specimen", "visit_occurrence"]:
    omop_ents = search_entities({"entityType": "fabric_lakehouse_table"}, keywords=tbl, limit=5)
    for e in omop_ents:
        entity_pool[e["name"].lower()] = {
            "guid": e["id"],
            "typeName": e["entityType"],
            "qualifiedName": e.get("qualifiedName", "")
        }

print(f"  Entity pool: {len(entity_pool)} entities available for linking")

# Term-to-entity mapping rules
TERM_ENTITY_MAP = {
    # --- Barncancerforskning ---
    "ALL (Akut Lymfatisk Leukemi)": ["diagnoses"],
    "AML (Akut Myeloisk Leukemi)": ["diagnoses"],
    "Leukemi": ["diagnoses", "condition_occurrence"],
    "Lymfom": ["diagnoses", "condition_occurrence"],
    "Neuroblastom": ["diagnoses"],
    "Hjärntumör": ["diagnoses"],
    "Njurtumör (Wilms)": ["diagnoses"],
    "Osteosarkom": ["diagnoses"],
    "Retinoblastom": ["diagnoses"],
    "CNS-tumörer": ["diagnoses"],
    "Barncancerregistret": ["brainchild barncancerforskning"],
    "Barncancerfonden": ["brainchild barncancerforskning"],
    "Behandlingsprotokoll": ["medications", "drug_exposure"],
    "Biobank": ["silver_specimen", "specimen"],
    "Biopsi": ["silver_specimen", "specimen"],
    "Cellgiftsbehandling (Kemoterapi)": ["medications", "drug_exposure"],
    "DNA-sekvensering": ["silver_specimen"],
    "Exom-sekvensering": ["silver_specimen"],
    "Genetisk rådgivning": ["silver_patient"],
    "Genomisk variant": ["silver_specimen"],
    "Immunterapi": ["medications", "drug_exposure"],
    "Klinisk prövning": ["encounters", "visit_occurrence"],
    "Mätbar restsjukdom (MRD)": ["vitals_labs", "measurement"],
    "Palliativ vård": ["encounters"],
    "Patologi": ["brainchild dicom server"],
    "Protonstrålning": ["medications"],
    "SIOPE (European Paediatric Oncology)": ["brainchild barncancerforskning"],
    "Stamcellstransplantation": ["medications", "drug_exposure"],
    "Strålbehandling": ["medications"],
    "Tumörboard": ["encounters"],
    "Vävnadsprov": ["silver_specimen", "specimen"],
    "Överlevnadsstatistik": ["silver_patient", "person"],
    # --- Klinisk Data ---
    "ALAT/ASAT (Levertransaminaser)": ["vitals_labs", "measurement"],
    "Blodstatus (Hb, LPK, TPK)": ["vitals_labs", "measurement"],
    "CRP (C-reaktivt protein)": ["vitals_labs", "measurement"],
    "Kreatinin": ["vitals_labs", "measurement"],
    "Hemoglobin": ["vitals_labs", "measurement"],
    "Trombocyter": ["vitals_labs", "measurement"],
    "Leukocyter": ["vitals_labs", "measurement"],
    "eGFR": ["vitals_labs", "measurement"],
    "Akutmottagning": ["encounters", "visit_occurrence"],
    "Brytpunktssamtal": ["encounters"],
    "Dagvård": ["encounters"],
    "Epikris": ["encounters"],
    "Inskrivning": ["encounters", "visit_occurrence"],
    "Journalanteckning": ["encounters"],
    "Läkemedelsordination": ["medications", "drug_exposure"],
    "Remiss": ["encounters"],
    "Vårdplan": ["encounters"],
    "Vårdtillfälle": ["encounters", "visit_occurrence"],
    "Återbesök": ["encounters", "visit_occurrence"],
    # --- Dataarkitektur ---
    "Apache Spark": ["lh_brainchild"],
    "Data Lakehouse": ["lh_brainchild"],
    "Data Lineage": ["sql to bronze etl"],
    "Data Mesh": ["hälso- och sjukvårdsanalys"],
    "Data Quality Score": ["hälso- och sjukvårdsanalys"],
    "Delta Lake": ["lh_brainchild"],
    "ETL-pipeline": ["sql to bronze etl"],
    "Feature Store": ["measurement"],
    "Master Data Management (MDM)": ["patients"],
    "Schema Evolution": ["lh_brainchild"],
    # --- Kliniska Standarder ---
    "ATC (Anatomical Therapeutic Chemical Classification)": ["medications", "drug_exposure"],
    "DRG-klassificering": ["diagnoses"],
    "ACMG-klassificering": ["silver_specimen"],
    "NordDRG": ["diagnoses"],
    "ICF (International Classification of Functioning)": ["diagnoses"],
    # --- Interoperabilitet ---
    "HL7 v2": ["brainchild fhir server (r4)"],
    "IHE-profiler": ["brainchild dicom server"],
    "Inera": ["patients"],
    "Nationell Patientöversikt (NPÖ)": ["patients"],
    "openEHR": ["brainchild fhir server (r4)"],
    "CDA (Clinical Document Architecture)": ["brainchild fhir server (r4)"],
    "Terminologitjänst": ["brainchild fhir server (r4)"],
    "SITHS-kort": ["patients"],
}

linked = 0
not_found = 0
for term_info in unassigned_terms:
    term_name = term_info["name"]
    if term_name not in TERM_ENTITY_MAP:
        continue

    target_names = TERM_ENTITY_MAP[term_name]
    assigned_entities = []
    for tname in target_names:
        tname_lower = tname.lower()
        if tname_lower in entity_pool:
            ep = entity_pool[tname_lower]
            assigned_entities.append({
                "guid": ep["guid"],
                "typeName": ep["typeName"],
                "entityStatus": "ACTIVE",
                "displayText": tname,
                "relationshipType": "AtlasGlossarySemanticAssignment",
                "relationshipStatus": "ACTIVE"
            })

    if assigned_entities:
        # Get current term details
        r = requests.get(f"{ATLAS}/glossary/term/{term_info['guid']}", headers=h, timeout=15)
        if r.status_code == 200:
            term_body = r.json()
            existing = term_body.get("assignedEntities", [])
            # Merge, avoiding duplicates
            existing_guids = {ae["guid"] for ae in existing}
            for ae in assigned_entities:
                if ae["guid"] not in existing_guids:
                    existing.append(ae)
            term_body["assignedEntities"] = existing
            r2 = requests.put(f"{ATLAS}/glossary/term/{term_info['guid']}",
                             headers=h, json=term_body, timeout=15)
            if r2.status_code in (200, 204):
                ent_names = [t for t in target_names if t.lower() in entity_pool]
                print(f"  ✓ {term_name} → {', '.join(ent_names)}")
                linked += 1
                ok += 1
            else:
                print(f"  ✗ {term_name}: {r2.status_code} {r2.text[:150]}")
                fail += 1
    else:
        not_found += 1

print(f"\n  Linked: {linked}, Entity not found: {not_found}")

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 70)
print("REMEDIATION SUMMARY")
print("=" * 70)
print(f"  Successful operations: {ok}")
print(f"  Failed operations:     {fail}")
print(f"\nFas 1 complete!")
