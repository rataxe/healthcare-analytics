"""Fix remaining: 1.3 moves + 1.4 remaining term links"""
import requests, json, sys, os
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
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
MOVE_API = f"{ACCT}/datamap/api/entity/moveTo"

ok = 0
fail = 0

# ============================================================
# STEP 1: Find and list all healthcare custom entities
# ============================================================
print("=" * 70)
print("STEP 1: FIND CUSTOM HEALTHCARE ENTITIES")
print("=" * 70)

all_custom = []
for kw in ["BrainChild", "FHIR", "DICOM", "healthcare"]:
    r = requests.post(SEARCH, headers=h, json={"keywords": kw, "limit": 20}, timeout=15)
    if r.status_code == 200:
        for e in r.json().get("value", []):
            et = e.get("entityType", "?")
            if "healthcare" in et:
                if e["id"] not in [x["id"] for x in all_custom]:
                    all_custom.append(e)

for e in all_custom:
    coll = e.get("collectionId", "?")
    print(f"  {e['entityType']:35s} | {e['name']:45s} | coll={coll}")

# ============================================================
# STEP 2: Move BrainChild entities to barncancer
# ============================================================
print("\n" + "=" * 70)
print("STEP 2: MOVE TO BARNCANCER")
print("=" * 70)

targets_to_move = []
for e in all_custom:
    name = e.get("name", "")
    coll = e.get("collectionId", "?")
    # Move if: contains "BrainChild" or "Barncancer" and NOT already in barncancer/fabric-brainchild
    if ("brainchild" in name.lower() or "barncancer" in name.lower()):
        if coll not in ("barncancer", "fabric-brainchild"):
            targets_to_move.append(e)

if targets_to_move:
    for e in targets_to_move:
        url = f"{MOVE_API}?collectionId=barncancer&api-version=2023-09-01"
        body = {"entityGuids": [e["id"]]}
        r = requests.post(url, headers=h, json=body, timeout=15)
        if r.status_code in (200, 204):
            print(f"  OK Moved {e['name']} -> barncancer")
            ok += 1
        else:
            print(f"  FAIL {e['name']}: {r.status_code} {r.text[:200]}")
            fail += 1
else:
    print("  No entities need moving to barncancer")

# Also try FHIR and DICOM services that have "BrainChild" in name
for e in all_custom:
    name = e.get("name", "")
    et = e.get("entityType", "")
    coll = e.get("collectionId", "?")
    if ("fhir" in et or "dicom" in et) and coll not in ("barncancer", "fabric-brainchild"):
        url = f"{MOVE_API}?collectionId=barncancer&api-version=2023-09-01"
        body = {"entityGuids": [e["id"]]}
        r = requests.post(url, headers=h, json=body, timeout=15)
        if r.status_code in (200, 204):
            print(f"  OK Moved {e['name']} -> barncancer")
            ok += 1
        else:
            print(f"  FAIL {e['name']}: {r.status_code} {r.text[:200]}")
            fail += 1

# ============================================================
# STEP 3: Re-run term linking for remaining unassigned terms
# ============================================================
print("\n" + "=" * 70)
print("STEP 3: LINK REMAINING TERMS")
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

# Check which are still unassigned
unassigned = []
for t in all_terms:
    r = requests.get(f"{ATLAS}/glossary/term/{t['guid']}", headers=h, timeout=15)
    if r.status_code == 200:
        ft = r.json()
        if not ft.get("assignedEntities"):
            unassigned.append({"guid": t["guid"], "name": ft["name"]})

print(f"  Still unassigned: {len(unassigned)} terms")

# Build entity pool (fresh)
entity_pool = {}
for coll_id in ["sql-databases", "fabric-brainchild", "fabric-analytics", "halsosjukvard", "barncancer"]:
    ents = []
    r = requests.post(SEARCH, headers=h,
                     json={"filter": {"collectionId": coll_id}, "limit": 100}, timeout=15)
    if r.status_code == 200:
        ents = r.json().get("value", [])
    for e in ents:
        entity_pool[e["name"].lower()] = {
            "guid": e["id"],
            "typeName": e.get("entityType", "?"),
            "qualifiedName": e.get("qualifiedName", "")
        }

# Also search for specific entity types
for et in ["Process", "healthcare_data_product", "healthcare_fhir_service",
           "healthcare_dicom_service", "healthcare_fhir_resource", "healthcare_dicom_study"]:
    r = requests.post(SEARCH, headers=h,
                     json={"filter": {"entityType": et}, "limit": 50}, timeout=15)
    if r.status_code == 200:
        for e in r.json().get("value", []):
            entity_pool[e["name"].lower()] = {
                "guid": e["id"],
                "typeName": e.get("entityType", "?"),
                "qualifiedName": e.get("qualifiedName", "")
            }

print(f"  Entity pool: {len(entity_pool)} entities")

# Term → entity mapping (comprehensive)
TERM_MAP = {
    # Barncancerforskning
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
    "Barncancerregistret": [],  # will search
    "Barncancerfonden": [],
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
    "Patologi": [],
    "Protonstrålning": ["medications"],
    "SIOPE (European Paediatric Oncology)": [],
    "Stamcellstransplantation": ["medications", "drug_exposure"],
    "Strålbehandling": ["medications"],
    "Tumörboard": ["encounters"],
    "Vävnadsprov": ["silver_specimen", "specimen"],
    "Överlevnadsstatistik": ["silver_patient", "person"],
    # Klinisk Data
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
    # Dataarkitektur
    "Apache Spark": [],
    "Data Lakehouse": [],
    "Data Lineage": [],
    "Data Mesh": [],
    "Data Quality Score": [],
    "Delta Lake": [],
    "ETL-pipeline": [],
    "Feature Store": ["measurement"],
    "Master Data Management (MDM)": ["patients"],
    "Schema Evolution": [],
    # Kliniska Standarder
    "ATC (Anatomical Therapeutic Chemical Classification)": ["medications", "drug_exposure"],
    "DRG-klassificering": ["diagnoses"],
    "ACMG-klassificering": ["silver_specimen"],
    "NordDRG": ["diagnoses"],
    "ICF (International Classification of Functioning)": ["diagnoses"],
    # Interoperabilitet
    "HL7 v2": [],
    "IHE-profiler": [],
    "Inera": ["patients"],
    "Nationell Patientöversikt (NPÖ)": ["patients"],
    "openEHR": [],
    "CDA (Clinical Document Architecture)": [],
    "Terminologitjänst": [],
    "SITHS-kort": ["patients"],
}

linked = 0
skipped = 0
for term_info in unassigned:
    tn = term_info["name"]
    if tn not in TERM_MAP:
        skipped += 1
        continue

    targets = TERM_MAP[tn]
    if not targets:
        skipped += 1
        continue

    assigned_entities = []
    for tname in targets:
        tl = tname.lower()
        if tl in entity_pool:
            ep = entity_pool[tl]
            assigned_entities.append({
                "guid": ep["guid"],
                "typeName": ep["typeName"],
                "entityStatus": "ACTIVE",
                "displayText": tname,
                "relationshipType": "AtlasGlossarySemanticAssignment",
                "relationshipStatus": "ACTIVE"
            })

    if not assigned_entities:
        skipped += 1
        continue

    r = requests.get(f"{ATLAS}/glossary/term/{term_info['guid']}", headers=h, timeout=15)
    if r.status_code != 200:
        fail += 1
        continue

    term_body = r.json()
    existing = term_body.get("assignedEntities", [])
    existing_guids = {ae["guid"] for ae in existing}
    new_count = 0
    for ae in assigned_entities:
        if ae["guid"] not in existing_guids:
            existing.append(ae)
            new_count += 1

    if new_count == 0:
        skipped += 1
        continue

    term_body["assignedEntities"] = existing
    r2 = requests.put(f"{ATLAS}/glossary/term/{term_info['guid']}",
                     headers=h, json=term_body, timeout=15)
    if r2.status_code in (200, 204):
        matched = [t for t in targets if t.lower() in entity_pool]
        print(f"  OK {tn} -> {', '.join(matched)}")
        linked += 1
        ok += 1
    else:
        print(f"  FAIL {tn}: {r2.status_code} {r2.text[:150]}")
        fail += 1

print(f"\n  Newly linked: {linked}, Skipped: {skipped}")

# ============================================================
# STEP 4: Verify counts
# ============================================================
print("\n" + "=" * 70)
print("STEP 4: VERIFICATION")
print("=" * 70)

# Count classifications
for cls_name in ["SNOMED CT Code", "OMOP Concept ID"]:
    r = requests.post(SEARCH, headers=h,
                     json={"filter": {"classification": cls_name}, "limit": 1}, timeout=15)
    if r.status_code == 200:
        cnt = r.json().get("@search.count", 0)
        print(f"  {cls_name}: {cnt} entities")

# Check barncancer collection
r = requests.post(SEARCH, headers=h,
                 json={"filter": {"collectionId": "barncancer"}, "limit": 1}, timeout=15)
if r.status_code == 200:
    cnt = r.json().get("@search.count", 0)
    print(f"  barncancer collection: {cnt} entities")

# Count assigned terms
total_terms = len(all_terms)
assigned_count = 0
for t in all_terms:
    r = requests.get(f"{ATLAS}/glossary/term/{t['guid']}", headers=h, timeout=15)
    if r.status_code == 200:
        if r.json().get("assignedEntities"):
            assigned_count += 1
print(f"  Glossary terms assigned: {assigned_count}/{total_terms}")

print(f"\n  Total OK: {ok}, Total FAIL: {fail}")
