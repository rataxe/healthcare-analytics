"""
Fix term-entity linking using the Atlas Relationship API.
The glossary term PUT endpoint does NOT persist assignedEntities.
Instead, we must create AtlasGlossarySemanticAssignment relationships.
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

# ── STEP 1: Build entity pool ──────────────────────────────────────────────
print("Building entity pool...")
entity_pool = {}

for coll_id in ["sql-databases", "fabric-brainchild", "fabric-analytics",
                "halsosjukvard", "barncancer"]:
    r = requests.post(SEARCH, headers=h,
                     json={"filter": {"collectionId": coll_id}, "limit": 100}, timeout=15)
    if r.status_code == 200:
        for e in r.json().get("value", []):
            entity_pool[e["name"].lower()] = {
                "guid": e["id"],
                "typeName": e.get("entityType", "?"),
            }

for et in ["Process", "healthcare_data_product", "healthcare_fhir_service",
           "healthcare_dicom_service", "healthcare_fhir_resource_type",
           "healthcare_dicom_modality"]:
    r = requests.post(SEARCH, headers=h,
                     json={"filter": {"entityType": et}, "limit": 50}, timeout=15)
    if r.status_code == 200:
        for e in r.json().get("value", []):
            entity_pool[e["name"].lower()] = {
                "guid": e["id"],
                "typeName": e.get("entityType", "?"),
            }

print(f"  Entity pool: {len(entity_pool)} entities")

# ── STEP 2: Get unassigned terms ───────────────────────────────────────────
GG = "d939ea20-9c67-48af-98d9-b66965f7cde1"
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

unassigned = {}
for t in all_terms:
    tg = t["guid"]
    r2 = requests.get(f"{ATLAS}/glossary/term/{tg}", headers=h, timeout=15)
    if r2.status_code == 200:
        ft = r2.json()
        if not ft.get("assignedEntities"):
            unassigned[ft["name"]] = tg

print(f"  Unassigned terms: {len(unassigned)}")

# ── STEP 3: Term → entity mapping ─────────────────────────────────────────
TERM_MAP = {
    "ALL (Akut Lymfatisk Leukemi)": ["diagnoses", "condition_occurrence"],
    "AML (Akut Myeloisk Leukemi)": ["diagnoses", "condition_occurrence"],
    "Leukemi": ["diagnoses", "condition_occurrence"],
    "Lymfom": ["diagnoses", "condition_occurrence"],
    "Neuroblastom": ["diagnoses"],
    "Hjärntumör": ["diagnoses"],
    "Njurtumör (Wilms)": ["diagnoses"],
    "Osteosarkom": ["diagnoses"],
    "Retinoblastom": ["diagnoses"],
    "CNS-tumör": ["diagnoses"],
    "Barncancerfonden": ["patients"],
    "Behandlingsprotokoll": ["medications", "drug_exposure"],
    "Biobank": ["silver_specimen", "specimen"],
    "Biopsi": ["silver_specimen", "specimen"],
    "Cellgiftsbehandling (Kemoterapi)": ["medications", "drug_exposure"],
    "Kemoterapi": ["medications", "drug_exposure"],
    "DNA-sekvensering": ["silver_specimen"],
    "Exom-sekvensering": ["silver_specimen"],
    "Genetisk rådgivning": ["silver_patient"],
    "Genomisk variant": ["silver_specimen"],
    "Immunterapi": ["medications", "drug_exposure"],
    "Klinisk prövning": ["encounters", "visit_occurrence"],
    "Mätbar restsjukdom (MRD)": ["vitals_labs", "measurement"],
    "Minimal Residual Disease (MRD)": ["vitals_labs", "measurement"],
    "Palliativ vård": ["encounters"],
    "Protonterapi": ["medications"],
    "Protonstrålning": ["medications"],
    "Stamcellstransplantation": ["medications", "drug_exposure"],
    "Strålbehandling": ["medications"],
    "Tumörboard": ["encounters"],
    "Vävnadsprov": ["silver_specimen", "specimen"],
    "Överlevnadsstatistik": ["silver_patient", "person"],
    "Överlevnadsanalys": ["silver_patient", "person"],
    # Klinisk Data
    "ALAT/ASAT (Levertransaminaser)": ["vitals_labs", "measurement"],
    "Blodstatus (Hb, LPK, TPK)": ["vitals_labs", "measurement"],
    "CRP (C-reaktivt protein)": ["vitals_labs", "measurement"],
    "Kreatinin": ["vitals_labs", "measurement"],
    "Hemoglobin": ["vitals_labs", "measurement"],
    "Trombocyter": ["vitals_labs", "measurement"],
    "Leukocyter": ["vitals_labs", "measurement"],
    "eGFR (Estimerad Glomerulär Filtration)": ["vitals_labs", "measurement"],
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
    "Triage": ["encounters"],
    "Intensivvård (IVA)": ["encounters"],
    "Kontaktsjuksköterska": ["encounters"],
    "Multidisciplinär konferens (MDK)": ["encounters"],
    "Molekylär tumörboard": ["encounters"],
    "Troponin": ["vitals_labs", "measurement"],
    "GCS (Glasgow Coma Scale)": ["vitals_labs"],
    "NEWS (National Early Warning Score)": ["vitals_labs"],
    "VAS/NRS Smärtskattning": ["vitals_labs"],
    "Ki-67 Proliferationsindex": ["vitals_labs", "measurement"],
    # Dataarkitektur
    "Apache Spark": ["lh_brainchild"],
    "Data Lakehouse": ["lh_brainchild"],
    "Delta Lake": ["lh_brainchild"],
    "Schema Evolution": ["lh_brainchild"],
    "Feature Store": ["measurement"],
    "Master Data Management (MDM)": ["patients"],
    "ETL-pipeline": ["sql to bronze etl"],
    "Data Lineage": ["sql to bronze etl"],
    "Data Quality Score": ["patients"],
    "Data Mesh": ["patients"],
    # Kliniska Standarder
    "ATC (Anatomical Therapeutic Chemical Classification)": ["medications", "drug_exposure"],
    "DRG-klassificering": ["diagnoses"],
    "ACMG-klassificering": ["silver_specimen"],
    "NordDRG": ["diagnoses"],
    "ICF (International Classification of Functioning)": ["diagnoses"],
    "KVÅ (Klassifikation av vårdåtgärder)": ["encounters"],
    # Interoperabilitet
    "HL7 v2": ["brainchild fhir server (r4)"],
    "IHE-profiler": ["brainchild dicom server"],
    "Inera": ["patients"],
    "Nationell Patientöversikt (NPÖ)": ["patients"],
    "openEHR": ["brainchild fhir server (r4)"],
    "CDA (Clinical Document Architecture)": ["brainchild fhir server (r4)"],
    "Terminologitjänst": ["brainchild fhir server (r4)"],
    "SITHS-kort": ["patients"],
    # Barncancer-specifika
    "SBCR (Svenska Barncancerregistret)": ["patients"],
    "NOPHO (Nordic Society of Paediatric Haematology and Oncology)": ["patients"],
    "SIOP (International Society of Paediatric Oncology)": ["patients"],
    "Hodgkins lymfom": ["diagnoses", "condition_occurrence"],
    "Non-Hodgkins lymfom (barn)": ["diagnoses", "condition_occurrence"],
    "Medulloblastom": ["diagnoses"],
    "Ewing sarkom": ["diagnoses"],
    "Rhabdomyosarkom": ["diagnoses"],
    "Wilms tumör (Nefroblastom)": ["diagnoses"],
    "CAR-T cellterapi": ["medications", "drug_exposure"],
    "Seneffekter": ["diagnoses"],
    "Seneffektsmottagning": ["encounters"],
    "Rehabilitering (barn)": ["encounters"],
    "Tumörstadium": ["diagnoses"],
    "Tumörsite": ["diagnoses"],
    "Tumörmutationsbörda (TMB)": ["silver_specimen"],
    "MYCN-amplifiering": ["silver_specimen"],
    "Liquid Biopsy (Flytande biopsi)": ["silver_specimen"],
    "Flödescytometri": ["vitals_labs", "measurement"],
    "Immunhistokemi (IHK)": ["silver_specimen"],
    "FFPE (Formalinfixerat paraffin)": ["silver_specimen"],
    "RNA-sekvensering": ["silver_specimen"],
    "VCF (Variant Call Format)": ["silver_specimen"],
    "Whole Exome Sequencing (WES)": ["silver_specimen"],
    "Whole Genome Sequencing (WGS)": ["silver_specimen"],
    "OMOP Genomics": ["silver_specimen", "specimen"],
    # Juridik/styrning → patients som närmaste entity
    "Personnummer": ["patients"],
    "GDPR i vården": ["patients"],
    "Patientdatalagen (PDL)": ["patients"],
    "Pseudonymisering": ["patients"],
    "Informerat samtycke": ["patients"],
    "Informerat samtycke (kliniskt)": ["patients"],
    "Biobankslagen": ["silver_specimen"],
    "Etikprövning": ["patients"],
    "Etikprövningslagen": ["patients"],
    "GCP (Good Clinical Practice)": ["patients"],
    "Kvalitetsregister": ["patients"],
}

# ── STEP 4: Create relationships via API ────────────────────────────────────
print("\nCreating term-entity relationships...")
ok = 0
fail = 0
skip = 0

# Try both ATLAS and DATAMAP endpoints for relationship creation
REL_URLS = [
    f"{DATAMAP}/relationship",
    f"{ATLAS}/relationship",
]

def create_assignment(term_guid, term_name, entity_guid, entity_type_name):
    """Create a glossary semantic assignment relationship."""
    rel_body = {
        "typeName": "AtlasGlossarySemanticAssignment",
        "attributes": {},
        "status": "ACTIVE",
        "end1": {
            "guid": entity_guid,
            "typeName": entity_type_name,
        },
        "end2": {
            "guid": term_guid,
            "typeName": "AtlasGlossaryTerm",
        },
    }

    for url in REL_URLS:
        r = requests.post(url, headers=h, json=rel_body, timeout=15)
        if r.status_code in (200, 201):
            return True, ""
        # If 409 = already exists, that's OK
        if r.status_code == 409:
            return True, "exists"

    return False, f"{r.status_code} {r.text[:200]}"

for term_name, target_entities in TERM_MAP.items():
    if term_name not in unassigned:
        continue

    term_guid = unassigned[term_name]
    linked_any = False

    for ent_name in target_entities:
        ent_lower = ent_name.lower()
        if ent_lower not in entity_pool:
            continue

        ep = entity_pool[ent_lower]
        success, msg = create_assignment(term_guid, term_name, ep["guid"], ep["typeName"])
        if success:
            linked_any = True
        else:
            print(f"  FAIL {term_name} -> {ent_name}: {msg}")
            fail += 1

    if linked_any:
        matched = [e for e in target_entities if e.lower() in entity_pool]
        print(f"  OK {term_name} -> {', '.join(matched)}")
        ok += 1
    elif term_name in unassigned:
        skip += 1

print(f"\nLinked: {ok}, Failed: {fail}, Skipped/no-entity: {skip}")

# ── STEP 5: Verify ─────────────────────────────────────────────────────────
print("\nVerifying (sampling 10 terms)...")
import random
sample = random.sample(list(unassigned.items()), min(10, len(unassigned)))
verified = 0
for name, guid in sample:
    r = requests.get(f"{ATLAS}/glossary/term/{guid}", headers=h, timeout=15)
    if r.status_code == 200:
        ae = r.json().get("assignedEntities", [])
        status = f"{len(ae)} entities" if ae else "NONE"
        if ae:
            verified += 1
        print(f"  {name}: {status}")
print(f"  Verified {verified}/{len(sample)} sample terms have assignments")
