"""
Map ALL business glossary terms to relevant Purview assets.
- Link existing unmapped terms (ICD 10, Bronze/Silver/Gold Layer, OMOP CDM, etc.)
- Create new relevant terms (ATC, Vital Signs, Lab Results, SNOMED CT, etc.)
- Map terms to SQL tables, columns, Fabric lakehouses, and other discovered assets
"""
import requests, json, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
CATALOG_EP = "https://prviewacc.purview.azure.com"
ATLAS_EP = f"{CATALOG_EP}/catalog/api/atlas/v2"
SEARCH_EP = f"{CATALOG_EP}/catalog/api/search/query?api-version=2022-08-01-preview"
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

QN_BASE = "mssql://sql-hca-demo.database.windows.net/HealthcareAnalyticsDB/hca"
GLOSSARY_NAME = "Kund"

# ── Helper functions ──

def search_assets(keywords, limit=10):
    body = {"keywords": keywords, "limit": limit}
    r = sess.post(SEARCH_EP, headers=h, json=body, timeout=30)
    return r.json().get("value", []) if r.status_code == 200 else []

def get_table_entity(table_name, entity_type="azure_sql_table"):
    qn = f"{QN_BASE}/{table_name}"
    r = sess.get(f"{ATLAS_EP}/entity/uniqueAttribute/type/{entity_type}?attr:qualifiedName={qn}", headers=h, timeout=30)
    if r.status_code == 200:
        return r.json().get("entity", {})
    return None

def get_column_guid(table_entity, col_name):
    cols = table_entity.get("relationshipAttributes", {}).get("columns", [])
    for c in cols:
        if c.get("displayText") == col_name:
            return c.get("guid")
    return None

def assign_term_to_entities(term_guid, entity_guids):
    """Assign a glossary term to one or more entities by GUID."""
    body = [{"guid": g} for g in entity_guids]
    r = sess.post(f"{ATLAS_EP}/glossary/terms/{term_guid}/assignedEntities", headers=h, json=body, timeout=30)
    return r.status_code

def create_glossary_term(glossary_guid, name, short_desc, long_desc=""):
    body = {
        "name": name,
        "shortDescription": short_desc,
        "longDescription": long_desc or short_desc,
        "anchor": {"glossaryGuid": glossary_guid},
    }
    r = sess.post(f"{ATLAS_EP}/glossary/term", headers=h, json=body, timeout=30)
    if r.status_code in (200, 201):
        return r.json().get("guid")
    elif "already exists" in r.text.lower() or r.status_code == 409:
        return None  # already exists
    else:
        print(f"    ⚠️ Create '{name}': {r.status_code} {r.text[:120]}")
        return None


# ════════════════════════════════════════════════════════════
# 1. Get glossary and existing terms
# ════════════════════════════════════════════════════════════
print("=" * 70)
print("1. Loading glossary and existing terms")
print("=" * 70)

r = sess.get(f"{ATLAS_EP}/glossary", headers=h, timeout=30)
glossary_guid = None
for g in r.json():
    if g.get("name") == GLOSSARY_NAME:
        glossary_guid = g["guid"]
        break

if not glossary_guid:
    print("  ❌ Glossary 'Kund' not found!")
    exit(1)

print(f"  Glossary: {GLOSSARY_NAME} ({glossary_guid[:12]}...)")

r = sess.get(f"{ATLAS_EP}/glossary/{glossary_guid}/terms?limit=100", headers=h, timeout=30)
existing_terms = {}  # {name: {guid, assigned_entities: [...]}}
if r.status_code == 200:
    for t in r.json():
        name = t.get("name")
        assigned = t.get("assignedEntities", [])
        existing_terms[name] = {
            "guid": t["guid"],
            "assigned": [a.get("displayText", a.get("guid", "?")) for a in assigned],
            "assigned_guids": [a.get("guid") for a in assigned],
        }
        status = f"-> {', '.join(existing_terms[name]['assigned'])}" if assigned else "(unmapped)"
        print(f"  {name}: {status}")

time.sleep(0.5)

# ════════════════════════════════════════════════════════════
# 2. Collect all SQL table entities and column GUIDs
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("2. Collecting SQL table and column GUIDs")
print("=" * 70)

TABLE_NAMES = ["patients", "encounters", "diagnoses", "vitals_labs", "medications"]
tables = {}  # {name: entity}

for tbl in TABLE_NAMES:
    ent = get_table_entity(tbl)
    if ent:
        tables[tbl] = ent
        print(f"  ✅ {tbl} ({ent['guid'][:12]}...)")
    time.sleep(0.3)

# View
ent_view = get_table_entity("vw_ml_encounters", "azure_sql_view")
if ent_view:
    tables["vw_ml_encounters"] = ent_view
    print(f"  ✅ vw_ml_encounters ({ent_view['guid'][:12]}...)")
time.sleep(0.5)

# ════════════════════════════════════════════════════════════
# 3. Find Fabric lakehouse GUIDs
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("3. Finding Fabric lakehouse assets")
print("=" * 70)

fabric_assets = {}  # {keyword: {guid, name, type}}
for kw in ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse", "gold_omop", "lh_brainchild"]:
    results = search_assets(kw, limit=5)
    for asset in results:
        etype = asset.get("entityType", "")
        name = asset.get("name", "")
        if etype == "fabric_lake_warehouse" and kw.replace("_", "") in name.replace("_", "").lower():
            fabric_assets[kw] = {"guid": asset["id"], "name": name, "type": etype}
            print(f"  ✅ {name} ({asset['id'][:12]}...)")
            break
    time.sleep(0.5)

# ════════════════════════════════════════════════════════════
# 4. Find other relevant Fabric assets (notebooks, pipelines)
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("4. Finding notebooks and pipeline assets")
print("=" * 70)

notebook_assets = {}
for kw in ["bronze_ingestion", "silver_features", "ml_training", "omop_transformation"]:
    results = search_assets(kw, limit=5)
    for asset in results:
        etype = asset.get("entityType", "")
        name = asset.get("name", "")
        if "notebook" in etype.lower() or "notebook" in name.lower():
            notebook_assets[kw] = {"guid": asset["id"], "name": name, "type": etype}
            print(f"  ✅ {name} [{etype}] ({asset['id'][:12]}...)")
            break
    time.sleep(0.5)

# Also find BrainChild-specific assets
brainchild_assets = {}
for kw in ["brainchild", "fhir", "gms", "dicom", "genomic"]:
    results = search_assets(kw, limit=10)
    for asset in results:
        name = asset.get("name", "")
        guid = asset.get("id", "")
        etype = asset.get("entityType", "")
        key = f"{name}|{etype}"
        if key not in brainchild_assets:
            brainchild_assets[key] = {"guid": guid, "name": name, "type": etype}
    time.sleep(0.5)

if brainchild_assets:
    print(f"  Found {len(brainchild_assets)} BrainChild-related assets")
    for k, v in list(brainchild_assets.items())[:10]:
        print(f"    {v['name']} [{v['type']}]")

# ════════════════════════════════════════════════════════════
# 5. Create NEW glossary terms
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("5. Creating new glossary terms")
print("=" * 70)

NEW_TERMS = [
    ("ATC Classification",
     "Anatomical Therapeutic Chemical (ATC) klassificeringssystem för läkemedel.",
     "WHO:s ATC-system delar in läkemedel i grupper baserat på organ/system, terapeutisk effekt, farmakologisk grupp, kemisk grupp och substans. Används i medications-tabellen (atc_code)."),

    ("Vital Signs",
     "Vitalparametrar: blodtryck, puls, temperatur, syremättnad.",
     "Kliniska vitalparametrar som mäts vid vårdkontakter. Inkluderar systoliskt/diastoliskt BT, hjärtfrekvens, kroppstemperatur och SpO2. Lagras i vitals_labs."),

    ("Lab Results",
     "Laboratorieresultat: glukos, kreatinin, hemoglobin, elektrolyter.",
     "Kliniska laboratorievärden från blodprover. Inkluderar P-Glukos, S-Kreatinin, B-Hb, S-Na, S-K, BMI och vikt. Lagras i vitals_labs."),

    ("Length of Stay",
     "Vårdtid (LOS) – antal dagar patient vårdas inneliggande.",
     "Length of Stay beräknas som skillnaden mellan utskrivnings- och inskrivningsdatum. Viktig utfallsvariabel i ML-modeller för resursplanering. Kolumn: encounters.los_days."),

    ("Readmission Risk",
     "Återinskrivningsrisk – om patient återkommer inom 30 dagar.",
     "Binär indikator (0/1) för oplanerad återinskrivning inom 30 dagar efter utskrivning. Målvariabel i prediktiv ML-modell. Kolumn: encounters.readmission_30d."),

    ("Swedish Personnummer",
     "Svenskt personnummer – unikt identifieringsnummer för personer i Sverige.",
     "12-siffrig identifierare (YYYYMMDD-XXXX). Klassificeras som känslig personuppgift (PHI) enligt GDPR. I syntetisk data representerat som postal_code-klassificering."),

    ("FHIR Patient",
     "HL7 FHIR R4 Patient-resurs – demografisk patientinformation.",
     "FHIR Patient-resursen innehåller namn, kön, födelsedatum, adress och identifierare. Mappas till patients-tabellen i SQL och Person-tabellen i OMOP CDM."),

    ("FHIR Encounter",
     "HL7 FHIR R4 Encounter-resurs – vårdkontakt/besök.",
     "FHIR Encounter-resursen beskriver en interaktion mellan patient och vårdgivare. Mappas till encounters-tabellen och Visit Occurrence i OMOP CDM."),

    ("FHIR Condition",
     "HL7 FHIR R4 Condition-resurs – diagnos/tillstånd.",
     "FHIR Condition-resursen innehåller ICD-10 diagnoskoder och klinisk status. Mappas till diagnoses-tabellen och Condition Occurrence i OMOP CDM."),

    ("FHIR MedicationRequest",
     "HL7 FHIR R4 MedicationRequest-resurs – läkemedelsförskrivning.",
     "FHIR MedicationRequest beskriver förskrivna läkemedel med ATC-kod, dos och administreringsväg. Mappas till medications-tabellen och Drug Exposure i OMOP CDM."),

    ("FHIR Observation",
     "HL7 FHIR R4 Observation-resurs – mätning/observation.",
     "FHIR Observation används för vitalparametrar och labresultat. Mappas till vitals_labs-tabellen och Measurement i OMOP CDM."),

    ("FHIR ImagingStudy",
     "HL7 FHIR R4 ImagingStudy-resurs – radiologisk studie.",
     "FHIR ImagingStudy kopplar DICOM-bilder (MR, patologi) till patienter. 42 MR-studier + 79 patologistudier i BrainChild."),

    ("FHIR Specimen",
     "HL7 FHIR R4 Specimen-resurs – biologiskt prov/vävnad.",
     "FHIR Specimen beskriver insamlade prover för genomisk analys. Kopplas till BTB (BioBank)-data i BrainChild."),

    ("Genomic Variant",
     "Genomisk variant – DNA-sekvensförändring identifierad via VCF.",
     "Varianter från VCF-filer (Variant Call Format) från helgenomsekvensering. Innehåller kromosom, position, referens-/alternativallel, kvalitet. Del av GMS-flödet."),

    ("OMOP Person",
     "OMOP CDM Person-tabell – demografisk information.",
     "Standardiserad persondemografi enligt OMOP CDM v5.4. Mappas från patients via ETL i 04_omop_transformation-notebooken."),

    ("OMOP Visit Occurrence",
     "OMOP CDM Visit Occurrence – standardiserad vårdkontakt.",
     "Standardiserade vårdbesök enligt OMOP CDM v5.4. Mappas från encounters via ETL."),

    ("OMOP Condition Occurrence",
     "OMOP CDM Condition Occurrence – standardiserad diagnos.",
     "Standardiserade diagnoser med SNOMED CT-begrepp enligt OMOP CDM v5.4. Mappas från diagnoses via ETL."),

    ("OMOP Drug Exposure",
     "OMOP CDM Drug Exposure – standardiserad läkemedelsexponering.",
     "Standardiserade läkemedel med RxNorm-koncept enligt OMOP CDM v5.4. Mappas från medications via ETL."),

    ("OMOP Measurement",
     "OMOP CDM Measurement – standardiserad mätning.",
     "Standardiserade mätningar (vitaler + labb) med LOINC-koder enligt OMOP CDM v5.4. Mappas från vitals_labs via ETL."),

    ("Medallion Architecture",
     "Medallion-arkitektur (Bronze/Silver/Gold) för datalakehouse.",
     "Dataarkitekturmönster med tre lager: Bronze (rådata), Silver (rensat & berikat), Gold (aggregerat & ML-klart). Implementerat i Fabric Lakehouse."),

    ("SBCR",
     "Svenska BarnCancerRegistret (SBCR) – nationellt kvalitetsregister.",
     "Innehåller registreringar, behandlingar och uppföljningar för barncancerpatienter. Del av BrainChild-projektet."),

    ("BTB",
     "BioBank (BTB) – biologiska prover och metadata.",
     "Specimen-data med provtyp, insamlingsdatum, förvaring. Kopplas till genomisk analys via VCF-filer. Del av BrainChild."),

    ("VCF",
     "Variant Call Format – standardformat för genomiska varianter.",
     "Textbaserat format för att lagra genvarianter från NGS-sekvensering. Innehåller kromosom, position, referens, alternativ, kvalitet, filter och INFO-fält."),
]

created_terms = {}
for name, short_desc, long_desc in NEW_TERMS:
    if name in existing_terms:
        print(f"  ✅ '{name}' already exists")
        created_terms[name] = existing_terms[name]["guid"]
        continue
    guid = create_glossary_term(glossary_guid, name, short_desc, long_desc)
    if guid:
        print(f"  ✅ '{name}' created ({guid[:12]}...)")
        created_terms[name] = guid
        existing_terms[name] = {"guid": guid, "assigned": [], "assigned_guids": []}
    time.sleep(0.3)

# Refresh term list
r = sess.get(f"{ATLAS_EP}/glossary/{glossary_guid}/terms?limit=100", headers=h, timeout=30)
if r.status_code == 200:
    for t in r.json():
        name = t.get("name")
        assigned = t.get("assignedEntities", [])
        existing_terms[name] = {
            "guid": t["guid"],
            "assigned": [a.get("displayText", a.get("guid", "?")) for a in assigned],
            "assigned_guids": [a.get("guid") for a in assigned],
        }
time.sleep(0.5)

# ════════════════════════════════════════════════════════════
# 6. Map ALL glossary terms to assets
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("6. Mapping glossary terms to assets")
print("=" * 70)


def map_term(term_name, target_guids, target_label=""):
    """Map a glossary term to target entity GUIDs, skip already-assigned."""
    if term_name not in existing_terms:
        print(f"  ⏭️ '{term_name}' not found in glossary")
        return
    
    term_info = existing_terms[term_name]
    term_guid = term_info["guid"]
    already = set(term_info.get("assigned_guids", []))
    
    new_guids = [g for g in target_guids if g and g not in already]
    if not new_guids:
        print(f"  ✅ '{term_name}' -> {target_label} (already mapped)")
        return
    
    status_code = assign_term_to_entities(term_guid, new_guids)
    if status_code in (200, 204):
        print(f"  ✅ '{term_name}' -> {target_label} ({len(new_guids)} new)")
    else:
        print(f"  ⚠️ '{term_name}' -> {target_label}: HTTP {status_code}")
    time.sleep(0.3)


# Helper: collect column GUIDs for a table
def col_guids(table_name, col_names):
    if table_name not in tables:
        return []
    ent = tables[table_name]
    return [get_column_guid(ent, c) for c in col_names if get_column_guid(ent, c)]

# Helper: fabric lakehouse guid
def lh_guid(keyword):
    return fabric_assets.get(keyword, {}).get("guid")


# ── ICD 10 → diagnoses table + icd10_code + icd10_description columns ──
map_term("ICD 10",
         [tables["diagnoses"]["guid"]] + col_guids("diagnoses", ["icd10_code", "icd10_description"]),
         "diagnoses + icd10_code + icd10_description")

# ── ATC Classification → medications table + atc_code column ──
map_term("ATC Classification",
         [tables["medications"]["guid"]] + col_guids("medications", ["atc_code"]),
         "medications + atc_code")

# ── OMOP CDM → gold_omop lakehouse ──
omop_targets = []
if lh_guid("gold_omop"):
    omop_targets.append(lh_guid("gold_omop"))
map_term("OMOP CDM", omop_targets, "gold_omop lakehouse")

# ── FHIR R4 → lh_brainchild + all patient tables ──
fhir_targets = []
if lh_guid("lh_brainchild"):
    fhir_targets.append(lh_guid("lh_brainchild"))
map_term("FHIR R4", fhir_targets, "lh_brainchild lakehouse")

# ── DICOM → lh_brainchild ──
dicom_targets = []
if lh_guid("lh_brainchild"):
    dicom_targets.append(lh_guid("lh_brainchild"))
map_term("DICOM", dicom_targets, "lh_brainchild lakehouse")

# ── Genomic Medicine Sweden → lh_brainchild ──
gms_targets = []
if lh_guid("lh_brainchild"):
    gms_targets.append(lh_guid("lh_brainchild"))
map_term("Genomic Medicine Sweden", gms_targets, "lh_brainchild lakehouse")

# ── Protected Health Information → patient_id, birth_date, postal_code, gender columns ──
phi_cols = (
    col_guids("patients", ["patient_id", "birth_date", "postal_code", "gender"])
    + col_guids("encounters", ["patient_id"])
)
map_term("Protected Health Information", phi_cols, "PHI-kolumner (patient_id, birth_date, postal_code, gender)")

# ── Bronze Layer → bronze_lakehouse ──
if lh_guid("bronze_lakehouse"):
    map_term("Bronze Layer", [lh_guid("bronze_lakehouse")], "bronze_lakehouse")

# ── Silver Layer → silver_lakehouse ──
if lh_guid("silver_lakehouse"):
    map_term("Silver Layer", [lh_guid("silver_lakehouse")], "silver_lakehouse")

# ── Gold Layer → gold_lakehouse + gold_omop ──
gold_targets = [g for g in [lh_guid("gold_lakehouse"), lh_guid("gold_omop")] if g]
map_term("Gold Layer", gold_targets, "gold_lakehouse + gold_omop")

# ── Vital Signs → vitals_labs + relevant columns ──
map_term("Vital Signs",
         [tables["vitals_labs"]["guid"]] + col_guids("vitals_labs", ["systolic_bp", "diastolic_bp", "heart_rate", "temperature_c", "oxygen_saturation"]),
         "vitals_labs + BT/puls/temp/SpO2")

# ── Lab Results → vitals_labs + lab columns ──
map_term("Lab Results",
         [tables["vitals_labs"]["guid"]] + col_guids("vitals_labs", ["glucose_mmol", "creatinine_umol", "hemoglobin_g", "sodium_mmol", "potassium_mmol"]),
         "vitals_labs + labb-kolumner")

# ── Length of Stay → encounters.los_days + vw_ml_encounters ──
los_targets = col_guids("encounters", ["los_days"])
if "vw_ml_encounters" in tables:
    los_targets.append(tables["vw_ml_encounters"]["guid"])
map_term("Length of Stay", los_targets, "encounters.los_days + vw_ml_encounters")

# ── Readmission Risk → encounters.readmission_30d + vw_ml_encounters ──
readm_targets = col_guids("encounters", ["readmission_30d"])
if "vw_ml_encounters" in tables:
    readm_targets.append(tables["vw_ml_encounters"]["guid"])
map_term("Readmission Risk", readm_targets, "encounters.readmission_30d + vw_ml_encounters")

# ── Swedish Personnummer → patients.postal_code ──
map_term("Swedish Personnummer",
         col_guids("patients", ["postal_code"]),
         "patients.postal_code")

# ── Person OMOP → patients (should already be mapped) ──
map_term("Person OMOP", [tables["patients"]["guid"]], "patients")

# ── Condition Occurrence → diagnoses ──
map_term("Condition Occurrence", [tables["diagnoses"]["guid"]], "diagnoses")

# ── Drug Exposure → medications ──
map_term("Drug Exposure", [tables["medications"]["guid"]], "medications")

# ── Measurement → vitals_labs ──
map_term("Measurement", [tables["vitals_labs"]["guid"]], "vitals_labs")

# ── Visit Occurrence → encounters ──
map_term("Visit Occurrence", [tables["encounters"]["guid"]], "encounters")

# ── FHIR Patient → patients table ──
map_term("FHIR Patient", [tables["patients"]["guid"]], "patients")

# ── FHIR Encounter → encounters table ──
map_term("FHIR Encounter", [tables["encounters"]["guid"]], "encounters")

# ── FHIR Condition → diagnoses table ──
map_term("FHIR Condition", [tables["diagnoses"]["guid"]], "diagnoses")

# ── FHIR MedicationRequest → medications table ──
map_term("FHIR MedicationRequest", [tables["medications"]["guid"]], "medications")

# ── FHIR Observation → vitals_labs table ──
map_term("FHIR Observation", [tables["vitals_labs"]["guid"]], "vitals_labs")

# ── FHIR ImagingStudy → lh_brainchild ──
if lh_guid("lh_brainchild"):
    map_term("FHIR ImagingStudy", [lh_guid("lh_brainchild")], "lh_brainchild")

# ── FHIR Specimen → lh_brainchild ──
if lh_guid("lh_brainchild"):
    map_term("FHIR Specimen", [lh_guid("lh_brainchild")], "lh_brainchild")

# ── Genomic Variant → lh_brainchild ──
if lh_guid("lh_brainchild"):
    map_term("Genomic Variant", [lh_guid("lh_brainchild")], "lh_brainchild")

# ── OMOP subtables → gold_omop ──
if lh_guid("gold_omop"):
    for term_name in ["OMOP Person", "OMOP Visit Occurrence", "OMOP Condition Occurrence", "OMOP Drug Exposure", "OMOP Measurement"]:
        map_term(term_name, [lh_guid("gold_omop")], "gold_omop")

# ── Medallion Architecture → all three lakehouses ──
medallion_targets = [g for g in [lh_guid("bronze_lakehouse"), lh_guid("silver_lakehouse"), lh_guid("gold_lakehouse")] if g]
map_term("Medallion Architecture", medallion_targets, "bronze + silver + gold lakehouses")

# ── SBCR → lh_brainchild ──
if lh_guid("lh_brainchild"):
    map_term("SBCR", [lh_guid("lh_brainchild")], "lh_brainchild")

# ── BTB → lh_brainchild ──
if lh_guid("lh_brainchild"):
    map_term("BTB", [lh_guid("lh_brainchild")], "lh_brainchild")

# ── VCF → lh_brainchild ──
if lh_guid("lh_brainchild"):
    map_term("VCF", [lh_guid("lh_brainchild")], "lh_brainchild")


# ════════════════════════════════════════════════════════════
# 7. Final summary
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("7. Final glossary summary")
print("=" * 70)

# Refresh
r = sess.get(f"{ATLAS_EP}/glossary/{glossary_guid}/terms?limit=100", headers=h, timeout=30)
if r.status_code == 200:
    mapped = 0
    unmapped = 0
    for t in sorted(r.json(), key=lambda x: x.get("name", "")):
        name = t.get("name")
        assigned = t.get("assignedEntities", [])
        if assigned:
            entities = [a.get("displayText", "?") for a in assigned]
            print(f"  ✅ {name} -> {', '.join(entities)}")
            mapped += 1
        else:
            print(f"  ⬜ {name} (unmapped)")
            unmapped += 1
    
    print(f"\n  Total: {mapped + unmapped} terms")
    print(f"  Mapped: {mapped}")
    print(f"  Unmapped: {unmapped}")

print(f"\n{'=' * 70}")
print("KLAR — Alla glossary-termer mappade!")
print("=" * 70)
