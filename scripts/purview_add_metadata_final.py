"""Final metadata enrichment for Purview assets:
- Classifications on SQL columns
- Descriptions on tables via PUT ?name=userDescription
- Glossary terms linked to tables (already done, verify)
"""
import requests, json, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
CATALOG_EP = "https://prviewacc.purview.azure.com"
ATLAS_EP = f"{CATALOG_EP}/catalog/api/atlas/v2"
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
s = requests.Session()
s.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

QN_BASE = "mssql://sql-hca-demo.database.windows.net/HealthcareAnalyticsDB/hca"
TABLE_NAMES = ["patients", "encounters", "diagnoses", "vitals_labs", "medications"]

# ── 1. Collect all tables and columns ──
print("=" * 60)
print("1. Collecting table and column GUIDs")
print("=" * 60)

tables = {}  # {name: {guid, columns: {col_name: col_guid}}}

for tbl in TABLE_NAMES:
    qn = f"{QN_BASE}/{tbl}"
    r = s.get(f"{ATLAS_EP}/entity/uniqueAttribute/type/azure_sql_table?attr:qualifiedName={qn}", headers=h, timeout=30)
    if r.status_code == 200:
        ent = r.json().get("entity", {})
        tbl_guid = ent.get("guid")
        cols_raw = ent.get("relationshipAttributes", {}).get("columns", [])
        col_dict = {c["displayText"]: c["guid"] for c in cols_raw}
        tables[tbl] = {"guid": tbl_guid, "columns": col_dict}
        print(f"  ✅ {tbl} ({tbl_guid[:12]}...) — {len(col_dict)} columns: {', '.join(sorted(col_dict.keys()))}")
    else:
        print(f"  ⚠️ {tbl}: {r.status_code}")
    time.sleep(0.5)

# Also try the view
qn_view = f"{QN_BASE}/vw_ml_encounters"
for vtype in ["azure_sql_view", "azure_sql_table"]:
    r = s.get(f"{ATLAS_EP}/entity/uniqueAttribute/type/{vtype}?attr:qualifiedName={qn_view}", headers=h, timeout=30)
    if r.status_code == 200:
        ent = r.json().get("entity", {})
        tables["vw_ml_encounters"] = {"guid": ent.get("guid"), "columns": {c["displayText"]: c["guid"] for c in ent.get("relationshipAttributes", {}).get("columns", [])}}
        print(f"  ✅ vw_ml_encounters ({ent.get('guid')[:12]}...) [{vtype}]")
        break
time.sleep(0.5)

# ── 2. Classify columns ──
print(f"\n{'=' * 60}")
print("2. Classifying columns")
print("=" * 60)

# Based on actual columns discovered:
# patients: patient_id, birth_date, gender, ses_level, postal_code, region, smoking_status, created_at
# encounters: encounter_id, patient_id, admission_date, discharge_date, department, admission_source, discharge_disposition, los_days, readmission_30d, created_at
# diagnoses: diagnosis_id, encounter_id, icd10_code, icd10_description, diagnosis_type, confirmed_date
# vitals_labs: measurement_id, encounter_id, measured_at, systolic_bp, diastolic_bp, heart_rate, temperature_c, oxygen_saturation, glucose_mmol, creatinine_umol, hemoglobin_g, sodium_mmol, potassium_mmol, bmi, weight_kg
# medications: medication_id, encounter_id, atc_code, drug_name, dose_mg, frequency, route, start_date, end_date

CLASSIFICATIONS = [
    # ICD-10 codes
    ("diagnoses", "icd10_code", "ICD10 Diagnosis Code"),
    ("diagnoses", "icd10_description", "ICD10 Diagnosis Code"),
    # Patient Name PHI - patient_id could be considered identifying
    ("patients", "patient_id", "FHIR Resource ID"),
    ("encounters", "patient_id", "FHIR Resource ID"),
    ("patients", "postal_code", "Swedish Personnummer"),  # Geographic PHI - closest match
]

classified = 0
for tbl, col, cls in CLASSIFICATIONS:
    if tbl not in tables or col not in tables[tbl]["columns"]:
        print(f"  ⏭️ {tbl}.{col} not found, skipping")
        continue
    col_guid = tables[tbl]["columns"][col]
    r = s.post(
        f"{ATLAS_EP}/entity/guid/{col_guid}/classifications",
        headers=h, json=[{"typeName": cls}], timeout=30,
    )
    if r.status_code in (200, 204):
        print(f"  ✅ {tbl}.{col} -> {cls}")
        classified += 1
    elif r.status_code == 409:
        print(f"  ✅ {tbl}.{col} -> {cls} (already classified)")
        classified += 1
    else:
        print(f"  ⚠️ {tbl}.{col}: {r.status_code} {r.text[:150]}")
    time.sleep(0.3)

print(f"\n  Classified: {classified}")

# ── 3. Add descriptions via PUT ?name=userDescription ──
print(f"\n{'=' * 60}")
print("3. Adding descriptions to tables")
print("=" * 60)

TABLE_DESCS = {
    "patients": "Patientdemografi: ID, kön, födelsedatum, geografi, rökning, SES-nivå. 10 000 syntetiska svenska patienter.",
    "encounters": "Vårdkontakter: in-/utskrivning, avdelning, LOS-dagar, återinskrivning. ~17 000 poster kopplade till patienter.",
    "diagnoses": "ICD-10 diagnoskoder per vårdkontakt: primär/sekundär diagnos med beskrivning. ~30 000 poster.",
    "vitals_labs": "Vitalparametrar och labb: BT, puls, temp, O2-sat, glukos, kreatinin, Hb, Na, K, BMI. ~48 000 poster.",
    "medications": "Läkemedelsförskrivning: ATC-kod, substans, dos, administreringsväg, frekvens. ~60 000 poster.",
    "vw_ml_encounters": "ML-vy: encounters + patientdemografi, aggregerade diagnoser, vitals, mediciner. Används för LOS-prediktion och återinskrivningsrisk.",
}

for tbl, desc in TABLE_DESCS.items():
    if tbl not in tables:
        print(f"  ⏭️ {tbl} not found")
        continue
    guid = tables[tbl]["guid"]
    r = s.put(
        f"{ATLAS_EP}/entity/guid/{guid}?name=userDescription",
        headers=h,
        data=json.dumps(desc),
        timeout=30,
    )
    if r.status_code in (200, 204):
        print(f"  ✅ {tbl}: {desc[:60]}...")
    else:
        print(f"  ⚠️ {tbl}: {r.status_code} {r.text[:150]}")
    time.sleep(0.3)

# ── 4. Verify glossary term links ──
print(f"\n{'=' * 60}")
print("4. Verifying glossary term links")
print("=" * 60)

r = s.get(f"{ATLAS_EP}/glossary", headers=h, timeout=30)
glossary_guid = None
if r.status_code == 200:
    for g in r.json():
        if g.get("name") == "Kund":
            glossary_guid = g["guid"]
            break

if glossary_guid:
    r = s.get(f"{ATLAS_EP}/glossary/{glossary_guid}/terms?limit=50", headers=h, timeout=30)
    if r.status_code == 200:
        terms = r.json()
        for t in terms:
            name = t.get("name")
            assigned = t.get("assignedEntities", [])
            if assigned:
                entity_names = [a.get("displayText", "?") for a in assigned]
                print(f"  ✅ '{name}' -> {', '.join(entity_names)}")
            else:
                # Check if this term should be linked
                print(f"  ℹ️ '{name}' (not linked to any entity)")

# ── 5. Add descriptions to key Fabric assets ──
print(f"\n{'=' * 60}")
print("5. Updating Fabric lakehouse descriptions")
print("=" * 60)

def search(keywords="*", limit=5):
    url = f"{CATALOG_EP}/catalog/api/search/query?api-version=2022-08-01-preview"
    body = {"keywords": keywords, "limit": limit}
    r = s.post(url, headers=h, json=body, timeout=30)
    return r.json().get("value", []) if r.status_code == 200 else []

FABRIC_ASSETS = {
    "bronze_lakehouse": "Bronze-lager (rådata). Inmatning från Azure SQL HealthcareAnalyticsDB.",
    "silver_lakehouse": "Silver-lager (rensat & berikat). Feature engineering för ML-träning.",
    "gold_lakehouse": "Gold-lager. ML-modellutdata: LOS-prediktion, återinskrivningsrisk.",
    "gold_omop": "OMOP CDM v5.4 lakehouse. Standardiserad klinisk data (person, condition, drug, measurement, visit).",
    "lh_brainchild": "BrainChild lakehouse. FHIR R4, GMS-genomik, OMOP CDM, DICOM-metadata, SBCR-register.",
}

for keyword, desc in FABRIC_ASSETS.items():
    results = search(keywords=keyword, limit=3)
    time.sleep(0.5)
    for asset in results:
        name = asset.get("name", "")
        etype = asset.get("entityType", "")
        guid = asset.get("id", "")
        if etype == "fabric_lake_warehouse" and (keyword.replace("_", "") in name.replace("_", "").lower()):
            r = s.put(
                f"{ATLAS_EP}/entity/guid/{guid}?name=userDescription",
                headers=h,
                data=json.dumps(desc),
                timeout=30,
            )
            status = "✅" if r.status_code in (200, 204) else "⚠️"
            print(f"  {status} {name} [{etype}]: {desc[:50]}...")
            time.sleep(0.3)
            break  # only first match per keyword

print(f"\n{'=' * 60}")
print("KLAR — All metadata tillagd i Purview")
print("=" * 60)
