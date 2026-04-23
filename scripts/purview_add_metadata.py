"""Add metadata to Purview assets:
- Classifications on SQL columns
- Glossary terms linked to tables
- Descriptions on key assets
"""
import requests, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
CATALOG_EP = "https://prviewacc.purview.azure.com"
ATLAS_EP = f"{CATALOG_EP}/catalog/api/atlas/v2"
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Retry-capable session
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
session.mount("https://", HTTPAdapter(max_retries=retries))

def search(keywords="*", obj_type=None, filter_qn=None, limit=50):
    url = f"{CATALOG_EP}/catalog/api/search/query?api-version=2022-08-01-preview"
    filt = {"and": []}
    if obj_type:
        filt["and"].append({"objectType": obj_type})
    if filter_qn:
        filt["and"].append({"attributeName": "qualifiedName", "operator": "contains", "attributeValue": filter_qn})
    body = {"keywords": keywords, "filter": filt if filt["and"] else None, "limit": limit}
    r = session.post(url, headers=h, json=body, timeout=30)
    return r.json().get("value", []) if r.status_code == 200 else []

def get_entity(guid):
    url = f"{ATLAS_EP}/entity/guid/{guid}"
    r = session.get(url, headers=h, timeout=30)
    return r.json() if r.status_code == 200 else None

def add_classification(guid, classification_name):
    url = f"{ATLAS_EP}/entity/guid/{guid}/classifications"
    body = [{"typeName": classification_name}]
    r = session.post(url, headers=h, json=body, timeout=30)
    if r.status_code in (200, 204):
        return True
    elif r.status_code == 409:  # Already classified
        return True
    return False

def update_entity_description(guid, description):
    url = f"{ATLAS_EP}/entity/guid/{guid}"
    body = {"entity": {"guid": guid, "attributes": {"userDescription": description}}}
    r = session.put(url, headers=h, json=body, timeout=30)
    return r.status_code in (200, 204)

def assign_glossary_term(term_guid, entity_guid):
    url = f"{ATLAS_EP}/glossary/terms/{term_guid}/assignedEntities"
    body = [{"guid": entity_guid}]
    r = session.post(url, headers=h, json=body, timeout=30)
    if r.status_code in (200, 204):
        return True
    elif r.status_code == 409:
        return True
    return False

# ── 1. Find SQL tables and their GUIDs ──
print("=" * 60)
print("1. Finding SQL table assets")
print("=" * 60)

sql_tables = {}
tables = search(obj_type="Tables", filter_qn="sql-hca-demo")
for t in tables:
    name = t.get("name", "")
    guid = t.get("id", "")
    if name in ["patients", "encounters", "diagnoses", "vitals_labs", "medications", "vw_ml_encounters"]:
        sql_tables[name] = guid
        print(f"  ✅ {name} -> {guid[:12]}...")

time.sleep(1)

# ── 2. Find SQL columns ──
print(f"\n{'=' * 60}")
print("2. Finding SQL columns")
print("=" * 60)

columns = search(obj_type="Columns", filter_qn="sql-hca-demo", limit=200)
col_map = {}  # col_map[table_name][col_name] = guid
for c in columns:
    qn = c.get("qualifiedName", "")
    name = c.get("name", "")
    guid = c.get("id", "")
    # QN format: mssql://server/db/schema/table#column
    if "#" in qn:
        tbl_part = qn.split("#")[0].split("/")[-1]
        if tbl_part not in col_map:
            col_map[tbl_part] = {}
        col_map[tbl_part][name] = guid

for tbl in sorted(col_map.keys()):
    cols = sorted(col_map[tbl].keys())
    print(f"  {tbl}: {', '.join(cols)}")

time.sleep(1)

# ── 3. Classify columns ──
print(f"\n{'=' * 60}")
print("3. Adding classifications to columns")
print("=" * 60)

# Mapping: (table, column) -> classification name
COLUMN_CLASSIFICATIONS = {
    # Swedish Personnummer
    ("patients", "personnummer"): "Swedish Personnummer",
    ("patients", "personal_id"): "Swedish Personnummer",
    ("patients", "ssn"): "Swedish Personnummer",
    
    # Patient Name PHI
    ("patients", "first_name"): "Patient Name PHI",
    ("patients", "last_name"): "Patient Name PHI",
    ("patients", "name"): "Patient Name PHI",
    ("patients", "patient_name"): "Patient Name PHI",
    
    # ICD10 Diagnosis Code
    ("diagnoses", "icd10_code"): "ICD10 Diagnosis Code",
    ("diagnoses", "diagnosis_code"): "ICD10 Diagnosis Code",
    ("diagnoses", "primary_diagnosis"): "ICD10 Diagnosis Code",
    ("diagnoses", "secondary_diagnosis"): "ICD10 Diagnosis Code",
    
    # SNOMED CT Code
    ("diagnoses", "snomed_code"): "SNOMED CT Code",
    ("encounters", "snomed_code"): "SNOMED CT Code",
    
    # FHIR Resource ID
    ("patients", "fhir_id"): "FHIR Resource ID",
    ("encounters", "fhir_id"): "FHIR Resource ID",
    
    # OMOP Concept ID
    ("patients", "concept_id"): "OMOP Concept ID",
    ("encounters", "concept_id"): "OMOP Concept ID",
    ("diagnoses", "concept_id"): "OMOP Concept ID",
    ("medications", "concept_id"): "OMOP Concept ID",
}

classified = 0
for (tbl, col), classification in COLUMN_CLASSIFICATIONS.items():
    if tbl in col_map and col in col_map[tbl]:
        guid = col_map[tbl][col]
        ok = add_classification(guid, classification)
        status = "✅" if ok else "⚠️"
        print(f"  {status} {tbl}.{col} -> {classification}")
        classified += 1
        time.sleep(0.3)

if classified == 0:
    # Try broader matching - list actual columns to find matches
    print("  No exact matches found. Actual columns per table:")
    # Apply classifications based on actual column names
    for tbl, cols in col_map.items():
        for col_name, guid in cols.items():
            classification = None
            cl = col_name.lower()
            
            if "personnummer" in cl or "personal_id" in cl or cl == "ssn":
                classification = "Swedish Personnummer"
            elif cl in ("first_name", "last_name", "patient_name", "name", "given_name", "family_name"):
                classification = "Patient Name PHI"
            elif "icd" in cl or "diagnosis_code" in cl or "primary_diagnosis" in cl:
                classification = "ICD10 Diagnosis Code"
            elif "snomed" in cl:
                classification = "SNOMED CT Code"
            elif "fhir" in cl:
                classification = "FHIR Resource ID"
            elif "concept_id" in cl:
                classification = "OMOP Concept ID"
            
            if classification:
                ok = add_classification(guid, classification)
                status = "✅" if ok else "⚠️"
                print(f"  {status} {tbl}.{col_name} -> {classification}")
                classified += 1
                time.sleep(0.3)

print(f"\n  Total classified: {classified}")

time.sleep(1)

# ── 4. Add descriptions to tables ──
print(f"\n{'=' * 60}")
print("4. Adding descriptions to SQL tables")
print("=" * 60)

TABLE_DESCRIPTIONS = {
    "patients": "Patient demographics including ID, gender, birth date, and geographic info. Source: Azure SQL HealthcareAnalyticsDB. 10,000 synthetic Swedish patients.",
    "encounters": "Healthcare encounters/visits with admission/discharge dates, encounter types, and department info. ~17,000 records linked to patients.",
    "diagnoses": "ICD-10 diagnosis codes per encounter. Primary and secondary diagnoses with descriptions. ~30,000 records.",
    "vitals_labs": "Vital signs and lab measurements per encounter. Includes heart rate, blood pressure, temperature, lab values. ~48,000 records.",
    "medications": "Medication prescriptions per encounter. Drug names, ATC codes, dosage, route, frequency. ~60,000 records.",
    "vw_ml_encounters": "Materialized view joining encounters with patient demographics, aggregated diagnoses, vitals, and medications. Used for ML training (LOS prediction, readmission risk).",
}

for tbl, desc in TABLE_DESCRIPTIONS.items():
    if tbl in sql_tables:
        ok = update_entity_description(sql_tables[tbl], desc)
        status = "✅" if ok else "⚠️"
        print(f"  {status} {tbl}: {desc[:60]}...")
        time.sleep(0.3)

time.sleep(1)

# ── 5. Link glossary terms to tables ──
print(f"\n{'=' * 60}")
print("5. Linking glossary terms to tables")
print("=" * 60)

# First get glossary terms
glossary_url = f"{ATLAS_EP}/glossary"
r = session.get(glossary_url, headers=h, timeout=30)
glossary_guid = None
if r.status_code == 200:
    glossaries = r.json()
    for g in glossaries:
        if g.get("name") == "Kund":
            glossary_guid = g["guid"]
            break

if glossary_guid:
    # Get all terms
    terms_url = f"{ATLAS_EP}/glossary/{glossary_guid}/terms?limit=50"
    r = session.get(terms_url, headers=h, timeout=30)
    if r.status_code == 200:
        terms = r.json()
        term_map = {t["name"]: t["guid"] for t in terms}
        print(f"  Found {len(term_map)} glossary terms")
        
        # Link terms to tables
        TERM_TABLE_MAP = {
            "Person OMOP": ["patients"],
            "Condition Occurrence": ["diagnoses"],
            "Drug Exposure": ["medications"],
            "Measurement": ["vitals_labs"],
            "Visit Occurrence": ["encounters"],
        }
        
        for term_name, table_names in TERM_TABLE_MAP.items():
            if term_name in term_map:
                for tbl in table_names:
                    if tbl in sql_tables:
                        ok = assign_glossary_term(term_map[term_name], sql_tables[tbl])
                        status = "✅" if ok else "⚠️"
                        print(f"  {status} '{term_name}' -> {tbl}")
                        time.sleep(0.3)

# ── 6. Find and annotate Fabric lakehouse assets ──
print(f"\n{'=' * 60}")
print("6. Finding Fabric lakehouse assets")
print("=" * 60)

for kw in ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse", "gold_omop", "lh_brainchild"]:
    results = search(keywords=kw, limit=5)
    time.sleep(0.5)
    for asset in results:
        name = asset.get("name", "")
        etype = asset.get("entityType", "")
        guid = asset.get("id", "")
        if kw.replace("_", "") in name.replace("_", "").lower() or name.lower() == kw:
            print(f"  Found: {name} [{etype}] ({guid[:12]}...)")
            
            # Add description based on lakehouse
            desc_map = {
                "bronze_lakehouse": "Bronze layer lakehouse - raw data ingested from Azure SQL. Healthcare-Analytics project.",
                "silver_lakehouse": "Silver layer lakehouse - cleaned and enriched features for ML. Healthcare-Analytics project.",
                "gold_lakehouse": "Gold layer lakehouse - ML model outputs (LOS prediction, readmission risk). Healthcare-Analytics project.",
                "gold_omop": "Gold OMOP lakehouse - OMOP CDM v5.4 standardized clinical data. Healthcare-Analytics project.",
                "lh_brainchild": "BrainChild lakehouse - FHIR R4 resources, GMS genomics, OMOP CDM, DICOM metadata, SBCR registry data.",
            }
            if kw in desc_map:
                ok = update_entity_description(guid, desc_map[kw])
                status = "✅" if ok else "⚠️"
                print(f"    {status} Description added")
            
            # Link glossary terms
            if glossary_guid and term_map:
                fabric_term_map = {
                    "bronze_lakehouse": "Bronze Layer",
                    "silver_lakehouse": "Silver Layer",
                    "gold_lakehouse": "Gold Layer",
                    "gold_omop": "OMOP CDM",
                    "lh_brainchild": "FHIR R4",
                }
                term_name = fabric_term_map.get(kw)
                if term_name and term_name in term_map:
                    ok = assign_glossary_term(term_map[term_name], guid)
                    status = "✅" if ok else "⚠️"
                    print(f"    {status} Glossary term '{term_name}' linked")
            time.sleep(0.3)

print(f"\n{'=' * 60}")
print("DONE — Metadata added to Purview assets")
print("=" * 60)
