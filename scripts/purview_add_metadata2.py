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

def search(keywords="*", obj_type=None, filter_qn=None, limit=50):
    url = f"{CATALOG_EP}/catalog/api/search/query?api-version=2022-08-01-preview"
    filt = {"and": []}
    if obj_type:
        filt["and"].append({"objectType": obj_type})
    if filter_qn:
        filt["and"].append({"attributeName": "qualifiedName", "operator": "contains", "attributeValue": filter_qn})
    body = {"keywords": keywords, "filter": filt if filt["and"] else None, "limit": limit}
    r = s.post(url, headers=h, json=body, timeout=30)
    return r.json().get("value", []) if r.status_code == 200 else []

# ── 1. Get table entities with full detail ──
print("1. Finding SQL tables and their Atlas GUIDs")
print("=" * 60)

tables = search(obj_type="Tables", filter_qn="sql-hca-demo")
hca_tables = {}
for t in tables:
    name = t.get("name", "")
    if name not in ["patients", "encounters", "diagnoses", "vitals_labs", "medications", "vw_ml_encounters"]:
        continue
    qn = t.get("qualifiedName", "")
    search_id = t.get("id", "")
    # The id from search IS the Atlas guid in newer API versions
    # Let's check assetType and try to get entity via qualifiedName
    print(f"\n  Table: {name}")
    print(f"  QN: {qn}")
    print(f"  SearchID: {search_id}")
    
    # Try get by type+qualifiedName
    r = s.get(
        f"{ATLAS_EP}/entity/uniqueAttribute/type/azure_sql_table?attr:qualifiedName={qn}",
        headers=h, timeout=30
    )
    if r.status_code == 200:
        ent = r.json().get("entity", {})
        atlas_guid = ent.get("guid", "?")
        print(f"  AtlasGUID: {atlas_guid}")
        print(f"  Attrs: {list(ent.get('attributes', {}).keys())[:15]}")
        
        # Get columns
        ra = ent.get("relationshipAttributes", {})
        cols = ra.get("columns", [])
        print(f"  Columns ({len(cols)}):")
        col_list = []
        for c in cols:
            col_name = c.get("displayText", "?")
            col_guid = c.get("guid", "?")
            col_list.append((col_name, col_guid))
            print(f"    {col_name} ({col_guid[:12]}...)")
        
        hca_tables[name] = {"guid": atlas_guid, "qn": qn, "columns": col_list}
    else:
        print(f"  GET failed: {r.status_code} {r.text[:200]}")
    time.sleep(0.5)

# ── 2. Add classifications to columns ──
print(f"\n\n2. Adding classifications to columns")
print("=" * 60)

classified = 0
for tbl_name, tbl_info in hca_tables.items():
    for col_name, col_guid in tbl_info["columns"]:
        classification = None
        cl = col_name.lower()
        
        if "personnummer" in cl or "personal_id" in cl or cl == "ssn":
            classification = "Swedish Personnummer"
        elif cl in ("first_name", "last_name", "patient_name", "given_name", "family_name"):
            classification = "Patient Name PHI"
        elif "icd" in cl or cl in ("diagnosis_code", "primary_diagnosis", "secondary_diagnosis"):
            classification = "ICD10 Diagnosis Code"
        elif "snomed" in cl:
            classification = "SNOMED CT Code"
        elif "fhir" in cl and "id" in cl:
            classification = "FHIR Resource ID"
        elif cl == "concept_id" or cl.endswith("_concept_id"):
            classification = "OMOP Concept ID"
        
        if classification:
            url = f"{ATLAS_EP}/entity/guid/{col_guid}/classifications"
            body = [{"typeName": classification}]
            r = s.post(url, headers=h, json=body, timeout=30)
            if r.status_code in (200, 204):
                print(f"  ✅ {tbl_name}.{col_name} -> {classification}")
                classified += 1
            elif r.status_code == 409:
                print(f"  ✅ {tbl_name}.{col_name} -> {classification} (already)")
                classified += 1
            else:
                print(f"  ⚠️ {tbl_name}.{col_name}: {r.status_code} {r.text[:150]}")
            time.sleep(0.3)

print(f"\n  Total classified: {classified}")

# ── 3. Add descriptions to tables ──
print(f"\n\n3. Adding descriptions")
print("=" * 60)

TABLE_DESCRIPTIONS = {
    "patients": "Patient demographics including ID, gender, birth date, and geographic info. 10,000 synthetic Swedish patients.",
    "encounters": "Healthcare encounters/visits with admission/discharge dates, encounter types. ~17,000 records.",
    "diagnoses": "ICD-10 diagnosis codes per encounter. Primary and secondary diagnoses. ~30,000 records.",
    "vitals_labs": "Vital signs and lab measurements per encounter. Heart rate, BP, temperature, labs. ~48,000 records.",
    "medications": "Medication prescriptions per encounter. Drug names, ATC codes, dosage. ~60,000 records.",
    "vw_ml_encounters": "View joining encounters with demographics, diagnoses, vitals, medications. ML training data.",
}

for tbl_name, desc in TABLE_DESCRIPTIONS.items():
    if tbl_name in hca_tables:
        guid = hca_tables[tbl_name]["guid"]
        r = s.put(
            f"{ATLAS_EP}/entity/guid/{guid}",
            headers=h,
            json={"entity": {"guid": guid, "typeName": "azure_sql_table", "attributes": {"userDescription": desc}}},
            timeout=30,
        )
        if r.status_code in (200, 204):
            print(f"  ✅ {tbl_name}: {desc[:50]}...")
        else:
            print(f"  ⚠️ {tbl_name}: {r.status_code} {r.text[:150]}")
        time.sleep(0.3)

# ── 4. Glossary terms to tables ──
print(f"\n\n4. Linking glossary terms")
print("=" * 60)

# Get glossary terms
r = s.get(f"{ATLAS_EP}/glossary", headers=h, timeout=30)
glossary_guid = None
term_map = {}
if r.status_code == 200:
    for g in r.json():
        if g.get("name") == "Kund":
            glossary_guid = g["guid"]
            break
if glossary_guid:
    r = s.get(f"{ATLAS_EP}/glossary/{glossary_guid}/terms?limit=50", headers=h, timeout=30)
    if r.status_code == 200:
        for t in r.json():
            term_map[t["name"]] = t["guid"]

TERM_TABLE = {
    "Person OMOP": ["patients"],
    "Condition Occurrence": ["diagnoses"],
    "Drug Exposure": ["medications"],
    "Measurement": ["vitals_labs"],
    "Visit Occurrence": ["encounters"],
}

for term_name, table_names in TERM_TABLE.items():
    if term_name not in term_map:
        continue
    for tbl in table_names:
        if tbl not in hca_tables:
            continue
        tbl_guid = hca_tables[tbl]["guid"]
        r = s.post(
            f"{ATLAS_EP}/glossary/terms/{term_map[term_name]}/assignedEntities",
            headers=h,
            json=[{"guid": tbl_guid}],
            timeout=30,
        )
        if r.status_code in (200, 204):
            print(f"  ✅ '{term_name}' -> {tbl}")
        elif r.status_code == 409:
            print(f"  ✅ '{term_name}' -> {tbl} (already)")
        else:
            print(f"  ⚠️ '{term_name}' -> {tbl}: {r.status_code} {r.text[:150]}")
        time.sleep(0.3)

print(f"\n{'=' * 60}")
print("DONE")
