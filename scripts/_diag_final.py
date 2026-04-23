"""
Quick diagnostic: check why 5 column classifications failed + final status.
"""
import requests, sys, os, time
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
retries = Retry(total=5, backoff_factor=3, status_forcelist=[429, 500, 502, 503])
session.mount("https://", HTTPAdapter(max_retries=retries))

def api(method, url, **kw):
    kw.setdefault("timeout", 60)
    kw.setdefault("headers", h)
    for attempt in range(3):
        try:
            return getattr(session, method.lower())(url, **kw)
        except Exception as e:
            if attempt < 2:
                print(f"  Retry ({e})...")
                time.sleep(5 * (attempt + 1))
            else:
                raise

# ======================================================
# 1. Check the failed column entities
# ======================================================
print("=" * 60)
print("PART 1: Investigate failed column classifications")
print("=" * 60)

# Get table GUIDs from sql-databases collection
TABLES = ["encounters", "diagnoses", "vitals_labs", "medications"]
for tname in TABLES:
    time.sleep(1)
    body = {
        "keywords": tname,
        "filter": {"and": [{"typeName": "azure_sql_table"}, {"collectionId": "sql-databases"}]},
        "limit": 5,
    }
    r = api("POST", SEARCH, json=body)
    vals = r.json().get("value", [])
    if not vals:
        print(f"\n{tname}: NOT FOUND in sql-databases")
        continue
    
    for v in vals:
        tguid = v["id"]
        qn = v.get("qualifiedName", "?")
        print(f"\n{tname}: guid={tguid}")
        print(f"  QN: {qn}")
        
        time.sleep(1)
        r2 = api("GET", f"{ATLAS}/entity/guid/{tguid}")
        ent = r2.json().get("entity", {})
        status = ent.get("status", "?")
        col_ref = ent.get("collectionId", "?")
        print(f"  status={status}, collection={col_ref}")
        
        cols = ent.get("relationshipAttributes", {}).get("columns", [])
        # Find encounter_id or icd10_code
        for c in cols:
            cn = c.get("displayText", "?")
            cg = c.get("guid", "?")
            cstatus = c.get("entityStatus", "?")
            if cn in ("encounter_id", "icd10_code"):
                print(f"  Column '{cn}': guid={cg}, status={cstatus}")
                
                # Try to get the column entity
                time.sleep(1)
                r3 = api("GET", f"{ATLAS}/entity/guid/{cg}")
                if r3.status_code == 200:
                    col_ent = r3.json().get("entity", {})
                    col_status = col_ent.get("status", "?")
                    col_coll = col_ent.get("collectionId", "?")
                    col_cls = col_ent.get("classifications", [])
                    existing_cls = [cl.get("typeName", "?") for cl in col_cls]
                    print(f"    entity status={col_status}, collection={col_coll}")
                    print(f"    existing classifications: {existing_cls}")
                    
                    # Try applying classification
                    time.sleep(1)
                    cls_name = "ICD10 Diagnosis Code" if cn == "icd10_code" else "FHIR Resource ID"
                    url = f"{ATLAS}/entity/guid/{cg}/classifications"
                    payload = [{"typeName": cls_name}]
                    r4 = api("POST", url, json=payload)
                    print(f"    Apply '{cls_name}': {r4.status_code}")
                    if r4.status_code >= 400:
                        print(f"    Error: {r4.text[:300]}")
                    
                    # Try alternative: PUT single classification
                    if r4.status_code >= 400:
                        time.sleep(1)
                        url2 = f"{ATLAS}/entity/guid/{cg}/classification/{cls_name}"
                        payload2 = {"typeName": cls_name}
                        r5 = api("PUT", url2, json=payload2)
                        print(f"    PUT single: {r5.status_code}")
                        if r5.status_code >= 400:
                            print(f"    Error: {r5.text[:300]}")
                    
                    # Try bulk
                    if r4.status_code >= 400:
                        time.sleep(1)
                        url3 = f"{ATLAS}/entity/bulk/classification"
                        payload3 = {"classification": {"typeName": cls_name}, "entityGuids": [cg]}
                        r6 = api("POST", url3, json=payload3)
                        print(f"    Bulk: {r6.status_code}")
                        if r6.status_code >= 400:
                            print(f"    Error: {r6.text[:300]}")
                else:
                    print(f"    FETCH FAILED: {r3.status_code}")

# ======================================================
# 2. Final collection counts
# ======================================================
print()
print("=" * 60)
print("PART 2: Final collection counts")
print("=" * 60)
time.sleep(2)

for col_id in ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "barncancer", "fabric-brainchild"]:
    time.sleep(0.5)
    body = {"filter": {"collectionId": col_id}, "limit": 1}
    r = api("POST", SEARCH, json=body)
    d = r.json()
    count = d.get("@search.count", "?")
    label = "ROOT" if col_id == "prviewacc" else col_id
    print(f"  {label}: {count}")

# Check what types are still in root
print("\n  Root entity types:")
time.sleep(1)
body = {"filter": {"collectionId": "prviewacc"}, "limit": 50, "facets": [{"facet": "entityType", "count": 50}]}
r = api("POST", SEARCH, json=body)
d = r.json()
facets = d.get("@search.facets", {}).get("entityType", [])
for f in facets:
    print(f"    {f.get('value', '?')}: {f.get('count', '?')}")

# ======================================================
# 3. Classification counts
# ======================================================
print()
print("=" * 60)
print("PART 3: Classification search counts")
print("=" * 60)

for cn in ["Swedish Personnummer", "Patient Name PHI", "ICD10 Diagnosis Code",
           "SNOMED CT Code", "FHIR Resource ID", "OMOP Concept ID"]:
    time.sleep(0.5)
    body = {"filter": {"classification": cn}, "limit": 1}
    r = api("POST", SEARCH, json=body)
    cnt = r.json().get("@search.count", 0)
    print(f"  {cn}: {cnt}")

# ======================================================
# 4. Check existing classifications on patient_id
# ======================================================
print()
print("=" * 60)
print("PART 4: Verify patient_id column classifications")
print("=" * 60)

# Get patients table from sql-databases
time.sleep(1)
body = {
    "keywords": "patients",
    "filter": {"and": [{"typeName": "azure_sql_table"}, {"collectionId": "sql-databases"}]},
    "limit": 1,
}
r = api("POST", SEARCH, json=body)
vals = r.json().get("value", [])
if vals:
    tguid = vals[0]["id"]
    time.sleep(1)
    r2 = api("GET", f"{ATLAS}/entity/guid/{tguid}")
    ent = r2.json().get("entity", {})
    cols = ent.get("relationshipAttributes", {}).get("columns", [])
    for c in cols:
        cn = c.get("displayText", "?")
        cg = c.get("guid", "?")
        if cn == "patient_id":
            time.sleep(0.5)
            r3 = api("GET", f"{ATLAS}/entity/guid/{cg}")
            col_ent = r3.json().get("entity", {})
            cls_list = col_ent.get("classifications", [])
            print(f"  patients.patient_id classifications:")
            for cl in cls_list:
                print(f"    - {cl.get('typeName', '?')}")

print("\nDone!")
