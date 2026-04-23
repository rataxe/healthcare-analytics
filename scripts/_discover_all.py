"""Quick discovery: columns, classifications, Fabric items, sensitivity labels."""
import requests, json, time, sys, os
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
QN_BASE = "mssql://sql-hca-demo.database.windows.net/HealthcareAnalyticsDB/hca"

print("=" * 70)
print("1. CLASSIFICATION RULES")
print("=" * 70)
r = requests.get(f"{SCAN}/classificationrules?api-version=2022-07-01-preview", headers=h, timeout=15)
if r.status_code == 200:
    rules = [ru["name"] for ru in r.json().get("value", [])]
    print(f"  Rules ({len(rules)}): {rules}")

print("\n" + "=" * 70)
print("2. SQL TABLE COLUMNS + EXISTING CLASSIFICATIONS")
print("=" * 70)
sql_tables = {}
for tbl in ["patients", "encounters", "diagnoses", "vitals_labs", "medications"]:
    qn = f"{QN_BASE}/{tbl}"
    r = requests.get(f"{ATLAS}/entity/uniqueAttribute/type/azure_sql_table?attr:qualifiedName={qn}", headers=h, timeout=15)
    if r.status_code == 200:
        ent = r.json().get("entity", {})
        tbl_guid = ent.get("guid", "")
        cols_raw = ent.get("relationshipAttributes", {}).get("columns", [])
        col_dict = {}
        for c in cols_raw:
            col_dict[c["displayText"]] = c["guid"]
        sql_tables[tbl] = {"guid": tbl_guid, "columns": col_dict, "qn": qn}
        print(f"\n  {tbl} (guid={tbl_guid[:12]}...)")
        print(f"    Columns: {list(col_dict.keys())}")
        # Check classifications on first 3 cols
        for cname, cguid in list(col_dict.items())[:3]:
            r2 = requests.get(f"{DATAMAP}/entity/guid/{cguid}", headers=h, timeout=10)
            if r2.status_code == 200:
                cls = r2.json().get("entity", {}).get("classifications", [])
                if cls:
                    cls_names = [cl.get("typeName", "") for cl in cls]
                    print(f"    {cname}: classifications={cls_names}")
    time.sleep(0.2)

# View
print("\n  --- vw_ml_encounters ---")
for vtype in ["azure_sql_view", "azure_sql_table"]:
    qn = f"{QN_BASE}/vw_ml_encounters"
    r = requests.get(f"{ATLAS}/entity/uniqueAttribute/type/{vtype}?attr:qualifiedName={qn}", headers=h, timeout=15)
    if r.status_code == 200:
        ent = r.json().get("entity", {})
        cols_raw = ent.get("relationshipAttributes", {}).get("columns", [])
        col_dict = {c["displayText"]: c["guid"] for c in cols_raw}
        sql_tables["vw_ml_encounters"] = {"guid": ent.get("guid", ""), "columns": col_dict, "qn": qn}
        print(f"  vw_ml_encounters [{vtype}] (guid={ent.get('guid','')[:12]}...)")
        print(f"    Columns: {list(col_dict.keys())}")
        break

print("\n" + "=" * 70)
print("3. FABRIC LAKEHOUSE TABLES + COLUMNS")
print("=" * 70)
fabric_tables = {}
body = {"keywords": "*", "limit": 50, "filter": {"entityType": "fabric_lakehouse_table"}}
r = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r.status_code == 200:
    for ent in r.json().get("value", []):
        name = ent.get("name", "")
        guid = ent.get("id", "")
        qn = ent.get("qualifiedName", "")
        r2 = requests.get(f"{DATAMAP}/entity/guid/{guid}", headers=h, timeout=10)
        if r2.status_code == 200:
            entity = r2.json().get("entity", {})
            rel = entity.get("relationshipAttributes", {})
            cols = rel.get("columns", []) or rel.get("schema", [])
            col_dict = {}
            for c in cols:
                if isinstance(c, dict):
                    col_dict[c.get("displayText", "?")] = c.get("guid", "")
            fabric_tables[name] = {"guid": guid, "columns": col_dict, "qn": qn}
            print(f"\n  {name} (guid={guid[:12]}...)")
            print(f"    Columns ({len(col_dict)}): {list(col_dict.keys())[:15]}")
            # Check classifications
            cls_on_entity = entity.get("classifications", [])
            if cls_on_entity:
                print(f"    Table-level classifications: {[c.get('typeName','') for c in cls_on_entity]}")
        time.sleep(0.2)

print("\n" + "=" * 70)
print("4. FABRIC LAKEHOUSES (containers)")
print("=" * 70)
body = {"keywords": "*", "limit": 20, "filter": {"entityType": "fabric_lake_warehouse"}}
r = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r.status_code == 200:
    for ent in r.json().get("value", []):
        name = ent.get("name", "")
        guid = ent.get("id", "")
        print(f"  {name} (guid={guid[:12]}...) coll={ent.get('collectionId','?')}")

print("\n" + "=" * 70)
print("5. CUSTOM CLASSIFICATION TYPES")
print("=" * 70)
r = requests.get(f"{ATLAS}/types/typedefs", headers=h, timeout=30)
if r.status_code == 200:
    data = r.json()
    cls_defs = data.get("classificationDefs", [])
    custom = [c["name"] for c in cls_defs if not c["name"].startswith("MICROSOFT.")]
    print(f"  Custom ({len(custom)}): {custom}")
    # Also check for sensitivity-related
    sens = [c["name"] for c in cls_defs if "sensit" in c["name"].lower() or "label" in c["name"].lower()]
    print(f"  Sensitivity-related: {sens}")

print("\n" + "=" * 70)
print("6. EXISTING LINEAGE (Process entities)")
print("=" * 70)
body = {"keywords": "*", "limit": 50, "filter": {"entityType": "Process"}}
r = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r.status_code == 200:
    procs = r.json().get("value", [])
    print(f"  Process entities: {len(procs)}")
    for p in procs:
        print(f"    {p.get('name','?')} (qn={p.get('qualifiedName','?')[:60]})")

# Also check for hca_lineage_process
body2 = {"keywords": "hca_lineage", "limit": 50}
r2 = requests.post(SEARCH, headers=h, json=body2, timeout=15)
if r2.status_code == 200:
    hca_procs = [v for v in r2.json().get("value", []) if v.get("entityType") == "Process"]
    if hca_procs:
        print(f"  HCA lineage processes: {len(hca_procs)}")
        for p in hca_procs:
            print(f"    {p.get('name','?')}")

print("\n" + "=" * 70)
print("7. SENSITIVITY LABELS via Information Protection API")
print("=" * 70)
# Try MIP label endpoint
for ep in [
    f"{ACCT}/datamap/api/sensitivityLabels?api-version=2023-10-01-preview",
    f"{ACCT}/catalog/api/sensitivitylabels?api-version=2022-08-01-preview",
]:
    r = requests.get(ep, headers=h, timeout=15)
    print(f"  {ep.split('?')[0].split('/')[-1]}: {r.status_code}")
    if r.status_code == 200:
        labels = r.json().get("value", r.json()) if isinstance(r.json(), dict) else r.json()
        if isinstance(labels, list):
            for lb in labels[:10]:
                print(f"    {lb.get('name','?')} / {lb.get('id','?')}")
        break

print("\n" + "=" * 70)
print("8. FHIR/DICOM CUSTOM ENTITIES")
print("=" * 70)
for etype in ["healthcare_fhir_service", "healthcare_fhir_resource_type",
              "healthcare_dicom_service", "healthcare_dicom_modality"]:
    body = {"keywords": "*", "limit": 20, "filter": {"entityType": etype}}
    r = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if r.status_code == 200:
        ents = r.json().get("value", [])
        for e in ents:
            print(f"  [{etype.split('_')[-1]}] {e.get('name','?')} (guid={e.get('id','')[:12]}... qn={e.get('qualifiedName','')[:50]})")
    time.sleep(0.15)

print("\n" + "=" * 70)
print("DISCOVERY COMPLETE")
print("=" * 70)
