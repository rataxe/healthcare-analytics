"""Quick diagnostic of current Purview state."""
import json, requests, sys, os
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
SEARCH = ACCT + "/catalog/api/search/query?api-version=2022-08-01-preview"
ATLAS = ACCT + "/catalog/api/atlas/v2"

# 1. Entities by collection
print("=== Entities by collection ===")
for col_id in ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "barncancer", "fabric-brainchild"]:
    body = {"filter": {"collectionId": col_id}, "limit": 1}
    r = requests.post(SEARCH, headers=h, json=body)
    d = r.json()
    count = d.get("@search.count", "?")
    print(f"  {col_id}: {count}")

# 2. Find SQL column entities
print("\n=== SQL column entities (sample) ===")
body = {"keywords": "patients", "filter": {"typeName": "azure_sql_column"}, "limit": 10}
r = requests.post(SEARCH, headers=h, json=body)
for v in r.json().get("value", []):
    qn = v.get("qualifiedName", "?")
    print(f"  {qn}")

# 3. SQL table entities
print("\n=== SQL table entities ===")
body = {"keywords": "", "filter": {"typeName": "azure_sql_table"}, "limit": 10}
r = requests.post(SEARCH, headers=h, json=body)
for v in r.json().get("value", []):
    qn = v.get("qualifiedName", "?")
    cid = v.get("collectionId", "?")
    print(f"  {qn}  [{cid}]")

# 4. SQL view entities
print("\n=== SQL view entities ===")
body = {"keywords": "", "filter": {"typeName": "azure_sql_view"}, "limit": 10}
r = requests.post(SEARCH, headers=h, json=body)
for v in r.json().get("value", []):
    qn = v.get("qualifiedName", "?")
    print(f"  {qn}")

# 5. Entity type facets
print("\n=== Entity types with counts ===")
body = {"keywords": "*", "facets": [{"facet": "typeName", "count": 50}], "limit": 1}
r = requests.post(SEARCH, headers=h, json=body)
for f in r.json().get("@search.facets", {}).get("typeName", []):
    print(f"  {f['value']}: {f['count']}")

# 6. Classifications applied
print("\n=== Custom classifications ===")
for cls_name in ["ICD10_Diagnosis_Code", "Swedish_Personnummer", "FHIR_Resource_ID", "SNOMED_CT_Code", "Patient_Name_PHI", "OMOP_Concept_ID"]:
    body = {"filter": {"classification": cls_name}, "limit": 1}
    r = requests.post(SEARCH, headers=h, json=body)
    cnt = r.json().get("@search.count", 0)
    status = "APPLIED" if cnt > 0 else "NOT APPLIED"
    print(f"  {cls_name}: {cnt} ({status})")

# 7. System classifications
print("\n=== System classifications (sample) ===")
for cls_name in ["MICROSOFT.PERSONAL.NAME", "MICROSOFT.PERSONAL.IPADDRESS", "MICROSOFT.PERSONAL.GENDER"]:
    body = {"filter": {"classification": cls_name}, "limit": 1}
    r = requests.post(SEARCH, headers=h, json=body)
    cnt = r.json().get("@search.count", 0)
    if cnt > 0:
        print(f"  {cls_name}: {cnt}")

# 8. Get one SQL table entity to see its columns structure
print("\n=== Sample SQL table entity details ===")
body = {"keywords": "patients", "filter": {"typeName": "azure_sql_table"}, "limit": 1}
r = requests.post(SEARCH, headers=h, json=body)
vals = r.json().get("value", [])
if vals:
    guid = vals[0]["id"]
    r2 = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h)
    ent = r2.json().get("entity", {})
    attrs = ent.get("attributes", {})
    print(f"  Name: {attrs.get('name')}")
    print(f"  QN: {attrs.get('qualifiedName')}")
    # Check relationship attributes for columns
    rel_attrs = ent.get("relationshipAttributes", {})
    cols = rel_attrs.get("columns", [])
    print(f"  Columns: {len(cols)}")
    for c in cols[:5]:
        print(f"    - {c.get('displayText', '?')} [{c.get('guid', '?')}]")
    # Check classifications
    classifications = ent.get("classifications", [])
    print(f"  Classifications on table: {len(classifications)}")
    for cl in classifications:
        print(f"    - {cl.get('typeName', '?')}")

print("\nDiagnostic complete.")
