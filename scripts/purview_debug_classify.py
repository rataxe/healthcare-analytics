import requests, json, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
ATLAS_EP = "https://prviewacc.purview.azure.com/catalog/api/atlas/v2"
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
s = requests.Session()
s.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

# Known column GUIDs from table relationship attrs
# diagnoses.icd10_code
diag_cols = {}
qn = "mssql://sql-hca-demo.database.windows.net/HealthcareAnalyticsDB/hca/diagnoses"
r = s.get(f"{ATLAS_EP}/entity/uniqueAttribute/type/azure_sql_table?attr:qualifiedName={qn}", headers=h, timeout=30)
if r.status_code == 200:
    for c in r.json()["entity"]["relationshipAttributes"]["columns"]:
        diag_cols[c["displayText"]] = c["guid"]

print("Diagnoses columns:")
for n, g in diag_cols.items():
    print(f"  {n}: {g}")

# Try classifying icd10_code with full error
icd10_guid = diag_cols.get("icd10_code")
if icd10_guid:
    print(f"\nClassifying icd10_code ({icd10_guid})...")
    
    # First check existing classifications
    r = s.get(f"{ATLAS_EP}/entity/guid/{icd10_guid}/classifications", headers=h, timeout=30)
    print(f"  GET classifications: {r.status_code}")
    if r.status_code == 200:
        print(f"  Existing: {json.dumps(r.json(), indent=2)[:500]}")
    else:
        print(f"  Error: {r.text[:500]}")

    # Try POST classification
    r = s.post(
        f"{ATLAS_EP}/entity/guid/{icd10_guid}/classifications",
        headers=h,
        json=[{"typeName": "ICD10 Diagnosis Code"}],
        timeout=30,
    )
    print(f"\n  POST classification: {r.status_code}")
    print(f"  Full error: {r.text[:500]}")

    # Try via entity/bulk/setClassifications
    print("\n  Trying bulk setClassifications...")
    r2 = s.post(
        f"{ATLAS_EP}/entity/bulk/setClassifications",
        headers=h,
        json={"guidHeaderMap": {
            icd10_guid: {"classifications": [{"typeName": "ICD10 Diagnosis Code"}]}
        }},
        timeout=30,
    )
    print(f"  Bulk set: {r2.status_code} {r2.text[:500]}")

# List all classification type defs
print("\n\nListing classification defs...")
r = s.get(f"{ATLAS_EP}/types/typedefs?type=classification", headers=h, timeout=30)
if r.status_code == 200:
    defs = r.json().get("classificationDefs", [])
    custom = [d for d in defs if not d.get("name", "").startswith(("MICROSOFT.", "Microsoft."))]
    print(f"  Custom classification types ({len(custom)}):")
    for d in custom:
        print(f"    {d['name']} (category={d.get('category')}, guid={d.get('guid', '?')[:12]})")
