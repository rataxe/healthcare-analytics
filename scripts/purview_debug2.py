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

# ── 1. Get patients table with FULL column GUIDs ──
print("1. Getting patients entity with full column GUIDs")
qn = "mssql://sql-hca-demo.database.windows.net/HealthcareAnalyticsDB/hca/patients"
r = s.get(f"{ATLAS_EP}/entity/uniqueAttribute/type/azure_sql_table?attr:qualifiedName={qn}", headers=h, timeout=30)
if r.status_code == 200:
    ent = r.json()
    e = ent.get("entity", {})
    ra = e.get("relationshipAttributes", {})
    cols = ra.get("columns", [])
    print(f"  Columns: {len(cols)}")
    for c in cols:
        print(f"    {c.get('displayText')}: guid={c.get('guid')}")
        # Check if guid is the same as table's guid
    print(f"\n  Table guid: {e.get('guid')}")
    
    # Full JSON of first column
    if cols:
        print(f"\n  Full column object sample:")
        print(json.dumps(cols[0], indent=2))

time.sleep(1)

# ── 2. Try getting column entity directly ──
print("\n2. Getting a column entity directly")
col_qn = f"{qn}#patient_id"
r = s.get(f"{ATLAS_EP}/entity/uniqueAttribute/type/azure_sql_column?attr:qualifiedName={col_qn}", headers=h, timeout=30)
if r.status_code == 200:
    col_ent = r.json().get("entity", {})
    print(f"  Column guid: {col_ent.get('guid')}")
    print(f"  Column type: {col_ent.get('typeName')}")
    print(f"  Column attrs: {list(col_ent.get('attributes', {}).keys())}")
else:
    print(f"  Failed: {r.status_code} {r.text[:200]}")

time.sleep(1)

# ── 3. Test description via partial update (createOrUpdate) ──
print("\n3. Testing description update via createOrUpdate")
tbl_guid = e.get("guid") if r.status_code == 200 else None

# Try using the entity bulk create/update endpoint with partial=true
# According to Atlas API, partial update uses: POST /entity with isIncomplete or attribute-level update
# Let's try PUT entity/guid with only the needed attribute via query param
r2 = s.put(
    f"{ATLAS_EP}/entity/guid/{e.get('guid')}?name=userDescription",
    headers=h,
    data=json.dumps("Patient demographics - 10,000 synthetic Swedish patients."),
    timeout=30,
)
print(f"  PUT ?name=userDescription: {r2.status_code}")
if r2.text:
    print(f"  Response: {r2.text[:200]}")

time.sleep(1)

# Try entity/guid/{guid} with full body including name
r3 = s.post(
    f"{ATLAS_EP}/entity",
    headers=h,
    json={"entity": {
        "typeName": "azure_sql_table",
        "attributes": {
            "qualifiedName": qn,
            "name": "patients",
            "userDescription": "Patient demographics - 10,000 synthetic Swedish patients.",
        }
    }},
    timeout=30,
)
print(f"\n  POST /entity (createOrUpdate): {r3.status_code}")
if r3.text:
    print(f"  Response: {r3.text[:300]}")
