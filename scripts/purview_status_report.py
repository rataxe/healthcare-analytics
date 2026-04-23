"""Check classification status for ALL columns across all SQL tables."""
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

QN_BASE = "mssql://sql-hca-demo.database.windows.net/HealthcareAnalyticsDB/hca"
TABLE_NAMES = ["patients", "encounters", "diagnoses", "vitals_labs", "medications"]

print("=" * 70)
print("PURVIEW METADATA STATUS REPORT")
print("=" * 70)

all_classified = []
all_unclassified = []

for tbl in TABLE_NAMES:
    qn = f"{QN_BASE}/{tbl}"
    r = s.get(f"{ATLAS_EP}/entity/uniqueAttribute/type/azure_sql_table?attr:qualifiedName={qn}", headers=h, timeout=30)
    if r.status_code != 200:
        continue
    
    ent = r.json().get("entity", {})
    tbl_desc = ent.get("attributes", {}).get("userDescription", "")
    tbl_guid = ent.get("guid")
    cols = ent.get("relationshipAttributes", {}).get("columns", [])
    
    print(f"\n📋 {tbl.upper()} ({tbl_guid[:12]}...)")
    print(f"   Description: {tbl_desc[:80]}..." if tbl_desc else "   Description: (none)")
    
    # Check glossary terms on table
    terms = ent.get("relationshipAttributes", {}).get("meanings", [])
    if terms:
        term_names = [t.get("displayText", "?") for t in terms]
        print(f"   Glossary terms: {', '.join(term_names)}")
    
    for c in sorted(cols, key=lambda x: x.get("displayText", "")):
        col_name = c["displayText"]
        col_guid = c["guid"]
        
        # Get classifications for this column
        r2 = s.get(f"{ATLAS_EP}/entity/guid/{col_guid}/classifications", headers=h, timeout=30)
        if r2.status_code == 200:
            cls_list = r2.json().get("list", [])
            if cls_list:
                cls_names = [cl.get("typeName", "?") for cl in cls_list]
                sources = [cl.get("source", "?") for cl in cls_list]
                print(f"   ✅ {col_name}: {', '.join(cls_names)} (source: {', '.join(sources)})")
                all_classified.append(f"{tbl}.{col_name}")
            else:
                print(f"   ⬜ {col_name}: (no classification)")
                all_unclassified.append(f"{tbl}.{col_name}")
        else:
            print(f"   ⬜ {col_name}: (no classification)")
            all_unclassified.append(f"{tbl}.{col_name}")
        time.sleep(0.2)
    time.sleep(0.5)

print(f"\n{'=' * 70}")
print(f"SUMMARY")
print(f"{'=' * 70}")
print(f"  Classified columns: {len(all_classified)}")
for c in all_classified:
    print(f"    ✅ {c}")
print(f"  Unclassified columns: {len(all_unclassified)}")
for c in all_unclassified:
    print(f"    ⬜ {c}")
