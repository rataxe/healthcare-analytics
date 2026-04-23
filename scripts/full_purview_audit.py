"""Full audit of what Purview has — entities, search, collections, data sources, scans."""
import requests, time, json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=2.0)))

ACCT = "https://prviewacc.purview.azure.com"
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"

def sep(title):
    print(f"\n{'='*60}\n{title}\n{'='*60}")

# ── 1. COLLECTIONS ──
sep("1. COLLECTIONS")
r = sess.get(f"{ACCT}/account/collections?api-version=2019-11-01-preview", headers=h, timeout=30)
if r.status_code == 200:
    for c in r.json().get("value", []):
        parent = c.get("parentCollection", {}).get("referenceName", "-")
        print(f"  {c['name']:20s} | friendly: {c.get('friendlyName', '?'):25s} | parent: {parent}")
else:
    print(f"  Error: {r.status_code}")
time.sleep(0.5)

# ── 2. DATA SOURCES ──
sep("2. DATA SOURCES")
r = sess.get(f"{SCAN_EP}/scan/datasources?api-version=2022-07-01-preview", headers=h, timeout=30)
if r.status_code == 200:
    for ds in r.json().get("value", []):
        print(f"  {ds['name']:25s} | kind: {ds.get('kind','?'):20s} | collection: {ds.get('properties',{}).get('collection',{}).get('referenceName','?')}")
else:
    print(f"  Error: {r.status_code}")
time.sleep(0.5)

# ── 3. SCAN STATUS ──
sep("3. LATEST SCAN RUNS")
for ds, scan in [("sql-hca-demo","healthcare-scan"),("Fabric","Scan-HCA"),("Fabric","Scan-BrainChild")]:
    r = sess.get(f"{SCAN_EP}/scan/datasources/{ds}/scans/{scan}/runs?api-version=2022-07-01-preview", headers=h, timeout=30)
    if r.status_code == 200:
        runs = r.json().get("value", [])
        if runs:
            s = runs[0]
            status = s.get("status", "?")
            start = str(s.get("startTime", "?"))[:19]
            end = str(s.get("endTime", "?"))[:19]
            print(f"  {ds}/{scan}: {status} | {start} -> {end}")
        else:
            print(f"  {ds}/{scan}: no runs")
    else:
        print(f"  {ds}/{scan}: {r.status_code}")
    time.sleep(0.3)

# ── 4. ATLAS SEARCH — all entities ──
sep("4. ATLAS SEARCH (all entities)")
body = {"keywords": "*", "limit": 100}
r = sess.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    data = r.json()
    total = data.get("@search.count", 0)
    entities = data.get("value", [])
    print(f"  Total entities found: {total}")
    
    # Group by type
    type_counts = {}
    for e in entities:
        etype = e.get("entityType", "?")
        type_counts[etype] = type_counts.get(etype, 0) + 1
    for t, c in sorted(type_counts.items()):
        print(f"    {t}: {c}")
    
    print(f"\n  Entity details (first 30):")
    for e in entities[:30]:
        name = e.get("name", "?")
        etype = e.get("entityType", "?")
        qn = e.get("qualifiedName", "")[:60]
        coll = e.get("assetType", ["?"])[0] if e.get("assetType") else "?"
        print(f"    [{etype:30s}] {name:35s} | {qn}")
else:
    print(f"  Search error: {r.status_code} — {r.text[:200]}")
time.sleep(0.5)

# ── 5. CHECK TERM ASSIGNMENTS ──
sep("5. TERM ASSIGNMENTS ON ENTITIES")
# Check patients table for assigned terms
r = sess.post(SEARCH, headers=h, json={"keywords": "patients", "limit": 5, "filter": {"objectType": "Tables"}}, timeout=30)
if r.status_code == 200:
    for e in r.json().get("value", []):
        guid = e.get("id", "")
        name = e.get("name", "?")
        terms = e.get("term", [])
        print(f"  {name}: {len(terms)} terms assigned")
        
        # Get full entity to see terms
        if guid:
            time.sleep(0.3)
            er = sess.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=30)
            if er.status_code == 200:
                ent = er.json().get("entity", {})
                meanings = ent.get("relationshipAttributes", {}).get("meanings", [])
                print(f"    meanings: {len(meanings)}")
                for m in meanings[:5]:
                    print(f"      - {m.get('displayText', '?')}")
                
                # Classifications
                classifs = ent.get("classifications", [])
                print(f"    classifications: {len(classifs)}")
                for cl in classifs[:5]:
                    cn = cl.get("typeName", "?") if isinstance(cl, dict) else str(cl)
                    print(f"      - {cn}")
                
                # Labels
                labels = ent.get("labels", [])
                print(f"    labels: {labels}")
else:
    print(f"  Error: {r.status_code}")
time.sleep(0.5)

# ── 6. UNIFIED CATALOG APIs ──
sep("6. UNIFIED CATALOG / DATA GOVERNANCE APIs")
uc_endpoints = [
    ("Governance domains", f"{ACCT}/datagovernance/catalog/domains", "2025-02-01-preview"),
    ("Data products", f"{ACCT}/datagovernance/catalog/dataproducts", "2025-02-01-preview"),
    ("Health", f"{ACCT}/datagovernance/catalog/health", "2025-02-01-preview"),
]
for label, url, ver in uc_endpoints:
    r = sess.get(f"{url}?api-version={ver}", headers=h, timeout=30)
    print(f"  {label}: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        items = data.get("value", []) if isinstance(data, dict) else data
        if isinstance(items, list):
            print(f"    -> {len(items)} items")
    elif r.status_code != 404:
        print(f"    -> {r.text[:150]}")
    time.sleep(0.3)

# ── 7. CHECK DATA PRODUCT ENTITIES (custom type) ──
sep("7. DATA PRODUCT ENTITIES (healthcare_data_product)")
body = {"keywords": "*", "limit": 50, "filter": {"typeName": "healthcare_data_product"}}
r = sess.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    data = r.json()
    count = data.get("@search.count", 0)
    print(f"  Found: {count} data product entities")
    for e in data.get("value", []):
        print(f"    - {e.get('name', '?')}")
else:
    print(f"  Error: {r.status_code}")

# ── 8. CHECK PII CLASSIFICATIONS ──
sep("8. PII CLASSIFICATIONS ON COLUMNS")
body = {"keywords": "*", "limit": 100, "filter": {"objectType": "Columns"}}
r = sess.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    cols = r.json().get("value", [])
    pii_count = 0
    for e in cols:
        classifs = e.get("classification", [])
        if classifs:
            pii_count += 1
            cnames = []
            for c in classifs:
                if isinstance(c, dict):
                    cnames.append(c.get("typeName", "?"))
                else:
                    cnames.append(str(c))
            print(f"    {e.get('name','?'):25s} -> {', '.join(cnames)}")
    print(f"\n  Columns with PII classifications: {pii_count}/{len(cols)}")

print("\n" + "="*60)
print("AUDIT COMPLETE")
print("="*60)
