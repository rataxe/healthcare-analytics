"""Quick Purview audit — check everything visible in Unified Catalog.

Uses a fresh requests.Session per section to avoid SSL connection-pool
exhaustion (Purview rate-limits TLS handshakes aggressively).
"""
import requests, time, json, sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

DELAY = 2.0  # seconds between sections

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

ACCT = "https://prviewacc.purview.azure.com"
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api"
SEARCH_URL = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=2.0, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def sep(title: str) -> None:
    print(f"\n{'='*60}\n  {title}\n{'='*60}", flush=True)


# ── 1. COLLECTIONS ──
try:
    sep("1. COLLECTIONS")
    s = make_session()
    # Try both account-level and scan-level collection endpoints
    for url in [
        f"{ACCT}/account/collections?api-version=2019-11-01-preview",
        f"{SCAN_EP}/scan/collections?api-version=2022-07-01-preview",
        f"{ACCT}/collections?api-version=2022-11-01-preview",
    ]:
        r = s.get(url, headers=h, timeout=30)
        if r.status_code == 200:
            colls = r.json().get("value", r.json().get("count", []))
            if isinstance(colls, list):
                print(f"  {len(colls)} collections (via {url.split('?')[0].split('/')[-1]})")
                for c in colls:
                    name = c.get("name", c.get("friendlyName", "?"))
                    friendly = c.get("friendlyName", "")
                    parent = c.get("parentCollection", {}).get("referenceName", "root")
                    print(f"    - {name}: {friendly} (parent={parent})")
                break
            else:
                print(f"  Response not a list: {str(colls)[:100]}")
    else:
        print(f"  All collection endpoints failed")
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")
time.sleep(DELAY)

# ── 2. DATA SOURCES ──
try:
    sep("2. DATA SOURCES")
    s = make_session()
    r = s.get(f"{SCAN_EP}/scan/datasources?api-version=2022-07-01-preview", headers=h, timeout=30)
    if r.status_code == 200:
        sources = r.json().get("value", [])
        print(f"  {len(sources)} data sources")
        for src in sources:
            print(f"    - {src['name']}: kind={src.get('kind','?')} collection={src.get('properties',{}).get('collection',{}).get('referenceName','?')}")
    else:
        print(f"  ERROR: {r.status_code}")
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")
time.sleep(DELAY)

# ── 3. SCANS & LATEST RUNS ──
try:
    sep("3. SCANS & LATEST RUNS")
    s = make_session()
    for ds_name in ["sql-hca-demo", "Fabric"]:
        r = s.get(f"{SCAN_EP}/scan/datasources/{ds_name}/scans?api-version=2022-07-01-preview", headers=h, timeout=30)
        if r.status_code == 200:
            scans = r.json().get("value", [])
            for sc in scans:
                sc_name = sc["name"]
                time.sleep(0.5)
                rr = s.get(f"{SCAN_EP}/scan/datasources/{ds_name}/scans/{sc_name}/runs?api-version=2022-07-01-preview", headers=h, timeout=30)
                runs = rr.json().get("value", []) if rr.status_code == 200 else []
                if runs:
                    latest = runs[0]
                    print(f"    {ds_name}/{sc_name}: {latest.get('status','?')} @ {str(latest.get('startTime','?'))[:19]}")
                else:
                    print(f"    {ds_name}/{sc_name}: NO RUNS")
        time.sleep(0.5)
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")
time.sleep(DELAY)

# ── 4. GLOSSARY ──
try:
    sep("4. GLOSSARY (terms & categories)")
    s = make_session()
    r = s.get(f"{ATLAS}/glossary", headers=h, timeout=60)
    if r.status_code == 200:
        data = r.json()
        glossaries = data if isinstance(data, list) else [data]
        for g in glossaries:
            gid = g["guid"]
            print(f"  Glossary: {g.get('name')} (guid={gid[:12]}...)")

            time.sleep(1)
            cr = s.get(f"{ATLAS}/glossary/{gid}/categories", headers=h, timeout=60)
            cats = cr.json() if cr.status_code == 200 and isinstance(cr.json(), list) else []
            print(f"  Categories: {len(cats)}")
            for c in cats:
                print(f"    - {c.get('name')}")

            time.sleep(1)
            tr = s.get(f"{ATLAS}/glossary/{gid}/terms?limit=500", headers=h, timeout=60)
            terms = tr.json() if tr.status_code == 200 and isinstance(tr.json(), list) else []
            print(f"  Terms: {len(terms)}")
            for t in terms:
                cat_name = ""
                cats_list = t.get("categories", [])
                if cats_list:
                    cat_name = f" [{cats_list[0].get('displayText', '?')}]"
                print(f"    - {t.get('name')}{cat_name}")
    else:
        print(f"  No glossary: {r.status_code} — {r.text[:200]}")
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")
time.sleep(DELAY)

# ── 5. ATLAS ENTITY SEARCH ──
try:
    sep("5. ATLAS ENTITIES (search)")
    s = make_session()
    type_counts = {}
    body = {"keywords": "*", "limit": 100}
    r = s.post(SEARCH_URL, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        data = r.json()
        total = data.get("@search.count", 0)
        print(f"  Total entities in search: {total}")
        for e in data.get("value", []):
            etype = e.get("entityType", "?")
            type_counts[etype] = type_counts.get(etype, 0) + 1
        for t, c in sorted(type_counts.items()):
            print(f"    {t}: {c}")
    else:
        print(f"  ERROR: {r.status_code} {r.text[:200]}")
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")
time.sleep(DELAY)

# ── 6. CLASSIFICATIONS ──
try:
    sep("6. CLASSIFICATIONS (custom)")
    s = make_session()
    r = s.get(f"{ATLAS}/types/typedefs?type=classification", headers=h, timeout=30)
    if r.status_code == 200:
        classifs = r.json().get("classificationDefs", [])
        custom = [c for c in classifs if not c.get("name", "").startswith("MICROSOFT.")]
        print(f"  Total classifications: {len(classifs)}, Custom: {len(custom)}")
        for c in custom:
            print(f"    - {c.get('name')}")
    else:
        print(f"  ERROR: {r.status_code}")
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")
time.sleep(DELAY)

# ── 7. DATA PRODUCTS (healthcare_data_product entities) ──
try:
    sep("7. DATA PRODUCTS (custom entity type)")
    s = make_session()
    body = {"keywords": "*", "filter": {"typeName": "healthcare_data_product"}, "limit": 50}
    r = s.post(SEARCH_URL, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        data = r.json()
        count = data.get("@search.count", 0)
        print(f"  healthcare_data_product entities: {count}")
        for e in data.get("value", []):
            print(f"    - {e.get('name', '?')}: {e.get('description', '')[:60]}")
    else:
        print(f"  ERROR: {r.status_code}")
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")
time.sleep(DELAY)

# ── 8. CHECK SPECIFIC TABLES ──
try:
    sep("8. CHECK SPECIFIC TABLES — classifications & terms")
    s = make_session()
    tables = ["patients", "encounters", "diagnoses", "vitals_labs", "medications", "vw_ml_encounters"]
    for tbl in tables:
        body = {"keywords": tbl, "filter": {"typeName": "azure_sql_table"}, "limit": 5}
        if tbl == "vw_ml_encounters":
            body["filter"]["typeName"] = "azure_sql_view"
        r = s.post(SEARCH_URL, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            results = r.json().get("value", [])
            if results:
                e = results[0]
                terms = [m.get("displayText", "?") for m in e.get("term", [])]
                classifs = []
                for c in e.get("classification", []):
                    if isinstance(c, dict):
                        classifs.append(c.get("typeName", "?"))
                    elif isinstance(c, str):
                        classifs.append(c)
                labels = [l.get("labelName", "?") for l in e.get("label", [])]
                print(f"  {tbl}: terms={terms}, classifs={classifs}, labels={labels}")
            else:
                print(f"  {tbl}: NOT FOUND in search")
        else:
            print(f"  {tbl}: search error {r.status_code}")
        time.sleep(0.5)
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")
time.sleep(DELAY)

# ── 9. GOVERNANCE DOMAINS (Unified Catalog) ──
try:
    sep("9. GOVERNANCE DOMAINS")
    s = make_session()
    found = False
    for ver in ["2025-09-15-preview", "2025-02-01-preview", "2024-11-01-preview"]:
        r = s.get(f"{ACCT}/datagovernance/catalog/domains?api-version={ver}", headers=h, timeout=30)
        print(f"  {ver}: {r.status_code}")
        if r.status_code == 200:
            items = r.json().get("value", [])
            print(f"  {len(items)} domains")
            for d in items:
                print(f"    - {d.get('name', '?')}")
            found = True
            break
        time.sleep(0.5)
    if not found:
        print("  ❌ Governance domains API not available — must create manually in portal")
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")
time.sleep(DELAY)

# ── 10. UNIFIED CATALOG DATA PRODUCTS ──
try:
    sep("10. UNIFIED CATALOG DATA PRODUCTS")
    s = make_session()
    found = False
    for ver in ["2025-09-15-preview", "2025-02-01-preview", "2024-11-01-preview"]:
        r = s.get(f"{ACCT}/datagovernance/catalog/dataproducts?api-version={ver}", headers=h, timeout=30)
        print(f"  {ver}: {r.status_code}")
        if r.status_code == 200:
            items = r.json().get("value", [])
            print(f"  {len(items)} data products")
            for d in items:
                print(f"    - {d.get('name', '?')}")
            found = True
            break
        time.sleep(0.5)
    if not found:
        print("  ❌ Data products API not available — must create manually in portal")
    s.close()
except Exception as e:
    print(f"  ERROR: {e}")

sep("SUMMARY")
print("  Done. Review above to identify what's missing from Unified Catalog.")
