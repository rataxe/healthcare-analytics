"""Deep Purview Audit — verify everything is in the RIGHT place."""
import requests, json, sys, time
from collections import defaultdict
from azure.identity import AzureCliCredential
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token

session = requests.Session()
retries = Retry(total=5, backoff_factor=2.0, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.headers.update({"Authorization": f"Bearer {token}"})

CATALOG = "https://prviewacc.purview.azure.com"
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
GLOSSARY_GUID = "d939ea20-9c67-48af-98d9-b66965f7cde1"

def api_get(url):
    time.sleep(0.5)
    return session.get(url)

def api_post(url, **kwargs):
    time.sleep(0.5)
    return session.post(url, **kwargs)

ok_count = 0
warn_count = 0
fail_count = 0

def ok(msg):
    global ok_count; ok_count += 1; print(f"  OK  {msg}")
def warn(msg):
    global warn_count; warn_count += 1; print(f"  WARN  {msg}")
def fail(msg):
    global fail_count; fail_count += 1; print(f"  FAIL  {msg}")
def info(msg):
    print(f"  INFO  {msg}")

print("=" * 70)
print("  DEEP PURVIEW AUDIT")
print("  Collections - Entities - Terms - Labels - Classifications")
print("=" * 70)

# -- 1. Collections hierarchy --
print("\n--- 1. COLLECTIONS HIERARCHY ---")
r = api_get(f"{SCAN_EP}/account/collections?api-version=2019-11-01-preview")
colls = r.json().get("value", [])
coll_map = {}
for c in colls:
    name = c.get("friendlyName", c["name"])
    coll_map[c["name"]] = name

parent_map = {}
for c in colls:
    parent_map[c["name"]] = c.get("parentCollection", {}).get("referenceName")

def print_tree(node, indent=0):
    print(f"  {'  ' * indent}> {coll_map.get(node, node)}")
    children = [c for c, p in parent_map.items() if p == node]
    for child in sorted(children, key=lambda x: coll_map.get(x, x)):
        print_tree(child, indent + 1)

roots = [c for c, p in parent_map.items() if p is None]
for root in roots:
    print_tree(root)
ok(f"{len(colls)} collections found")

found_names = set(coll_map.values())
for en in ["IT", "SQL Databases", "Fabric Analytics", "Barncancerforskning", "Fabric BrainChild"]:
    if en in found_names:
        ok(f"Collection '{en}' exists")
    else:
        fail(f"Collection '{en}' MISSING")
if any("lsosjukv" in n for n in found_names):
    ok("Collection 'Halsosjukvard' exists")
else:
    fail("Collection 'Halsosjukvard' MISSING")

# -- 2. Data Sources --
print("\n--- 2. DATA SOURCES -> COLLECTIONS ---")
r = api_get(f"{SCAN_EP}/scan/datasources?api-version=2022-07-01-preview")
sources = r.json().get("value", [])
for ds in sources:
    ds_name = ds["name"]
    coll_ref = ds.get("properties", {}).get("collection", {}).get("referenceName", "?")
    coll_name = coll_map.get(coll_ref, coll_ref)
    info(f"Source '{ds_name}' -> Collection '{coll_name}'")
    if ds_name == "sql-hca-demo":
        if "SQL" in coll_name:
            ok(f"sql-hca-demo in correct collection ({coll_name})")
        else:
            warn(f"sql-hca-demo in unexpected collection: {coll_name}")
    elif ds_name == "Fabric":
        ok(f"Fabric source in collection ({coll_name})")

# -- 3. All Glossaries --
print("\n--- 3. GLOSSARIES & CATEGORIES ---")
r = api_get(f"{CATALOG}/catalog/api/atlas/v2/glossary")
raw = r.json() if r.status_code == 200 else []
all_glossaries = raw if isinstance(raw, list) else [raw]

cat_map = {}
all_terms = []

for g in all_glossaries:
    gname = g.get("name", "?")
    guid = g["guid"]
    info(f"Glossary: '{gname}' (guid={guid[:8]}...)")
    
    cr = api_get(f"{CATALOG}/catalog/api/atlas/v2/glossary/{guid}/categories")
    gcats = cr.json() if cr.status_code == 200 else []
    if isinstance(gcats, list):
        for gc in gcats:
            cat_map[gc["guid"]] = gc.get("name", "?")
            info(f"  Category: '{gc.get('name', '?')}'")
    
    tr = api_get(f"{CATALOG}/catalog/api/atlas/v2/glossary/{guid}/terms?limit=500")
    gterms = tr.json() if tr.status_code == 200 else []
    if isinstance(gterms, list):
        for t in gterms:
            t["_glossary"] = gname
        all_terms.extend(gterms)

# -- 4. Terms by category --
print(f"\n--- 4. ALL GLOSSARY TERMS ({len(all_terms)} total) ---")

by_cat = defaultdict(list)
for t in all_terms:
    cats_list = t.get("categories", [])
    if cats_list:
        for cat_ref in cats_list:
            cname = cat_ref.get("displayText", cat_map.get(cat_ref.get("categoryGuid", ""), "?"))
            by_cat[cname].append(t["name"])
    else:
        by_cat["(no category)"].append(t["name"])

dp_count = 0
okr_count = 0
dq_count = 0
base_count = 0

for cat_name in sorted(by_cat.keys()):
    tnames = sorted(by_cat[cat_name])
    print(f"\n  [{cat_name}] ({len(tnames)} terms)")
    for tn in tnames:
        if tn.startswith("DP"):
            dp_count += 1
            print(f"    [DP] {tn}")
        elif "OKR" in tn:
            okr_count += 1
            print(f"    [OKR] {tn}")
        elif tn.startswith("DQ-"):
            dq_count += 1
            print(f"    [DQ] {tn}")
        else:
            base_count += 1
            print(f"    {tn}")

print(f"\n  Summary: {base_count} base, {dp_count} DP, {okr_count} OKR, {dq_count} DQ")
if dp_count >= 4: ok(f"{dp_count} Data Product terms")
else: warn(f"Only {dp_count} Data Product terms (expected >=4)")
if okr_count >= 4: ok(f"{okr_count} OKR terms")
else: warn(f"Only {okr_count} OKR terms (expected >=4)")
if dq_count >= 10: ok(f"{dq_count} DQ rule terms")
else: warn(f"Only {dq_count} DQ rule terms (expected >=10)")

# -- 5. Atlas entities --
print("\n--- 5. ATLAS ENTITIES - PLACEMENT ---")
search_body = {"keywords": "*", "limit": 100}
r = api_post(f"{CATALOG}/catalog/api/search/query?api-version=2022-08-01-preview", json=search_body)
search_results = r.json().get("value", []) if r.status_code == 200 else []
info(f"Search returned {len(search_results)} entities")

by_type = defaultdict(list)
for e in search_results:
    by_type[e.get("entityType", "unknown")].append(e)

for etype in sorted(by_type.keys()):
    entities = by_type[etype]
    print(f"\n  [{etype}] ({len(entities)})")
    for e in entities:
        name = e.get("name", "?")
        coll_id = e.get("collectionId", "?")
        coll_display = coll_map.get(coll_id, coll_id)
        labels = e.get("label", [])
        terms_d = [t.get("name", "?") for t in e.get("term", [])]
        classif = []
        raw_classif = e.get("classification", [])
        if isinstance(raw_classif, list):
            for c in raw_classif:
                if isinstance(c, dict):
                    classif.append(c.get("typeName", "?"))
                elif isinstance(c, str):
                    classif.append(c)
        
        parts = [f"coll={coll_display}"]
        if labels: parts.append(f"labels={labels}")
        if terms_d: parts.append(f"terms={terms_d}")
        if classif: parts.append(f"PII={classif}")
        print(f"    {name:40s} | {' | '.join(parts)}")

# -- 6. Patients deep check --
print("\n--- 6. PATIENTS TABLE - DEEP CHECK ---")
patients_guid = "aa926266-f6f3-4bd3-ab0c-c6e21cb3f3e2"
r = api_get(f"{CATALOG}/catalog/api/atlas/v2/entity/guid/{patients_guid}")
columns = []
if r.status_code == 200:
    entity = r.json().get("entity", {})
    attrs = entity.get("attributes", {})
    info(f"Name: {attrs.get('name')}")
    info(f"QualifiedName: {attrs.get('qualifiedName')}")
    desc = attrs.get("userDescription", attrs.get("description", "(none)"))
    info(f"Description: {desc}")
    
    labels = entity.get("labels", [])
    if labels: ok(f"Labels: {labels}")
    else: warn("No labels on patients entity")
    
    classifications = entity.get("classifications", [])
    if classifications:
        ok(f"Classifications: {[c.get('typeName') for c in classifications]}")
    else:
        warn("No PII classifications on patients entity")
    
    rel_attrs = entity.get("relationshipAttributes", {})
    meanings = rel_attrs.get("meanings", [])
    if meanings:
        ok(f"Glossary terms ({len(meanings)}): {[m.get('displayText') for m in meanings]}")
    else:
        warn("No glossary terms on patients")
    
    columns = rel_attrs.get("columns", [])
    info(f"Columns linked: {len(columns)}")
else:
    fail(f"Could not fetch patients entity: {r.status_code}")

# -- 7. Sample columns --
print("\n--- 7. SAMPLE COLUMNS ---")
if columns:
    for col in columns[:5]:
        col_guid = col.get("guid", "")
        col_name = col.get("displayText", "?")
        cr = api_get(f"{CATALOG}/catalog/api/atlas/v2/entity/guid/{col_guid}")
        if cr.status_code == 200:
            ce = cr.json().get("entity", {})
            ca = ce.get("attributes", {})
            data_type = ca.get("data_type", "(missing)")
            col_desc = ca.get("userDescription", ca.get("description", "(none)"))
            info(f"{col_name}: type={data_type}, desc={str(col_desc)[:60] if col_desc else '(none)'}")
        else:
            warn(f"Column {col_name}: fetch failed ({cr.status_code})")

# -- 8. Data Product entities --
print("\n--- 8. DATA PRODUCT ENTITIES ---")
dp_search = {"keywords": "*", "limit": 50, "filter": {"entityType": "healthcare_data_product"}}
r = api_post(f"{CATALOG}/catalog/api/search/query?api-version=2022-08-01-preview", json=dp_search)
dp_entities = r.json().get("value", []) if r.status_code == 200 else []
info(f"Data Product entities: {len(dp_entities)}")
for dpe in dp_entities:
    name = dpe.get("name", "?")
    coll_id = dpe.get("collectionId", "?")
    coll_display = coll_map.get(coll_id, coll_id)
    print(f"    {name:50s} | coll={coll_display}")
if len(dp_entities) >= 4:
    ok(f"{len(dp_entities)} data product entities found")
else:
    warn(f"Only {len(dp_entities)} data product entities")

# -- 9. PII Classifications --
print("\n--- 9. PII CLASSIFICATIONS ---")
pr = api_get(f"{CATALOG}/catalog/api/atlas/v2/entity/guid/{patients_guid}/classifications")
if pr.status_code == 200:
    pii_list = pr.json().get("list", [])
    for pii in pii_list:
        ok(f"PII: {pii.get('typeName')}")
    if not pii_list:
        warn("No PII classifications on patients")
else:
    info(f"Classifications endpoint: {pr.status_code}")

# -- 10. All tables deep check --
print("\n--- 10. ALL TABLE/VIEW ENTITIES ---")
table_guids = {
    "patients": "aa926266-f6f3-4bd3-ab0c-c6e21cb3f3e2",
    "encounters": "d86a7adf-0f07-4dcd-b01b-68e07b2f5d37",
    "diagnoses": "d4f88b9a-0b18-45db-b4e5-08e1f14e4fb4",
    "vitals_labs": "319942ea-2820-4a1e-a319-f8a33fd8f750",
    "medications": "eb7a580f-a1df-4fcf-a5c9-7ee5cc66efd0",
    "vw_ml_encounters": "37485c47-c3aa-44e2-8e78-b5f9f1b2e7ad",
}
for tname, tguid in table_guids.items():
    tr = api_get(f"{CATALOG}/catalog/api/atlas/v2/entity/guid/{tguid}")
    if tr.status_code == 200:
        te = tr.json().get("entity", {})
        tlabels = te.get("labels", [])
        tclass = te.get("classifications", [])
        tmeanings = te.get("relationshipAttributes", {}).get("meanings", [])
        tcols = te.get("relationshipAttributes", {}).get("columns", [])
        
        parts = []
        if tlabels: parts.append(f"labels={tlabels}")
        if tclass: parts.append(f"PII={[c.get('typeName') for c in tclass]}")
        if tmeanings: parts.append(f"terms={len(tmeanings)}")
        parts.append(f"cols={len(tcols)}")
        
        ok(f"{tname}: {' | '.join(parts)}")
    else:
        fail(f"{tname}: entity not found ({tr.status_code})")

# -- SUMMARY --
print("\n" + "=" * 70)
print(f"  DEEP AUDIT COMPLETE: {ok_count} OK, {warn_count} warnings, {fail_count} failures")
if fail_count == 0 and warn_count == 0:
    print("  ALL CHECKS PASSED!")
elif fail_count == 0:
    print("  NO FAILURES - Some warnings to review")
else:
    print("  ISSUES FOUND - Review above")
print("=" * 70)
"""Deep Purview Audit — verify everything is in the RIGHT place."""
import requests, json, sys, time
from collections import defaultdict
from azure.identity import AzureCliCredential
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token

session = requests.Session()
retries = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
session.mount("https://", HTTPAdapter(max_retries=retries))
session.headers.update({"Authorization": f"Bearer {token}"})
h = {"Authorization": f"Bearer {token}"}

CATALOG = "https://prviewacc.purview.azure.com"
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
GLOSSARY_GUID = "d939ea20-9c67-48af-98d9-b66965f7cde1"

def api_get(url):
    time.sleep(0.3)
    return session.get(url)

def api_post(url, **kwargs):
    time.sleep(0.3)
    return session.post(url, **kwargs)

ok_count = 0
warn_count = 0
fail_count = 0

def ok(msg):
    global ok_count; ok_count += 1; print(f"  ✅ {msg}")
def warn(msg):
    global warn_count; warn_count += 1; print(f"  ⚠️  {msg}")
def fail(msg):
    global fail_count; fail_count += 1; print(f"  ❌ {msg}")
def info(msg):
    print(f"  ℹ️  {msg}")

print("=" * 70)
print("  DEEP PURVIEW AUDIT")
print("  Collections • Entities • Terms • Labels • Classifications")
print("=" * 70)

# ── 1. Collections hierarchy ──
print("\n━━━ 1. COLLECTIONS HIERARCHY ━━━")
r = requests.get(f"{SCAN_EP}/account/collections?api-version=2019-11-01-preview", headers=h)
colls = r.json().get("value", [])
coll_map = {}  # id -> friendlyName
for c in colls:
    name = c.get("friendlyName", c["name"])
    coll_map[c["name"]] = name

# Build tree
parent_map = {}
for c in colls:
    cid = c["name"]
    parent = c.get("parentCollection", {}).get("referenceName")
    parent_map[cid] = parent

def print_tree(node, indent=0):
    print(f"  {'  ' * indent}📁 {coll_map.get(node, node)}")
    children = [c for c, p in parent_map.items() if p == node]
    for child in sorted(children, key=lambda x: coll_map.get(x, x)):
        print_tree(child, indent + 1)

roots = [c for c, p in parent_map.items() if p is None]
for root in roots:
    print_tree(root)
ok(f"{len(colls)} collections found")

# Expected hierarchy:
expected_hierarchy = {
    "IT": "prviewacc",
    "sjcblj": "prviewacc",          # Hälsosjukvård  
}
# We'll check that key collections exist
expected_names = ["IT", "Hälsosjukvård", "SQL Databases", "Fabric Analytics", "Barncancerforskning", "Fabric BrainChild"]
found_names = set(coll_map.values())
for en in expected_names:
    if en in found_names:
        ok(f"Collection '{en}' exists")
    else:
        fail(f"Collection '{en}' MISSING")

# ── 2. Data Sources placement ──
print("\n━━━ 2. DATA SOURCES → COLLECTIONS ━━━")
r = requests.get(f"{SCAN_EP}/scan/datasources?api-version=2022-07-01-preview", headers=h)
sources = r.json().get("value", [])
for ds in sources:
    ds_name = ds["name"]
    coll_ref = ds.get("properties", {}).get("collection", {}).get("referenceName", "?")
    coll_name = coll_map.get(coll_ref, coll_ref)
    info(f"Source '{ds_name}' → Collection '{coll_name}'")
    if ds_name == "sql-hca-demo":
        if "SQL" in coll_name or "Hälsosjukvård" in coll_name:
            ok(f"sql-hca-demo in correct collection ({coll_name})")
        else:
            warn(f"sql-hca-demo in unexpected collection: {coll_name}")
    elif ds_name == "Fabric":
        ok(f"Fabric source registered in collection ({coll_name})")

# ── 3. Glossary Categories ──
print("\n━━━ 3. GLOSSARY CATEGORIES ━━━")
r = requests.get(f"{CATALOG}/catalog/api/atlas/v2/glossary/{GLOSSARY_GUID}/categories", headers=h)
cats = r.json() if r.status_code == 200 else []
cat_map = {}
for c in cats:
    cat_map[c["guid"]] = c.get("name", "?")

# Also get categories from data_products glossary
r2 = requests.get(f"{CATALOG}/catalog/api/atlas/v2/glossary", headers=h)
all_glossaries = r2.json() if r2.status_code == 200 else []
all_cats = {}
if isinstance(all_glossaries, list):
    for g in all_glossaries:
        gname = g.get("name", "?")
        guid = g["guid"]
        info(f"Glossary: '{gname}' (guid={guid[:8]}...)")
        gr = requests.get(f"{CATALOG}/catalog/api/atlas/v2/glossary/{guid}/categories", headers=h)
        gcats = gr.json() if gr.status_code == 200 else []
        for gc in gcats:
            all_cats[gc["guid"]] = {"name": gc.get("name", "?"), "glossary": gname}
            cat_map[gc["guid"]] = gc.get("name", "?")
elif isinstance(all_glossaries, dict):
    # Single glossary
    gname = all_glossaries.get("name", "?")
    info(f"Glossary: '{gname}' (guid={all_glossaries['guid'][:8]}...)")

for guid, data in all_cats.items():
    info(f"  Category: '{data['name']}' in glossary '{data['glossary']}'")

# ── 4. ALL Glossary Terms by category ──
print("\n━━━ 4. ALL GLOSSARY TERMS BY CATEGORY ━━━")
# Get terms from ALL glossaries
all_terms = []
if isinstance(all_glossaries, list):
    for g in all_glossaries:
        guid = g["guid"]
        tr = requests.get(f"{CATALOG}/catalog/api/atlas/v2/glossary/{guid}/terms?limit=500", headers=h)
        gterms = tr.json() if tr.status_code == 200 else []
        if isinstance(gterms, list):
            all_terms.extend(gterms)
elif isinstance(all_glossaries, dict):
    tr = requests.get(f"{CATALOG}/catalog/api/atlas/v2/glossary/{all_glossaries['guid']}/terms?limit=500", headers=h)
    gterms = tr.json() if tr.status_code == 200 else []
    if isinstance(gterms, list):
        all_terms.extend(gterms)

info(f"Total terms across all glossaries: {len(all_terms)}")

by_cat = defaultdict(list)
for t in all_terms:
    cats_list = t.get("categories", [])
    if cats_list:
        for cat_ref in cats_list:
            cname = cat_ref.get("displayText", cat_map.get(cat_ref.get("categoryGuid", ""), "?"))
            by_cat[cname].append(t["name"])
    else:
        by_cat["(no category)"].append(t["name"])

dp_count = 0
okr_count = 0
dq_count = 0
base_count = 0

for cat_name in sorted(by_cat.keys()):
    tnames = sorted(by_cat[cat_name])
    print(f"\n  📂 [{cat_name}] ({len(tnames)} terms)")
    for tn in tnames:
        prefix = ""
        if tn.startswith("DP"):
            dp_count += 1
            prefix = "🏷️ "
        elif "OKR" in tn:
            okr_count += 1
            prefix = "📊 "
        elif tn.startswith("DQ-"):
            dq_count += 1
            prefix = "🔍 "
        else:
            base_count += 1
        print(f"    {prefix}{tn}")

print(f"\n  Summary: {base_count} base terms, {dp_count} data products, {okr_count} OKRs, {dq_count} DQ rules")
if dp_count >= 4:
    ok(f"{dp_count} Data Product terms found")
else:
    warn(f"Only {dp_count} Data Product terms (expected ≥4)")
if okr_count >= 4:
    ok(f"{okr_count} OKR terms found")
else:
    warn(f"Only {okr_count} OKR terms (expected ≥4)")
if dq_count >= 10:
    ok(f"{dq_count} DQ rule terms found")
else:
    warn(f"Only {dq_count} DQ rule terms (expected ≥10)")

# ── 5. Atlas Entities — detailed ──
print("\n━━━ 5. ATLAS ENTITIES — PLACEMENT & METADATA ━━━")

# Get all entities via search
search_body = {"keywords": "*", "limit": 100}
r = requests.post(f"{CATALOG}/catalog/api/search/query?api-version=2022-08-01-preview", headers=h, json=search_body)
search_results = r.json().get("value", []) if r.status_code == 200 else []
info(f"Search returned {len(search_results)} entities")

# Group by type
by_type = defaultdict(list)
for e in search_results:
    etype = e.get("entityType", "unknown")
    by_type[etype].append(e)

for etype in sorted(by_type.keys()):
    entities = by_type[etype]
    print(f"\n  [{etype}] ({len(entities)} entities)")
    for e in entities:
        name = e.get("name", "?")
        coll_id = e.get("collectionId", "?")
        coll_name_display = coll_map.get(coll_id, coll_id)
        labels = e.get("label", [])
        terms_display = [t.get("name", "?") for t in e.get("term", [])]
        classifications = [c.get("typeName", "?") for c in e.get("classification", [])]
        
        parts = [f"collection={coll_name_display}"]
        if labels:
            parts.append(f"labels={labels}")
        if terms_display:
            parts.append(f"terms={terms_display}")
        if classifications:
            parts.append(f"PII={classifications}")
        print(f"    {name:40s} | {' | '.join(parts)}")

# Check specific entity details (patients table)
print("\n━━━ 6. PATIENTS TABLE — DEEP CHECK ━━━")
patients_guid = "aa926266-f6f3-4bd3-ab0c-c6e21cb3f3e2"
r = requests.get(f"{CATALOG}/catalog/api/atlas/v2/entity/guid/{patients_guid}", headers=h)
if r.status_code == 200:
    entity = r.json().get("entity", {})
    attrs = entity.get("attributes", {})
    info(f"Name: {attrs.get('name')}")
    info(f"QualifiedName: {attrs.get('qualifiedName')}")
    info(f"Description: {attrs.get('userDescription', attrs.get('description', '(none)'))}")
    
    # Labels
    labels = entity.get("labels", [])
    if labels:
        ok(f"Labels: {labels}")
    else:
        warn("No labels on patients entity")
    
    # Classifications
    classifications = entity.get("classifications", [])
    if classifications:
        ok(f"Classifications ({len(classifications)}): {[c.get('typeName') for c in classifications]}")
    else:
        warn("No PII classifications on patients entity")
    
    # Terms
    rel_attrs = entity.get("relationshipAttributes", {})
    meanings = rel_attrs.get("meanings", [])
    if meanings:
        ok(f"Glossary terms ({len(meanings)}): {[m.get('displayText') for m in meanings]}")
    else:
        warn("No glossary terms mapped to patients")
    
    # Columns
    columns = rel_attrs.get("columns", [])
    info(f"Columns: {len(columns)} linked")
else:
    fail(f"Could not fetch patients entity: {r.status_code}")

# Check a few column entities 
print("\n━━━ 7. SAMPLE COLUMN ENTITIES ━━━")
# Get column GUIDs from patients
if r.status_code == 200 and columns:
    for col in columns[:5]:
        col_guid = col.get("guid", "")
        col_name = col.get("displayText", "?")
        cr = requests.get(f"{CATALOG}/catalog/api/atlas/v2/entity/guid/{col_guid}", headers=h)
        if cr.status_code == 200:
            ce = cr.json().get("entity", {})
            ca = ce.get("attributes", {})
            data_type = ca.get("data_type", "(missing)")
            desc = ca.get("userDescription", ca.get("description", "(none)"))
            info(f"Column: {col_name} | type={data_type} | desc={desc[:50] if desc else '(none)'}")
        else:
            warn(f"Column {col_name}: fetch failed ({cr.status_code})")

# ── 8. Data Product entities ──
print("\n━━━ 8. DATA PRODUCT ENTITIES ━━━")
dp_search = {"keywords": "healthcare_data_product", "limit": 30,
             "filter": {"entityType": "healthcare_data_product"}}
r = requests.post(f"{CATALOG}/catalog/api/search/query?api-version=2022-08-01-preview", headers=h, json=dp_search)
dp_entities = r.json().get("value", []) if r.status_code == 200 else []
info(f"Data Product entities: {len(dp_entities)}")
for dpe in dp_entities:
    name = dpe.get("name", "?")
    coll_id = dpe.get("collectionId", "?")
    coll_name_display = coll_map.get(coll_id, coll_id)
    print(f"    {name:50s} | collection={coll_name_display}")

# ── 9. Classification types ──
print("\n━━━ 9. PII CLASSIFICATIONS CHECK ━━━")
# Check what classifications exist on our entities
class_search = {"keywords": "*", "limit": 100, 
                "filter": {"and": [{"classification": "MICROSOFT.PERSONAL.*"}]}}
# Just check directly what classifications patients has
pr = requests.get(f"{CATALOG}/catalog/api/atlas/v2/entity/guid/{patients_guid}/classifications", headers=h)
if pr.status_code == 200:
    pii_list = pr.json().get("list", [])
    for pii in pii_list:
        ok(f"PII Classification: {pii.get('typeName')}")
    if not pii_list:
        warn("No PII classifications on patients")
else:
    info(f"Classifications endpoint returned {pr.status_code}")

# ── SUMMARY ──
print("\n" + "=" * 70)
print(f"  DEEP AUDIT SUMMARY: {ok_count} OK, {warn_count} warnings, {fail_count} failures")
if fail_count == 0:
    print("  ✅ Everything is in the right place!")
else:
    print("  ⚠️  Issues found — review above")
print("=" * 70)
