"""Deep-dive into specific issues found by _diag_complete.py."""
import requests, json, sys, os
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8")
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
SCAN_BASE = f"{ACCT}/scan"
SCAN_API = "api-version=2022-07-01-preview"

# ============================================================
# A. FABRIC SCAN EXCEPTIONS DETAIL
# ============================================================
print("=" * 70)
print("A. FABRIC SCAN EXCEPTIONS - FULL DETAIL")
print("=" * 70)
r = requests.get(f"{SCAN_BASE}/datasources/Fabric/scans/Scan-IzR/runs?{SCAN_API}", headers=h, timeout=15)
if r.status_code == 200:
    runs = r.json().get("value", [])
    for run in runs[:1]:
        print(f"  Status: {run.get('status')}")
        print(f"  Start:  {run.get('startTime')}")
        print(f"  End:    {run.get('endTime')}")
        diag = run.get("diagnostics", {})
        if isinstance(diag, dict):
            print(f"\n  Exception counts:")
            for k, v in diag.get("exceptionCountMap", {}).items():
                print(f"    {k}: {v}")
            notifs = diag.get("notifications", [])
            print(f"\n  Notifications ({len(notifs)}):")
            for n in notifs[:10]:
                print(f"    - {json.dumps(n, ensure_ascii=False)[:300]}")
        # Also check run detail
        run_id = run.get("id", "")
        if run_id:
            r2 = requests.get(
                f"{SCAN_BASE}/datasources/Fabric/scans/Scan-IzR/runs/{run_id}?{SCAN_API}",
                headers=h, timeout=15)
            if r2.status_code == 200:
                detail = r2.json()
                error = detail.get("error")
                if error:
                    print(f"\n  Error detail: {json.dumps(error, ensure_ascii=False)[:500]}")

# ============================================================
# B. WHAT ARE THE FABRIC ENTITY TYPES?
# ============================================================
print("\n" + "=" * 70)
print("B. FABRIC ENTITY TYPES (discover actual types in fabric-analytics)")
print("=" * 70)
# Search fabric-analytics collection for all entities, sample by offset
body = {"filter": {"collectionId": "fabric-analytics"}, "limit": 50}
r3 = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r3.status_code == 200:
    ents = r3.json().get("value", [])
    type_counts = {}
    for e in ents:
        et = e.get("entityType", "?")
        type_counts[et] = type_counts.get(et, 0) + 1
    print("  Sample of 50 entities - type distribution:")
    for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")
    # Show a few examples of each type
    print("\n  Examples by type:")
    shown = set()
    for e in ents:
        et = e.get("entityType", "?")
        if et not in shown:
            shown.add(et)
            print(f"    [{et}] {e.get('name', '?')[:80]} | qn={e.get('qualifiedName','')[:100]}")

# Now search for broader Fabric types
print("\n  Searching specific Fabric/PowerBI types:")
for ft in ["powerbi_dataset", "powerbi_table", "powerbi_column", "powerbi_report",
           "powerbi_workspace", "powerbi_dashboard", "powerbi_dataflow",
           "azure_datalake_gen2_resource_set", "azure_datalake_gen2_path",
           "azure_datalake_gen2_filesystem", "azure_datalake_gen2_service",
           "Notebook", "Pipeline", "Lakehouse"]:
    body = {"filter": {"entityType": ft}, "limit": 1}
    r4 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if r4.status_code == 200:
        cnt = r4.json().get("@search.count", 0)
        if cnt > 0:
            print(f"    {ft}: {cnt}")

# ============================================================
# C. BARNCANCER COLLECTION - WHY EMPTY?
# ============================================================
print("\n" + "=" * 70)
print("C. BARNCANCER COLLECTION - investigation")
print("=" * 70)
# fabric-brainchild is child of barncancer, has 29 entities
# barncancer itself has 0 - is this expected?
# Check what custom entities are in halsosjukvard that SHOULD be in barncancer
body = {"filter": {"collectionId": "fabric-brainchild"}, "limit": 50}
r5 = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r5.status_code == 200:
    bc_ents = r5.json().get("value", [])
    bc_types = {}
    for e in bc_ents:
        et = e.get("entityType", "?")
        bc_types[et] = bc_types.get(et, 0) + 1
    print(f"  fabric-brainchild entities ({len(bc_ents)}):")
    for t, c in sorted(bc_types.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")

# Check "BrainChild Barncancerforskning" data product - is it in halsosjukvard or barncancer?
body = {"keywords": "BrainChild Barncancerforskning", "limit": 5}
r6 = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r6.status_code == 200:
    for e in r6.json().get("value", []):
        print(f"\n  '{e.get('name')}' -> coll={e.get('collectionId')} type={e.get('entityType')}")

# ============================================================
# D. SNOMED CT / OMOP COLUMNS - WHERE SHOULD THEY GO?
# ============================================================
print("\n" + "=" * 70)
print("D. SNOMED CT & OMOP - finding target columns")
print("=" * 70)
# Search for columns with snomed-related names
body = {"keywords": "snomed", "filter": {"entityType": "azure_sql_column"}, "limit": 10}
r7 = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r7.status_code == 200:
    cols = r7.json().get("value", [])
    print(f"  SNOMED columns found: {r7.json().get('@search.count', 0)}")
    for c in cols:
        print(f"    {c.get('qualifiedName','?')[-80:]}")

# Search for columns with concept_id in name
body = {"keywords": "concept_id", "filter": {"entityType": "azure_sql_column"}, "limit": 10}
r8 = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r8.status_code == 200:
    cols = r8.json().get("value", [])
    print(f"\n  concept_id columns found: {r8.json().get('@search.count', 0)}")
    for c in cols:
        print(f"    {c.get('qualifiedName','?')[-80:]}")

# Search all SQL columns to see what we have
body = {"filter": {"entityType": "azure_sql_column"}, "limit": 100}
r9 = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r9.status_code == 200:
    all_cols = r9.json().get("value", [])
    total = r9.json().get("@search.count", 0)
    print(f"\n  Total SQL columns: {total}")
    # Group by table
    by_table = {}
    for c in all_cols:
        qn = c.get("qualifiedName", "")
        parts = qn.split("/")
        table = parts[-2] if len(parts) >= 2 else "?"
        col = parts[-1] if parts else "?"
        by_table.setdefault(table, []).append(col)
    for table, cols in sorted(by_table.items()):
        print(f"    {table}: {', '.join(cols[:10])}")

# ============================================================
# E. TERM-ENTITY LINK COVERAGE
# ============================================================
print("\n" + "=" * 70)
print("E. TERM-ENTITY ASSIGNMENT GAPS")
print("=" * 70)
# Get all terms with full detail to see which are assigned
GG = "d939ea20-9c67-48af-98d9-b66965f7cde1"
all_terms = []
offset = 0
while True:
    r10 = requests.get(f"{ATLAS}/glossary/{GG}/terms?limit=100&offset={offset}",
                       headers=h, timeout=15)
    batch = r10.json() if r10.status_code == 200 else []
    if not batch:
        break
    all_terms.extend(batch)
    offset += len(batch)
    if len(batch) < 100:
        break

unassigned = []
assigned = []
for t in all_terms:
    tg = t["guid"]
    r11 = requests.get(f"{ATLAS}/glossary/term/{tg}", headers=h, timeout=15)
    if r11.status_code == 200:
        ft = r11.json()
        ae = ft.get("assignedEntities", [])
        cats = ft.get("categories", [])
        cat_name = cats[0].get("displayText", "?") if cats else "?"
        if ae:
            assigned.append((ft["name"], cat_name, len(ae)))
        else:
            unassigned.append((ft["name"], cat_name))

print(f"  Assigned: {len(assigned)}")
print(f"  Unassigned: {len(unassigned)}")
print(f"\n  Unassigned by category:")
cat_unassigned = {}
for name, cat in unassigned:
    cat_unassigned.setdefault(cat, []).append(name)
for cat, terms in sorted(cat_unassigned.items()):
    print(f"    {cat}: {len(terms)}")
    for t in terms[:5]:
        print(f"      - {t}")
    if len(terms) > 5:
        print(f"      ... +{len(terms)-5} more")

# ============================================================
# F. ROOT COLLECTION CONTENTS CHECK
# ============================================================
print("\n" + "=" * 70)
print("F. ROOT COLLECTION (prviewacc) - what's there?")
print("=" * 70)
body = {"filter": {"collectionId": "prviewacc"}, "limit": 20}
r12 = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r12.status_code == 200:
    root_ents = r12.json().get("value", [])
    root_types = {}
    for e in root_ents:
        et = e.get("entityType", "?")
        root_types[et] = root_types.get(et, 0) + 1
    print(f"  Root entity types (sample 20):")
    for t, c in sorted(root_types.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")
    # Non-glossary entities in root?
    non_glossary = [e for e in root_ents if not e.get("entityType", "").startswith("AtlasGlossary")]
    if non_glossary:
        print(f"\n  NON-GLOSSARY entities still in root: {len(non_glossary)}")
        for e in non_glossary:
            print(f"    [{e.get('entityType')}] {e.get('name','?')}")

print("\nDeep-dive complete!")
