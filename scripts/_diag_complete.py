"""Comprehensive Purview diagnostic - check EVERYTHING."""
import requests, sys, os, json
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

issues = []
sources = []

# ============================================================
# 1. COLLECTIONS
# ============================================================
print("=" * 70)
print("1. COLLECTIONS")
print("=" * 70)
r = requests.get(f"{ACCT}/account/collections?api-version=2019-11-01-preview", headers=h, timeout=15)
colls = []
if r.status_code == 200:
    colls = r.json().get("value", [])
    for c in colls:
        nm = c.get("friendlyName", c["name"])
        parent = c.get("parentCollection", {}).get("referenceName", "ROOT")
        print(f"  {nm} ({c['name']}) -> parent={parent}")
    print(f"  Total: {len(colls)} collections")
else:
    issues.append(f"Collections API failed: {r.status_code}")
    print(f"  ERROR: {r.status_code}")

# ============================================================
# 2. DATA SOURCES
# ============================================================
print("\n" + "=" * 70)
print("2. DATA SOURCES")
print("=" * 70)
r = requests.get(f"{SCAN_BASE}/datasources?{SCAN_API}", headers=h, timeout=15)
if r.status_code == 200:
    sources = r.json().get("value", [])
    for s in sources:
        nm = s.get("name", "?")
        kind = s.get("kind", "?")
        coll = s.get("properties", {}).get("collection", {}).get("referenceName", "?")
        print(f"  {nm} | kind={kind} | coll={coll}")
    if not sources:
        issues.append("NO data sources registered!")
        print("  WARNING: No data sources found!")
else:
    issues.append(f"Data sources API failed: {r.status_code}")
    print(f"  ERROR: {r.status_code}")

# ============================================================
# 3. SCANS & RECENT RUNS
# ============================================================
print("\n" + "=" * 70)
print("3. SCANS & RECENT RUNS")
print("=" * 70)
for s in sources:
    src_name = s["name"]
    r2 = requests.get(f"{SCAN_BASE}/datasources/{src_name}/scans?{SCAN_API}",
                      headers=h, timeout=15)
    if r2.status_code != 200:
        continue
    scans = r2.json().get("value", [])
    for sc in scans:
        scan_name = sc.get("name", "?")
        scan_kind = sc.get("kind", "?")
        print(f"\n  [{src_name}] Scan: {scan_name} ({scan_kind})")

        r3 = requests.get(
            f"{SCAN_BASE}/datasources/{src_name}/scans/{scan_name}/runs?{SCAN_API}",
            headers=h, timeout=15)
        if r3.status_code != 200:
            continue
        runs = r3.json().get("value", [])
        if runs:
            for run in runs[:2]:
                status = run.get("status", "?")
                start = run.get("startTime", "?")
                end = run.get("endTime", "?")
                diag = run.get("diagnostics", {})
                exc_map = diag.get("exceptionCountMap", {}) if isinstance(diag, dict) else {}
                notifs = diag.get("notifications", []) if isinstance(diag, dict) else []

                emoji = "OK" if status == "Succeeded" else ("WARN" if status == "CompletedWithExceptions" else "FAIL")
                print(f"    [{emoji}] {status} | {start}")

                if exc_map:
                    print(f"      Exceptions: {json.dumps(exc_map, ensure_ascii=False)}")
                if isinstance(notifs, list):
                    for n in notifs[:3]:
                        msg = n.get("message", json.dumps(n, ensure_ascii=False)[:200])
                        print(f"      Notif: {msg[:200]}")

                if status not in ("Succeeded", "CompletedWithExceptions"):
                    issues.append(f"Scan {src_name}/{scan_name}: {status}")

                error_log = run.get("error")
                if error_log:
                    print(f"      ERROR: {json.dumps(error_log, ensure_ascii=False)[:300]}")
                    issues.append(f"Scan error: {src_name}/{scan_name}")
        else:
            issues.append(f"No runs for {src_name}/{scan_name}")
            print(f"    WARNING: No runs")

    if not scans:
        issues.append(f"Source {src_name} has no scans configured")
        print(f"  WARNING: {src_name} has no scans")

# ============================================================
# 4. ENTITY COUNTS BY COLLECTION
# ============================================================
print("\n" + "=" * 70)
print("4. ENTITY COUNTS BY COLLECTION")
print("=" * 70)
for c in colls:
    cname = c["name"]
    fname = c.get("friendlyName", cname)
    body = {"filter": {"collectionId": cname}, "limit": 1}
    r4 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if r4.status_code == 200:
        cnt = r4.json().get("@search.count", 0)
        flag = " <-- EMPTY!" if cnt == 0 and cname not in ("prviewacc", "upiwjm") else ""
        print(f"  {fname} ({cname}): {cnt}{flag}")
        if cnt == 0 and cname not in ("prviewacc", "upiwjm"):
            issues.append(f"Empty collection: {fname}")

# ============================================================
# 5. ENTITY COUNTS BY TYPE
# ============================================================
print("\n" + "=" * 70)
print("5. ENTITY COUNTS BY TYPE (non-zero)")
print("=" * 70)
all_types = [
    "azure_sql_server", "azure_sql_db", "azure_sql_schema",
    "azure_sql_table", "azure_sql_view", "azure_sql_column",
    "azure_sql_stored_procedure",
    "Process",
    "healthcare_data_product", "healthcare_fhir_service",
    "healthcare_fhir_resource_type", "healthcare_dicom_service",
    "healthcare_dicom_modality",
    "Notebook", "Pipeline", "Lakehouse",
    "azure_datalake_gen2_resource_set", "azure_datalake_gen2_path",
    "azure_datalake_gen2_filesystem",
    "AtlasGlossaryTerm", "AtlasGlossaryCategory", "AtlasGlossary",
]
for et in all_types:
    body = {"filter": {"entityType": et}, "limit": 1}
    r5 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if r5.status_code == 200:
        cnt = r5.json().get("@search.count", 0)
        if cnt > 0:
            print(f"  {et}: {cnt}")

body_all = {"keywords": "*", "limit": 1}
rt = requests.post(SEARCH, headers=h, json=body_all, timeout=15)
grand = rt.json().get("@search.count", 0) if rt.status_code == 200 else "?"
print(f"\n  GRAND TOTAL: {grand}")

# ============================================================
# 6. GLOSSARY
# ============================================================
print("\n" + "=" * 70)
print("6. GLOSSARY & CATEGORIES")
print("=" * 70)
rg = requests.get(f"{ATLAS}/glossary", headers=h, timeout=15)
glist = rg.json() if isinstance(rg.json(), list) else [rg.json()]
g = glist[0]
gguid = g["guid"]
print(f"  Glossary: {g.get('name','?')} ({gguid})")
cats = g.get("categories", [])
print(f"  Categories: {len(cats)}")
for c in cats:
    print(f"    - {c.get('displayText','?')}")

all_terms = []
offset = 0
while True:
    r8 = requests.get(f"{ATLAS}/glossary/{gguid}/terms?limit=100&offset={offset}",
                      headers=h, timeout=15)
    batch = r8.json() if r8.status_code == 200 else []
    if not batch:
        break
    all_terms.extend(batch)
    offset += len(batch)
    if len(batch) < 100:
        break
print(f"  Terms: {len(all_terms)}")

# Category counts
by_cat = {}
nocats = 0
for t in all_terms:
    tc = t.get("categories", [])
    if tc:
        cn = tc[0].get("displayText", "?")
        by_cat[cn] = by_cat.get(cn, 0) + 1
    else:
        nocats += 1
print(f"  Uncategorized: {nocats}")
for cn, cnt in sorted(by_cat.items()):
    print(f"    {cn}: {cnt}")

# ============================================================
# 7. CLASSIFICATIONS
# ============================================================
print("\n" + "=" * 70)
print("7. CLASSIFICATIONS")
print("=" * 70)
custom_cls = [
    "Swedish Personnummer", "Patient Name PHI", "ICD10 Diagnosis Code",
    "SNOMED CT Code", "FHIR Resource ID", "OMOP Concept ID"
]
for cls in custom_cls:
    body = {"filter": {"classification": cls}, "limit": 5}
    rc = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if rc.status_code == 200:
        cnt = rc.json().get("@search.count", 0)
        entities = rc.json().get("value", [])
        flag = "OK" if cnt > 0 else "MISSING!"
        print(f"  {cls}: {cnt} [{flag}]")
        for e in entities[:2]:
            print(f"    -> {e.get('qualifiedName','?')[:80]}")
        if cnt == 0:
            issues.append(f"Classification {cls} not applied to any entity")

# Also check Microsoft auto-classifications
print("\n  Microsoft auto-classifications:")
body = {"filter": {"classification": "MICROSOFT.*"}, "limit": 1}
rm = requests.post(SEARCH, headers=h, json=body, timeout=15)
# Try a few common ones
for mc in ["MICROSOFT.PERSONAL.NAME", "MICROSOFT.PERSONAL.EMAIL", "MICROSOFT.GOVERNMENT.EU.NATIONAL.IDENTIFICATION.NUMBER"]:
    body = {"filter": {"classification": mc}, "limit": 1}
    rm2 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if rm2.status_code == 200:
        cnt = rm2.json().get("@search.count", 0)
        if cnt > 0:
            print(f"    {mc}: {cnt}")

# ============================================================
# 8. LINEAGE
# ============================================================
print("\n" + "=" * 70)
print("8. LINEAGE (Process entities)")
print("=" * 70)
body = {"filter": {"entityType": "Process"}, "limit": 10}
rp = requests.post(SEARCH, headers=h, json=body, timeout=15)
if rp.status_code == 200:
    procs = rp.json().get("value", [])
    cnt = rp.json().get("@search.count", 0)
    print(f"  Total Process entities: {cnt}")
    for p in procs[:5]:
        print(f"    - {p.get('name','?')} | coll={p.get('collectionId','?')}")
    # Check one process for inputs/outputs
    if procs:
        pg = procs[0].get("id", "")
        rp2 = requests.get(f"{ATLAS}/entity/guid/{pg}", headers=h, timeout=15)
        if rp2.status_code == 200:
            pe = rp2.json().get("entity", {})
            inputs = pe.get("attributes", {}).get("inputs", [])
            outputs = pe.get("attributes", {}).get("outputs", [])
            print(f"\n  Sample lineage ({pe.get('attributes',{}).get('name','?')}):")
            print(f"    inputs: {len(inputs)}, outputs: {len(outputs)}")

# ============================================================
# 9. GOVERNANCE DOMAINS
# ============================================================
print("\n" + "=" * 70)
print("9. GOVERNANCE DOMAINS")
print("=" * 70)
DG = f"{ACCT}/datagovernance/catalog"
rd = requests.get(f"{DG}/businessDomains?api-version=2025-09-15-preview", headers=h, timeout=15)
if rd.status_code == 200:
    doms = rd.json().get("value", [])
    for d in doms:
        print(f"  {d['name']} | status={d.get('status','?')} | id={d['id'][:20]}...")
else:
    print(f"  API: {rd.status_code}")

# ============================================================
# 10. TERM-ENTITY ASSIGNMENTS
# ============================================================
print("\n" + "=" * 70)
print("10. TERM-ENTITY ASSIGNMENTS")
print("=" * 70)
assigned_count = 0
unassigned_terms = []
for t in all_terms:
    tg = t["guid"]
    rt2 = requests.get(f"{ATLAS}/glossary/term/{tg}", headers=h, timeout=15)
    if rt2.status_code == 200:
        ft = rt2.json()
        ae = ft.get("assignedEntities", [])
        if ae:
            assigned_count += 1
        else:
            unassigned_terms.append(ft.get("name", "?"))
print(f"  Assigned: {assigned_count}/{len(all_terms)}")
print(f"  Unassigned: {len(unassigned_terms)}")
if unassigned_terms:
    print(f"  Sample unassigned: {', '.join(unassigned_terms[:10])}")

# ============================================================
# 11. SQL TABLE DETAILS
# ============================================================
print("\n" + "=" * 70)
print("11. SQL TABLE DETAILS")
print("=" * 70)
body = {"filter": {"entityType": "azure_sql_table"}, "limit": 20}
rtb = requests.post(SEARCH, headers=h, json=body, timeout=15)
if rtb.status_code == 200:
    tables = rtb.json().get("value", [])
    for t in tables:
        nm = t.get("name", "?")
        coll = t.get("collectionId", "?")
        cls_list = t.get("classification", [])
        terms_list = t.get("term", [])
        cls_str = ", ".join(cls_list) if cls_list else "none"
        term_str = ", ".join([tt.get("name", "?") for tt in terms_list]) if terms_list else "none"
        print(f"  {nm} | coll={coll} | cls=[{cls_str}] | terms=[{term_str}]")

# ============================================================
# 12. CUSTOM ENTITY DETAILS
# ============================================================
print("\n" + "=" * 70)
print("12. CUSTOM ENTITIES (healthcare_*)")
print("=" * 70)
for ct in ["healthcare_data_product", "healthcare_fhir_service", "healthcare_fhir_resource_type",
           "healthcare_dicom_service", "healthcare_dicom_modality"]:
    body = {"filter": {"entityType": ct}, "limit": 10}
    rce = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if rce.status_code == 200:
        ents = rce.json().get("value", [])
        cnt = rce.json().get("@search.count", 0)
        if cnt > 0:
            print(f"\n  {ct}: {cnt}")
            for e in ents:
                print(f"    - {e.get('name','?')} | coll={e.get('collectionId','?')}")

# ============================================================
# 13. CHECK domainId ON TERMS
# ============================================================
print("\n" + "=" * 70)
print("13. TERM domainId CHECK (governance domain links)")
print("=" * 70)
domain_linked = 0
for t in all_terms[:20]:
    tg = t["guid"]
    rt3 = requests.get(f"{ATLAS}/glossary/term/{tg}", headers=h, timeout=15)
    if rt3.status_code == 200:
        ft = rt3.json()
        did = ft.get("domainId")
        if did:
            domain_linked += 1
            print(f"  {ft['name']}: domainId={did}")
print(f"  Terms with domainId (sample 20): {domain_linked}")

# ============================================================
# FINAL SUMMARY
# ============================================================
print("\n" + "=" * 70)
print("ISSUES SUMMARY")
print("=" * 70)
if issues:
    for i, issue in enumerate(issues, 1):
        print(f"  {i}. {issue}")
else:
    print("  No critical issues found!")
print(f"\nTotal issues: {len(issues)}")
print("\nDiagnostic complete!")
