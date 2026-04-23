"""Full Purview diagnostic — finds why nothing shows in the UI."""
import json, os, sys, time
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

ACCT = "https://prviewacc.purview.azure.com"
TENANT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
SCAN_EP = f"{ACCT}/scan"

def jt(d, n=300):
    return json.dumps(d, indent=2, ensure_ascii=False)[:n]

print("=" * 70)
print("  FULL PURVIEW DIAGNOSTIC")
print("=" * 70)

# ── 1. PERMISSIONS ──
print("\n--- 1. PERMISSIONS PER COLLECTION ---")
for coll in ["prviewacc", "halsosjukvard", "barncancer", "sql-databases", "fabric-analytics", "fabric-brainchild"]:
    r = requests.get(
        f"{ACCT}/policystore/collections/{coll}/effectiveAccess?api-version=2021-07-01",
        headers=h, timeout=15,
    )
    if r.status_code == 200:
        data = r.json()
        roles = [p.get("roleName", "?") for p in data.get("effectiveAccessPolicies", [])]
        print(f"  {coll}: {roles}")
    else:
        print(f"  {coll}: HTTP {r.status_code}")

# ── 2. METADATA POLICY (roles) ──
print("\n--- 2. METADATA POLICY (root collection) ---")
r = requests.get(
    f"{ACCT}/policystore/metadataPolicy?collectionName=prviewacc&api-version=2021-07-01",
    headers=h, timeout=15,
)
if r.status_code == 200:
    rules = r.json().get("properties", {}).get("attributeRules", [])
    for rule in rules:
        name = rule.get("name", "?")
        # Extract member IDs from dnfCondition
        members = []
        for cond_group in rule.get("dnfCondition", []):
            for cond in cond_group:
                vals = cond.get("attributeValueIncludedIn", [])
                members.extend(vals)
        print(f"  Role: {name}")
        if members:
            print(f"    Members: {members[:3]}")
else:
    print(f"  Policy: {r.status_code}")

# ── 3. DATA SOURCES ──
print("\n--- 3. DATA SOURCES ---")
r = requests.get(f"{SCAN_EP}/datasources?api-version=2022-07-01-preview", headers=h, timeout=15)
if r.status_code == 200:
    for ds in r.json().get("value", []):
        nm = ds.get("name", "?")
        kind = ds.get("kind", "?")
        props = ds.get("properties", {})
        coll = props.get("collection", {})
        print(f"  {nm}: kind={kind}, collection={coll}")
else:
    print(f"  HTTP {r.status_code}")

# ── 4. SCAN CONFIG + LINEAGE SETTINGS ──
print("\n--- 4. SCAN CONFIG ---")
for ds_name in ["sql-hca-demo", "Fabric"]:
    r = requests.get(
        f"{SCAN_EP}/datasources/{ds_name}/scans?api-version=2022-07-01-preview",
        headers=h, timeout=15,
    )
    if r.status_code == 200:
        for scan in r.json().get("value", []):
            sn = scan.get("name", "?")
            props = scan.get("properties", {})
            ruleset = props.get("scanRulesetName", "?")
            coll = props.get("collection", {})
            connected = props.get("connectedVia", {}).get("referenceName", "?")
            lineage = props.get("includeLineage", "N/A")
            print(f"  {ds_name}/{sn}:")
            print(f"    ruleset={ruleset}, collection={coll}")
            print(f"    connectedVia={connected}, includeLineage={lineage}")

            # Latest scan run
            r2 = requests.get(
                f"{SCAN_EP}/datasources/{ds_name}/scans/{sn}/runs?api-version=2022-07-01-preview",
                headers=h, timeout=15,
            )
            if r2.status_code == 200:
                runs = r2.json().get("value", [])
                if runs:
                    latest = runs[0]
                    status = latest.get("status", "?")
                    start = latest.get("startTime", "?")
                    end = latest.get("endTime", "?")
                    diag = latest.get("diagnostics", {})
                    print(f"    lastRun: {status} ({start} → {end})")
                    if diag:
                        notifications = diag.get("notifications", [])
                        exinfo = diag.get("exceptionCountMap", {})
                        if notifications:
                            for n in notifications[:3]:
                                print(f"    DIAG: {n.get('message','?')[:200]}")
                        if exinfo:
                            print(f"    Exceptions: {exinfo}")

# ── 5. ENTITY COUNTS BY COLLECTION ──
print("\n--- 5. ENTITY COUNTS BY COLLECTION ---")
for coll_id in ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "barncancer", "fabric-brainchild"]:
    body = {"keywords": "*", "limit": 1, "filter": {"collectionId": coll_id}}
    r = requests.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        cnt = r.json().get("@search.count", 0)
        print(f"  {coll_id}: {cnt} entities")
    else:
        print(f"  {coll_id}: HTTP {r.status_code}")

# ── 6. PROCESS / LINEAGE ENTITIES ──
print("\n--- 6. LINEAGE PROCESS ENTITIES ---")
body = {"keywords": "*", "limit": 50, "filter": {"entityType": "Process"}}
r = requests.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    procs = r.json().get("value", [])
    total = r.json().get("@search.count", 0)
    print(f"  Found: {total} Process entities")
    for p in procs[:5]:
        print(f"    - {p.get('name','?')} coll={p.get('collectionId','?')}")
else:
    print(f"  Search: {r.status_code}")

# Check one Process entity detail
body2 = {"keywords": "SQL ETL", "limit": 3}
r3 = requests.post(SEARCH, headers=h, json=body2, timeout=30)
if r3.status_code == 200:
    for v in r3.json().get("value", []):
        if v.get("entityType") == "Process":
            guid = v.get("id", "")
            r4 = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
            if r4.status_code == 200:
                ent = r4.json().get("entity", {})
                attrs = ent.get("attributes", {})
                rels = ent.get("relationshipAttributes", {})
                print(f"\n  DETAIL: {attrs.get('name','?')} (guid={guid})")
                print(f"    typeName: {ent.get('typeName','?')}")
                print(f"    status: {ent.get('status','?')}")
                print(f"    collectionId: {ent.get('collectionId','?')}")
                inp = rels.get("inputs", [])
                out = rels.get("outputs", [])
                print(f"    inputs ({len(inp)}): {[i.get('displayText','?') for i in inp]}")
                print(f"    outputs ({len(out)}): {[o.get('displayText','?') for o in out]}")
                col_map = attrs.get("columnMapping", "")
                if col_map:
                    print(f"    columnMapping: {col_map[:200]}")
            break

# ── 7. SENSITIVITY LABELS / MIP ──
print("\n--- 7. SENSITIVITY LABELS ---")
# Try scan API
for api_v in ["2022-07-01-preview", "2023-09-01"]:
    r = requests.get(f"{SCAN_EP}/sensitivitylabels?api-version={api_v}", headers=h, timeout=15)
    print(f"  scan API ({api_v}): {r.status_code} {r.text[:200] if r.status_code != 200 else 'OK'}")

# Try catalog/datamap
r = requests.get(f"{ACCT}/catalog/api/atlas/v2/types/typedef/name/Microsoft.Label", headers=h, timeout=15)
print(f"  Microsoft.Label typedef: {r.status_code}")

# Try MIP token
try:
    mip_tok = cred.get_token("https://syncservice.o365syncservice.com/.default").token
    print(f"  MIP token: acquired OK")
except Exception as e:
    print(f"  MIP token: FAILED - {e}")

# Try Purview MIP config
r = requests.get(f"{ACCT}/scan/sensitivitylabels/current?api-version=2022-07-01-preview", headers=h, timeout=15)
print(f"  MIP current config: {r.status_code} {r.text[:200]}")

# ── 8. CLASSIFICATIONS ON SQL COLUMNS ──
print("\n--- 8. SQL COLUMN CLASSIFICATIONS ---")
body_sql = {"keywords": "patients", "limit": 5, "filter": {"entityType": "azure_sql_table"}}
r = requests.post(SEARCH, headers=h, json=body_sql, timeout=30)
if r.status_code == 200:
    for v in r.json().get("value", []):
        if v.get("name") == "patients":
            guid = v.get("id", "")
            r_ent = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
            if r_ent.status_code == 200:
                ent = r_ent.json().get("entity", {})
                cls = ent.get("classifications", [])
                print(f"  patients table: {len(cls)} classifications")
                cols = ent.get("relationshipAttributes", {}).get("columns", [])
                print(f"  patients columns: {len(cols)}")
                for col in cols[:8]:
                    cg = col.get("guid", "")
                    rc = requests.get(f"{ATLAS}/entity/guid/{cg}", headers=h, timeout=15)
                    if rc.status_code == 200:
                        ce = rc.json().get("entity", {})
                        cn = ce.get("attributes", {}).get("name", "?")
                        ccls = [c.get("typeName", "?") for c in ce.get("classifications", [])]
                        print(f"    {cn}: {ccls if ccls else '(none)'}")
                    time.sleep(0.1)
            break

# ── 9. FABRIC TABLE CLASSIFICATIONS ──
print("\n--- 9. FABRIC TABLE CLASSIFICATIONS ---")
body_fab = {"keywords": "hca_patients", "limit": 5, "filter": {"entityType": "fabric_lakehouse_table"}}
r = requests.post(SEARCH, headers=h, json=body_fab, timeout=30)
if r.status_code == 200:
    for v in r.json().get("value", []):
        if "hca_patients" in v.get("name", ""):
            guid = v.get("id", "")
            r_ent = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
            if r_ent.status_code == 200:
                ent = r_ent.json().get("entity", {})
                cls = ent.get("classifications", [])
                print(f"  hca_patients: {len(cls)} classifications")
                cols = ent.get("relationshipAttributes", {}).get("columns", [])
                print(f"  hca_patients columns: {len(cols)}")
                for col in cols[:8]:
                    cg = col.get("guid", "")
                    rc = requests.get(f"{ATLAS}/entity/guid/{cg}", headers=h, timeout=15)
                    if rc.status_code == 200:
                        ce = rc.json().get("entity", {})
                        cn = ce.get("attributes", {}).get("name", "?")
                        ccls = [c.get("typeName", "?") for c in ce.get("classifications", [])]
                        print(f"    {cn}: {ccls if ccls else '(none)'}")
                    time.sleep(0.1)
            break

# ── 10. GLOSSARY TERM→ENTITY LINKS (verify broken ones) ──
print("\n--- 10. BROKEN TERM-ENTITY LINKS ---")
glossary_guid = "d939ea20-9c67-48af-98d9-b66965f7cde1"
r = requests.get(f"{ATLAS}/glossary/{glossary_guid}/terms?limit=200&offset=0", headers=h, timeout=15)
if r.status_code == 200:
    terms = r.json()
    print(f"  Total terms: {len(terms)}")
    # Find FHIR ImagingStudy term
    for t in terms:
        if t.get("name") == "FHIR ImagingStudy":
            tguid = t.get("guid", "")
            # Get full term
            rt = requests.get(f"{ATLAS}/glossary/term/{tguid}", headers=h, timeout=15)
            if rt.status_code == 200:
                td = rt.json()
                assigned = td.get("assignedEntities", [])
                print(f"  FHIR ImagingStudy: {len(assigned)} assigned entities")
                for a in assigned[:5]:
                    print(f"    - {a.get('displayText','?')} guid={a.get('guid','?')} type={a.get('typeName','?')}")
        if t.get("name") == "VCF (Variant Call Format)":
            tguid = t.get("guid", "")
            rt = requests.get(f"{ATLAS}/glossary/term/{tguid}", headers=h, timeout=15)
            if rt.status_code == 200:
                td = rt.json()
                assigned = td.get("assignedEntities", [])
                print(f"  VCF: {len(assigned)} assigned entities")

# ── 11. GOVERNANCE DOMAINS (multi-scope tokens) ──
print("\n--- 11. GOVERNANCE DOMAINS ---")
for scope in ["https://purview.azure.net/.default"]:
    tok = cred.get_token(scope).token
    h2 = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    for api_v in ["2025-09-15-preview", "2024-03-01-preview"]:
        base = f"{TENANT_EP}/datagovernance/catalog"
        r = requests.get(f"{base}/governanceDomains?api-version={api_v}", headers=h2, timeout=15)
        print(f"  Domains ({api_v}): {r.status_code} {r.text[:300] if r.status_code != 200 else jt(r.json())}")
        r = requests.get(f"{base}/dataProducts?api-version={api_v}", headers=h2, timeout=15)
        print(f"  Products ({api_v}): {r.status_code} {r.text[:300] if r.status_code != 200 else jt(r.json())}")

# ── 12. CHECK PURVIEW PORTAL URL ──
print("\n--- 12. PURVIEW PORTAL ---")
print(f"  Classic Portal: https://web.purview.azure.com/resource/prviewacc")
print(f"  New UX: https://purview.microsoft.com")
print(f"  Account: {ACCT}")

# ── 13. CHECK IF ENTITY HAS 'status' = ACTIVE ──
print("\n--- 13. ENTITY STATUS CHECK ---")
# Check a SQL table
body_chk = {"keywords": "patients", "limit": 1, "filter": {"entityType": "azure_sql_table"}}
r = requests.post(SEARCH, headers=h, json=body_chk, timeout=30)
if r.status_code == 200:
    for v in r.json().get("value", []):
        guid = v.get("id", "")
        r_e = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
        if r_e.status_code == 200:
            ent = r_e.json().get("entity", {})
            print(f"  {ent.get('attributes',{}).get('name','?')}: status={ent.get('status','?')}")
            print(f"    collectionId={ent.get('collectionId','?')}")

# Check a custom entity
body_chk2 = {"keywords": "BrainChild FHIR", "limit": 5}
r = requests.post(SEARCH, headers=h, json=body_chk2, timeout=30)
if r.status_code == 200:
    for v in r.json().get("value", []):
        if "FHIR" in v.get("name", "") and v.get("entityType", "").startswith("healthcare"):
            guid = v.get("id", "")
            r_e = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
            if r_e.status_code == 200:
                ent = r_e.json().get("entity", {})
                print(f"  {ent.get('attributes',{}).get('name','?')}: status={ent.get('status','?')}")
                print(f"    typeName={ent.get('typeName','?')}")
                print(f"    collectionId={ent.get('collectionId','?')}")
            break

# Check a Process entity
body_chk3 = {"keywords": "SQL ETL", "limit": 3}
r = requests.post(SEARCH, headers=h, json=body_chk3, timeout=30)
if r.status_code == 200:
    for v in r.json().get("value", []):
        if v.get("entityType") == "Process":
            guid = v.get("id", "")
            r_e = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
            if r_e.status_code == 200:
                ent = r_e.json().get("entity", {})
                print(f"  {ent.get('attributes',{}).get('name','?')}: status={ent.get('status','?')}")
                print(f"    collectionId={ent.get('collectionId','?')}")
            break

print("\n" + "=" * 70)
print("  DIAGNOSTIC COMPLETE")
print("=" * 70)
