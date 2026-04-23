"""
Investigate Purview Scan API + BrainChild missing entities
===========================================================
1. Test every known scan API version + endpoint combination
2. Find where data sources live
3. Check BrainChild scan status & error details
4. Enumerate what BrainChild Fabric tables exist vs missing
5. Check if BrainChild workspace is accessible from Purview
"""
import json
import os
import sys

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
sess = requests.Session()

ACCT = "https://prviewacc.purview.azure.com"
TENANT_ID = "71c4b6d5-0065-4c6c-a125-841a582754eb"
TENANT_EP = f"https://{TENANT_ID}-api.purview-service.microsoft.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"
HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"

B = "\033[94m"
G = "\033[92m"
Y = "\033[93m"
R = "\033[91m"
C = "\033[96m"
D = "\033[2m"
BOLD = "\033[1m"
RST = "\033[0m"


def hdr(title):
    print(f"\n{BOLD}{B}{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}{RST}")


def get_headers(scope="purview"):
    if scope == "purview":
        token = cred.get_token("https://purview.azure.net/.default").token
    elif scope == "fabric":
        token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
    elif scope == "management":
        token = cred.get_token("https://management.azure.com/.default").token
    else:
        token = cred.get_token(scope).token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ══════════════════════════════════════════════════════════════════════
# 1. TEST ALL SCAN API ENDPOINT + VERSION COMBINATIONS
# ══════════════════════════════════════════════════════════════════════
def test_scan_apis():
    hdr("1. TEST SCAN API ENDPOINTS & VERSIONS")
    h = get_headers()

    endpoints = [
        ("TENANT_EP", TENANT_EP),
        ("ACCT/scan", f"{ACCT}/scan"),
        ("ACCT", ACCT),
    ]

    api_versions = [
        "2022-07-01-preview",
        "2022-02-01-preview",
        "2023-09-01",
        "2023-09-01-preview",
        "2024-03-01-preview",
        "2018-12-01-preview",
    ]

    for ep_name, ep_url in endpoints:
        for api_v in api_versions:
            url = f"{ep_url}/datasources?api-version={api_v}"
            try:
                r = sess.get(url, headers=h, timeout=15)
                status = r.status_code
                count = "?"
                if status == 200:
                    data = r.json()
                    count = len(data.get("value", []))
                    print(f"  {G}OK{RST}  {ep_name} + {api_v} -> {status} ({count} sources)")
                elif status == 404:
                    print(f"  {D}404{RST}  {ep_name} + {api_v}")
                else:
                    body_snip = r.text[:120] if r.text else ""
                    print(f"  {Y}{status}{RST}  {ep_name} + {api_v}: {body_snip}")
            except Exception as e:
                print(f"  {R}ERR{RST}  {ep_name} + {api_v}: {e}")


# ══════════════════════════════════════════════════════════════════════
# 2. LIST ALL DATA SOURCES (try working endpoint)
# ══════════════════════════════════════════════════════════════════════
def find_datasources():
    hdr("2. FIND DATA SOURCES (all known endpoints)")
    h = get_headers()

    # Try each combination until one works
    combos = [
        (f"{ACCT}/scan/datasources", "2022-07-01-preview"),
        (f"{ACCT}/scan/datasources", "2023-09-01"),
        (f"{ACCT}/scan/datasources", "2023-09-01-preview"),
        (f"{ACCT}/scan/datasources", "2024-03-01-preview"),
        (f"{TENANT_EP}/datasources", "2022-07-01-preview"),
        (f"{TENANT_EP}/datasources", "2023-09-01"),
    ]

    working_base = None
    working_api = None

    for base_url, api_v in combos:
        url = f"{base_url}?api-version={api_v}"
        try:
            r = sess.get(url, headers=h, timeout=15)
            if r.status_code == 200:
                data = r.json()
                sources = data.get("value", [])
                print(f"\n  {G}WORKING{RST}: {base_url}")
                print(f"  API version: {api_v}")
                print(f"  Data sources found: {len(sources)}")
                for ds in sources:
                    kind = ds.get("kind", "?")
                    name = ds.get("name", "?")
                    coll = ds.get("properties", {}).get("collection", {}).get("referenceName", "?")
                    print(f"    {C}{name}{RST} (kind={kind}, collection={coll})")
                    props = ds.get("properties", {})
                    for k, v in props.items():
                        if k not in ("collection",) and v:
                            print(f"      {k}: {v}")
                working_base = base_url.replace("/datasources", "")
                working_api = api_v
                break
        except Exception as e:
            print(f"  ERR: {base_url} + {api_v}: {e}")

    return working_base, working_api


# ══════════════════════════════════════════════════════════════════════
# 3. FIND SCANS (on working endpoint)
# ══════════════════════════════════════════════════════════════════════
def find_scans(base, api_v):
    hdr("3. LIST ALL SCANS")
    h = get_headers()

    if not base:
        print(f"  {R}No working scan endpoint found{RST}")
        return

    # First get datasource names
    r = sess.get(f"{base}/datasources?api-version={api_v}", headers=h, timeout=15)
    if r.status_code != 200:
        print(f"  {R}Cannot list datasources: {r.status_code}{RST}")
        return

    for ds in r.json().get("value", []):
        ds_name = ds["name"]
        print(f"\n  {BOLD}Datasource: {ds_name} (kind={ds.get('kind', '?')}){RST}")

        r2 = sess.get(f"{base}/datasources/{ds_name}/scans?api-version={api_v}",
                       headers=h, timeout=15)
        if r2.status_code != 200:
            print(f"    {R}Cannot list scans: {r2.status_code}{RST}")
            continue

        scans = r2.json().get("value", [])
        if not scans:
            print(f"    {Y}No scans configured{RST}")
            continue

        for scan in scans:
            scan_name = scan["name"]
            kind = scan.get("kind", "?")
            print(f"    {C}Scan: {scan_name}{RST} (kind={kind})")

            # Show scan properties
            scan_props = scan.get("properties", {})
            for k, v in scan_props.items():
                if v and k not in ("createdAt", "lastModifiedAt"):
                    val_str = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                    if len(val_str) > 100:
                        val_str = val_str[:100] + "..."
                    print(f"      {k}: {val_str}")

            # Check runs
            r3 = sess.get(
                f"{base}/datasources/{ds_name}/scans/{scan_name}/runs?api-version={api_v}",
                headers=h, timeout=15)
            if r3.status_code == 200:
                runs = r3.json().get("value", [])
                if runs:
                    for run in runs[:3]:
                        status = run.get("status", "?")
                        start = run.get("startTime", "?")
                        end = run.get("endTime", "?")
                        diag = run.get("diagnostics", {})
                        color = G if status == "Succeeded" else R if status == "Failed" else Y
                        print(f"      {color}Run: {status}{RST} | {start}")
                        if diag:
                            for k2, v2 in diag.items():
                                if v2:
                                    print(f"        {k2}: {v2}")
                        # If failed, get error detail
                        if status in ("Failed", "Canceled"):
                            err_msg = run.get("error", {})
                            if err_msg:
                                print(f"        {R}Error: {json.dumps(err_msg)[:200]}{RST}")
                else:
                    print(f"      {Y}No runs yet{RST}")


# ══════════════════════════════════════════════════════════════════════
# 4. CHECK BRAINCHILD ENTITIES IN PURVIEW (what exists vs expected)
# ══════════════════════════════════════════════════════════════════════
def check_brainchild_entities():
    hdr("4. BRAINCHILD ENTITIES IN PURVIEW")
    h = get_headers()

    # Expected BrainChild tables
    expected_bc_tables = [
        "brainchild_bronze_dicom_study",
        "brainchild_bronze_dicom_series",
        "brainchild_bronze_dicom_instance",
        "brainchild_silver_dicom_studies",
        "brainchild_silver_dicom_series",
        "brainchild_silver_dicom_pathology",
        # These might be missing:
        "gene_sequence",
        "variant_occurrence",
        "sbcr_registrations",
        "sbcr_treatments",
        "sbcr_followup",
        "patients_master",
        "specimen",
    ]

    # Search for ALL fabric lakehouse tables
    print(f"\n  {BOLD}All Fabric Lakehouse Tables in Purview:{RST}")
    body = {"keywords": "*", "filter": {"entityType": "fabric_lakehouse_table"}, "limit": 100}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    all_fabric_tables = {}
    if r.status_code == 200:
        results = r.json().get("value", [])
        for ent in results:
            name = ent.get("name", "?")
            guid = ent.get("id", "?")
            qname = ent.get("qualifiedName", "")
            desc = ent.get("description", "")
            # Detect which workspace
            is_bc = "brainchild" in qname.lower() or "brainchild" in name.lower() or BC_WS in qname
            is_hca = HCA_WS in qname
            ws_tag = f"{C}[BC]{RST}" if is_bc else f"{G}[HCA]{RST}" if is_hca else "[?]"
            all_fabric_tables[name] = {"guid": guid, "qname": qname, "is_bc": is_bc}
            has_desc = f" ({D}has desc{RST})" if desc else ""
            print(f"    {ws_tag} {name}{has_desc}")

    # Check expected BC tables
    print(f"\n  {BOLD}Expected BrainChild tables:{RST}")
    for tbl in expected_bc_tables:
        if tbl in all_fabric_tables:
            entry = all_fabric_tables[tbl]
            if entry["is_bc"]:
                print(f"    {G}OK{RST}  {tbl} (BrainChild workspace)")
            else:
                print(f"    {Y}OK{RST}  {tbl} (but in HCA workspace, not BrainChild)")
        else:
            print(f"    {R}MISS{RST} {tbl}")

    # Search for BrainChild lakehouses
    print(f"\n  {BOLD}BrainChild Lakehouses:{RST}")
    body = {"keywords": "brainchild", "filter": {"entityType": "fabric_lake_warehouse"}, "limit": 10}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        for ent in r.json().get("value", []):
            name = ent.get("name", "?")
            guid = ent.get("id", "?")
            qname = ent.get("qualifiedName", "")
            print(f"    {C}{name}{RST} (guid={guid[:12]}...)")
            print(f"      qname: {qname[:120]}")

    # Also search for all lakehouses
    print(f"\n  {BOLD}All Lakehouses:{RST}")
    body = {"keywords": "*", "filter": {"entityType": "fabric_lake_warehouse"}, "limit": 50}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        for ent in r.json().get("value", []):
            name = ent.get("name", "?")
            guid = ent.get("id", "?")
            qname = ent.get("qualifiedName", "")
            is_bc = BC_WS in qname
            ws_tag = f"{C}[BC]{RST}" if is_bc else f"{G}[HCA]{RST}" if HCA_WS in qname else "[?]"
            print(f"    {ws_tag} {name} (guid={guid[:12]}...)")


# ══════════════════════════════════════════════════════════════════════
# 5. CHECK FABRIC WORKSPACE DIRECTLY (what tables exist in lh_brainchild)
# ══════════════════════════════════════════════════════════════════════
def check_fabric_workspace():
    hdr("5. FABRIC WORKSPACE — ACTUAL TABLES IN LH_BRAINCHILD")
    h = get_headers("fabric")

    # List lakehouses in BrainChild workspace
    print(f"\n  {BOLD}BrainChild workspace lakehouses:{RST}")
    r = sess.get(f"https://api.fabric.microsoft.com/v1/workspaces/{BC_WS}/lakehouses",
                  headers=h, timeout=30)
    if r.status_code != 200:
        print(f"  {R}Cannot list lakehouses: {r.status_code}{RST}")
        print(f"  Body: {r.text[:300]}")
        return

    lakehouses = r.json().get("value", [])
    for lh in lakehouses:
        lh_name = lh.get("displayName", "?")
        lh_id = lh.get("id", "?")
        print(f"    {C}{lh_name}{RST} (id={lh_id})")

        # List tables in this lakehouse
        r2 = sess.get(
            f"https://api.fabric.microsoft.com/v1/workspaces/{BC_WS}/lakehouses/{lh_id}/tables",
            headers=h, timeout=30)
        if r2.status_code == 200:
            tables = r2.json().get("data", [])
            print(f"      Tables ({len(tables)}):")
            for t in tables:
                t_name = t.get("name", "?")
                t_type = t.get("type", "?")
                t_fmt = t.get("format", "?")
                loc = t.get("location", "")
                print(f"        {G}{t_name}{RST} ({t_type}/{t_fmt})")
        else:
            print(f"      {R}Cannot list tables: {r2.status_code}{RST}")
            print(f"      {r2.text[:200]}")

    # Also check HCA workspace lakehouses for comparison
    print(f"\n  {BOLD}HCA workspace lakehouses:{RST}")
    r = sess.get(f"https://api.fabric.microsoft.com/v1/workspaces/{HCA_WS}/lakehouses",
                  headers=h, timeout=30)
    if r.status_code == 200:
        for lh in r.json().get("value", []):
            lh_name = lh.get("displayName", "?")
            lh_id = lh.get("id", "?")
            print(f"    {C}{lh_name}{RST} (id={lh_id})")
            r2 = sess.get(
                f"https://api.fabric.microsoft.com/v1/workspaces/{HCA_WS}/lakehouses/{lh_id}/tables",
                headers=h, timeout=30)
            if r2.status_code == 200:
                tables = r2.json().get("data", [])
                print(f"      Tables ({len(tables)}): {', '.join(t['name'] for t in tables)}")


# ══════════════════════════════════════════════════════════════════════
# 6. ENTITY DETAIL — check a BC entity in Purview for collection info
# ══════════════════════════════════════════════════════════════════════
def check_bc_entity_details():
    hdr("6. BRAINCHILD ENTITY DETAILS (collection assignment)")
    h = get_headers()

    # Search for brainchild entities
    body = {"keywords": "brainchild", "limit": 10}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code != 200:
        print(f"  {R}Search failed: {r.status_code}{RST}")
        return

    results = r.json().get("value", [])
    for ent in results[:5]:
        name = ent.get("name", "?")
        guid = ent.get("id", "?")
        etype = ent.get("entityType", "?")
        collection = ent.get("collectionId", "?")
        print(f"\n  {C}{name}{RST} (type={etype})")
        print(f"    guid: {guid}")
        print(f"    collectionId: {collection}")

        # Get full entity
        r2 = sess.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=30)
        if r2.status_code == 200:
            entity = r2.json().get("entity", {})
            attrs = entity.get("attributes", {})
            desc = attrs.get("description", attrs.get("userDescription", ""))
            if desc:
                print(f"    description: {desc[:80]}")
            coll_id = entity.get("collectionId", "?")
            print(f"    atlas collectionId: {coll_id}")

            # Check labels
            labels = entity.get("labels", [])
            if labels:
                print(f"    labels: {labels}")

            # Check meanings (term assignments)
            meanings = entity.get("relationshipAttributes", {}).get("meanings", [])
            if meanings:
                term_names = [m.get("displayText", "?") for m in meanings]
                print(f"    terms: {term_names}")


# ══════════════════════════════════════════════════════════════════════
# 7. CHECK PURVIEW MSI ACCESS TO BRAINCHILD WORKSPACE
# ══════════════════════════════════════════════════════════════════════
def check_purview_msi_access():
    hdr("7. PURVIEW MSI ACCESS CHECK")
    h = get_headers("fabric")

    # Check if current user can see BrainChild workspace
    print(f"  Checking access to BrainChild workspace ({BC_WS})...")
    r = sess.get(f"https://api.fabric.microsoft.com/v1/workspaces/{BC_WS}", headers=h, timeout=15)
    if r.status_code == 200:
        ws = r.json()
        print(f"  {G}OK{RST} Workspace: {ws.get('displayName', '?')}")
        print(f"    capacityId: {ws.get('capacityId', '?')}")
    else:
        print(f"  {R}FAIL{RST} Cannot access workspace: {r.status_code}")
        print(f"  Body: {r.text[:200]}")

    # Check workspace role assignments
    print(f"\n  Checking Purview MSI in workspace role assignments...")
    r = sess.get(f"https://api.fabric.microsoft.com/v1/workspaces/{BC_WS}/roleAssignments",
                  headers=h, timeout=15)
    if r.status_code == 200:
        roles = r.json().get("value", [])
        print(f"  Role assignments ({len(roles)}):")
        for role in roles:
            principal = role.get("principal", {})
            p_name = principal.get("displayName", "?")
            p_type = principal.get("type", "?")
            p_id = principal.get("id", "?")
            r_name = role.get("role", "?")
            is_purview = "prview" in p_name.lower() or "purview" in p_name.lower()
            marker = f" {C}<-- PURVIEW{RST}" if is_purview else ""
            print(f"    {p_name} ({p_type}) -> {r_name} [id={p_id[:12]}...]{marker}")
    else:
        print(f"  {Y}Cannot list roles: {r.status_code}{RST}")


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"{BOLD}{B}{'=' * 70}")
    print(f"  PURVIEW SCAN & BRAINCHILD INVESTIGATION")
    print(f"{'=' * 70}{RST}")

    # Step 1: Find working scan API
    test_scan_apis()

    # Step 2: Find data sources
    working_base, working_api = find_datasources()

    # Step 3: List scans + runs + errors
    find_scans(working_base, working_api)

    # Step 4: BrainChild entities in Purview
    check_brainchild_entities()

    # Step 5: Actual tables in Fabric workspace
    check_fabric_workspace()

    # Step 6: Entity details for BC entities
    check_bc_entity_details()

    # Step 7: Purview MSI access
    check_purview_msi_access()

    print(f"\n{BOLD}{B}{'=' * 70}")
    print(f"  INVESTIGATION COMPLETE")
    print(f"{'=' * 70}{RST}")
