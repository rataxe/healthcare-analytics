"""
Fix Fabric Lineage in Purview
==============================
Diagnoses and fixes why Fabric items show no lineage in Purview.

Root causes addressed:
  1. Scan scope missing Notebook + DataPipeline (lineage producers)
  2. Wrong scan kind — must be FabricMsi (not PowerBIMsiScan)
  3. Purview MSI needs workspace access in Fabric
  4. Fabric admin API setting for metadata access
  5. Re-trigger scans after fix

Usage:
  python scripts/fix_fabric_lineage.py              # Diagnose + fix
  python scripts/fix_fabric_lineage.py --diagnose   # Diagnose only
"""
import argparse
import json
import sys
import time

import requests
from azure.identity import AzureCliCredential
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── CONFIG ──
cred = AzureCliCredential(process_timeout=30)

SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ACCT = "https://prviewacc.purview.azure.com"
FABRIC_API = "https://api.fabric.microsoft.com/v1"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
SCAN_API = "2023-09-01"

HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"

PURVIEW_MSI_NAME = "prviewacc"  # Purview account name = MSI display name

sess = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
sess.mount("https://", HTTPAdapter(max_retries=retry))


def get_purview_headers():
    token = cred.get_token("https://purview.azure.net/.default").token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def get_fabric_headers():
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def ok(msg):
    print(f"  ✅ {msg}")


def warn(msg):
    print(f"  ⚠️  {msg}")


def fail(msg):
    print(f"  ❌ {msg}")


def info(msg):
    print(f"  ℹ️  {msg}")


def sep(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


# ══════════════════════════════════════════════════════════════════════
# STEP 1: DIAGNOSE — Check current scan configuration
# ══════════════════════════════════════════════════════════════════════
def diagnose_scans(h):
    sep("1. DIAGNOSE — Current Fabric scan configuration")

    issues = []

    # List all scans under Fabric datasource
    r = sess.get(
        f"{SCAN_EP}/scan/datasources/Fabric/scans?api-version={SCAN_API}",
        headers=h, timeout=30
    )
    if r.status_code != 200:
        fail(f"Cannot list Fabric scans: {r.status_code}")
        return issues

    scans = r.json().get("value", [])
    if not scans:
        fail("No Fabric scans found — lineage requires scans")
        issues.append("no_scans")
        return issues

    for scan in scans:
        name = scan.get("name", "?")
        kind = scan.get("kind", "?")
        props = scan.get("properties", {})
        scope = props.get("scanScope", {})
        scope_type = scope.get("scopeType", "not set")
        fabric_items = scope.get("fabricItems", [])
        coll = props.get("collection", {}).get("referenceName", "?")

        print(f"\n  --- Scan: {name} ---")
        print(f"    Kind:       {kind}")
        print(f"    Collection: {coll}")
        print(f"    ScopeType:  {scope_type}")

        # Check 1: Scan kind
        if kind not in ("FabricMsi", "FabricMsiScan"):
            fail(f"    Scan kind '{kind}' does not support Fabric lineage")
            info(f"    Must be 'FabricMsi' for lineage extraction")
            issues.append(f"wrong_kind:{name}")
        else:
            ok(f"    Scan kind '{kind}' supports lineage")

        # Check 2: Resource types in scope
        if fabric_items:
            for item in fabric_items:
                ws_id = item.get("workspaceId", "?")
                res_types = item.get("resourceTypes", [])
                print(f"    Workspace:  {ws_id}")
                print(f"    Resources:  {res_types}")

                lineage_types = {"Notebook", "DataPipeline"}
                have_lineage_types = lineage_types.intersection(set(res_types))
                missing = lineage_types - set(res_types)

                if missing:
                    fail(f"    Missing lineage-producing resource types: {missing}")
                    issues.append(f"missing_types:{name}")
                else:
                    ok(f"    Has lineage-producing types: {have_lineage_types}")

                # Also check for full coverage
                recommended = {"Lakehouse", "SemanticModel", "Notebook",
                               "DataPipeline", "SQLEndpoint", "SparkJobDefinition"}
                extra_missing = recommended - set(res_types)
                if extra_missing:
                    warn(f"    Recommended types not in scope: {extra_missing}")
        else:
            if scope_type == "Workspace":
                ok(f"    Full workspace scope — all resource types included")
            else:
                warn(f"    No fabricItems defined and scopeType='{scope_type}'")
                issues.append(f"no_scope:{name}")

        # Check 3: Latest scan run
        r2 = sess.get(
            f"{SCAN_EP}/scan/datasources/Fabric/scans/{name}/runs?api-version={SCAN_API}",
            headers=h, timeout=30
        )
        if r2.status_code == 200:
            runs = r2.json().get("value", [])
            if runs:
                latest = runs[0]
                status = latest.get("status", "?")
                print(f"    Last run:   {status}")
                if status == "Failed":
                    err = latest.get("error", {})
                    fail(f"    Error: {err.get('message', 'unknown')[:200]}")
                    issues.append(f"scan_failed:{name}")
            else:
                warn(f"    No scan runs found")
                issues.append(f"no_runs:{name}")

    return issues


# ══════════════════════════════════════════════════════════════════════
# STEP 2: DIAGNOSE — Check Fabric workspace access for Purview MSI
# ══════════════════════════════════════════════════════════════════════
def diagnose_workspace_access(fh):
    sep("2. DIAGNOSE — Purview MSI workspace access")

    issues = []
    for ws_name, ws_id in [("Healthcare-Analytics", HCA_WS), ("BrainChild-Demo", BC_WS)]:
        r = sess.get(
            f"{FABRIC_API}/workspaces/{ws_id}/roleAssignments",
            headers=fh, timeout=30
        )
        if r.status_code == 200:
            roles = r.json().get("value", [])
            purview_access = None
            for role in roles:
                principal = role.get("principal", {})
                disp = principal.get("displayName", "")
                pid = principal.get("id", "")
                if PURVIEW_MSI_NAME.lower() in disp.lower() or "purview" in disp.lower():
                    purview_access = role.get("role", "?")
                    break

            if purview_access:
                ok(f"{ws_name}: Purview MSI has '{purview_access}' role")
            else:
                fail(f"{ws_name}: Purview MSI has NO workspace access")
                info(f"  Purview MSI must be Admin/Member/Contributor for lineage")
                issues.append(f"no_ws_access:{ws_id}")
        elif r.status_code == 401:
            warn(f"{ws_name}: Cannot check roles (no admin access via current identity)")
        else:
            warn(f"{ws_name}: Role check returned {r.status_code}")

    return issues


# ══════════════════════════════════════════════════════════════════════
# STEP 3: DIAGNOSE — Check if lineage entities exist
# ══════════════════════════════════════════════════════════════════════
def diagnose_lineage_entities(h):
    sep("3. DIAGNOSE — Lineage entities in catalog")

    # Search for process entities (lineage connectors)
    for entity_type in ["powerbi_dataset_process", "powerbi_dataflow",
                        "fabric_notebook", "fabric_pipeline",
                        "purview_custom_connector_generic_entity_with_columns"]:
        body = {
            "keywords": "*",
            "filter": {"entityType": entity_type},
            "limit": 5,
        }
        r = sess.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            count = r.json().get("@search.count", 0)
            if count > 0:
                ok(f"Found {count} '{entity_type}' entities (lineage sources)")
            else:
                warn(f"No '{entity_type}' entities found — no lineage from these")
        time.sleep(0.3)

    # Search for Fabric items broadly
    for kw in ["notebook", "pipeline", "lakehouse"]:
        body = {"keywords": kw, "limit": 5}
        r = sess.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            count = r.json().get("@search.count", 0)
            results = r.json().get("value", [])
            types = set(a.get("entityType", "?") for a in results[:5])
            info(f"'{kw}': {count} assets (types: {types})")
        time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════
# STEP 4: FIX — Recreate Fabric scans with correct config
# ══════════════════════════════════════════════════════════════════════
def fix_fabric_scans(h):
    sep("4. FIX — Recreate Fabric scans with lineage-capable config")

    # All resource types needed for full lineage extraction
    all_resource_types = [
        "Lakehouse",
        "SemanticModel",
        "Notebook",
        "DataPipeline",
        "SQLEndpoint",
        "SparkJobDefinition",
    ]

    # First, discover existing scan names
    existing_scans = {}
    r = sess.get(
        f"{SCAN_EP}/scan/datasources/Fabric/scans?api-version={SCAN_API}",
        headers=h, timeout=30
    )
    if r.status_code == 200:
        for s in r.json().get("value", []):
            name = s.get("name", "")
            coll = s.get("properties", {}).get("collection", {}).get("referenceName", "")
            existing_scans[coll] = name
            info(f"Existing scan: {name} → collection={coll}")

    # Use existing scan name for HCA if found, otherwise create new
    hca_scan_name = existing_scans.get("fabric-analytics", "Scan-HCA")
    bc_scan_name = existing_scans.get("fabric-brainchild", "Scan-BrainChild")

    scans_to_fix = [
        {
            "name": hca_scan_name,
            "collection": "fabric-analytics",
            "workspace_id": HCA_WS,
            "label": "Healthcare-Analytics",
        },
        {
            "name": bc_scan_name,
            "collection": "fabric-brainchild",
            "workspace_id": BC_WS,
            "label": "BrainChild-Demo",
        },
    ]

    # Try FabricMsi first (preferred for lineage), fallback to PowerBIMsiScan
    for scan_cfg in scans_to_fix:
        created = False

        for scan_kind in ["FabricMsi", "FabricMsiScan", "PowerBIMsiScan"]:
            body = {
                "kind": scan_kind,
                "name": scan_cfg["name"],
                "properties": {
                    "collection": {
                        "referenceName": scan_cfg["collection"],
                        "type": "CollectionReference",
                    },
                    "scanScope": {
                        "fabricItems": [
                            {
                                "workspaceId": scan_cfg["workspace_id"],
                                "resourceTypes": all_resource_types,
                            }
                        ],
                    },
                },
            }

            # For PowerBI scan kinds, use different property structure
            if scan_kind in ("PowerBIMsiScan", "PowerBIDelegatedScan"):
                body["properties"] = {
                    "includePersonalWorkspaces": False,
                    "collection": {
                        "referenceName": scan_cfg["collection"],
                        "type": "CollectionReference",
                    },
                }

            url = f"{SCAN_EP}/scan/datasources/Fabric/scans/{scan_cfg['name']}?api-version={SCAN_API}"
            r = sess.put(url, headers=h, json=body, timeout=30)

            if r.status_code in (200, 201):
                ok(f"{scan_cfg['label']}: Scan '{scan_cfg['name']}' updated with kind={scan_kind}")
                ok(f"  Resource types: {all_resource_types}")
                created = True
                break
            else:
                info(f"{scan_cfg['label']}: kind={scan_kind} → {r.status_code} (trying next)")

        if not created:
            fail(f"{scan_cfg['label']}: Could not create scan with any kind")
            info(f"  Last error: {r.text[:200]}")

    return True


# ══════════════════════════════════════════════════════════════════════
# STEP 5: FIX — Add Purview MSI to Fabric workspaces
# ══════════════════════════════════════════════════════════════════════
def fix_workspace_access(fh):
    sep("5. FIX — Add Purview MSI to Fabric workspaces")

    # First, get the Purview MSI object ID
    # The MSI service principal has the same name as the Purview account
    graph_token = cred.get_token("https://graph.microsoft.com/.default").token
    graph_h = {"Authorization": f"Bearer {graph_token}", "Content-Type": "application/json"}

    msi_object_id = None
    r = sess.get(
        f"https://graph.microsoft.com/v1.0/servicePrincipals?$filter=displayName eq '{PURVIEW_MSI_NAME}'&$select=id,displayName,appId",
        headers=graph_h, timeout=30
    )
    if r.status_code == 200:
        sps = r.json().get("value", [])
        if sps:
            msi_object_id = sps[0]["id"]
            ok(f"Purview MSI found: {sps[0]['displayName']} (id={msi_object_id[:12]}...)")
        else:
            fail(f"Purview MSI '{PURVIEW_MSI_NAME}' not found in Azure AD")
            info("Check that the Purview account name matches the MSI display name")
            return
    else:
        warn(f"Graph API returned {r.status_code} — trying with known name")
        # Fallback: try to add by display name pattern
        msi_object_id = None

    if not msi_object_id:
        fail("Cannot determine Purview MSI object ID")
        info("Manually add the Purview MSI to workspaces:")
        info(f"  1. Go to app.fabric.microsoft.com")
        info(f"  2. Open each workspace → Manage access")
        info(f"  3. Add '{PURVIEW_MSI_NAME}' as Admin or Contributor")
        return

    # Add MSI to each workspace as Admin (needed for full lineage)
    for ws_name, ws_id in [("Healthcare-Analytics", HCA_WS), ("BrainChild-Demo", BC_WS)]:
        body = {
            "principal": {
                "id": msi_object_id,
                "type": "ServicePrincipal",
            },
            "role": "Admin",
        }

        r = sess.post(
            f"{FABRIC_API}/workspaces/{ws_id}/roleAssignments",
            headers=fh, json=body, timeout=30
        )
        if r.status_code in (200, 201):
            ok(f"{ws_name}: Purview MSI added as Admin")
        elif r.status_code == 409:
            ok(f"{ws_name}: Purview MSI already has access")
        elif r.status_code == 400:
            # Might already be assigned — try PATCH to update role
            r2 = sess.patch(
                f"{FABRIC_API}/workspaces/{ws_id}/roleAssignments/{msi_object_id}",
                headers=fh, json={"role": "Admin"}, timeout=30
            )
            if r2.status_code in (200, 201):
                ok(f"{ws_name}: Purview MSI role updated to Admin")
            else:
                warn(f"{ws_name}: Could not set role: {r2.status_code}")
        else:
            warn(f"{ws_name}: {r.status_code} — {r.text[:200]}")

    print()
    info("Fabric admin setting also required:")
    info("  1. Go to admin.fabric.microsoft.com → Tenant settings")
    info("  2. Enable 'Allow service principals to use Fabric APIs'")
    info("  3. Enable 'Allow Azure Active Directory to discover Fabric metadata'")
    info("  4. Ensure the Purview MSI security group is allowed")


# ══════════════════════════════════════════════════════════════════════
# STEP 6: FIX — Trigger new scans
# ══════════════════════════════════════════════════════════════════════
def trigger_scans(h):
    sep("6. TRIGGER — Re-run Fabric scans for lineage extraction")

    # Discover scan names dynamically
    scan_names = []
    r = sess.get(
        f"{SCAN_EP}/scan/datasources/Fabric/scans?api-version={SCAN_API}",
        headers=h, timeout=30
    )
    if r.status_code == 200:
        scan_names = [s["name"] for s in r.json().get("value", [])]
    if not scan_names:
        scan_names = ["Scan-IzR", "Scan-BrainChild"]

    for scan_name in scan_names:
        run_id = f"run-lineage-{int(time.time())}"
        r = sess.post(
            f"{SCAN_EP}/scan/datasources/Fabric/scans/{scan_name}/runs/{run_id}?api-version={SCAN_API}",
            headers=h, json={}, timeout=30
        )
        if r.status_code in (200, 201, 202):
            ok(f"{scan_name}: Scan triggered ({run_id})")
        else:
            warn(f"{scan_name}: {r.status_code} — {r.text[:200]}")
        time.sleep(1)

    print()
    info("Scans take 3-10 minutes. Lineage appears after scan completes.")
    info("Check status: python scripts/purview_scan_detail.py")


# ══════════════════════════════════════════════════════════════════════
# STEP 7: MONITOR — Wait and check scan status
# ══════════════════════════════════════════════════════════════════════
def monitor_scans(h, timeout_sec=300):
    sep("7. MONITOR — Waiting for scans to complete")

    # Discover scan names dynamically
    scan_names = []
    r = sess.get(
        f"{SCAN_EP}/scan/datasources/Fabric/scans?api-version={SCAN_API}",
        headers=h, timeout=30
    )
    if r.status_code == 200:
        scan_names = [s["name"] for s in r.json().get("value", [])]
    if not scan_names:
        scan_names = ["Scan-IzR", "Scan-BrainChild"]

    waited = 0

    while waited < timeout_sec:
        all_done = True
        statuses = []

        for name in scan_names:
            r = sess.get(
                f"{SCAN_EP}/scan/datasources/Fabric/scans/{name}/runs?api-version={SCAN_API}",
                headers=h, timeout=30
            )
            if r.status_code == 200:
                runs = r.json().get("value", [])
                if runs:
                    s = runs[0].get("status", "Unknown")
                    statuses.append(f"{name}: {s}")
                    if s not in ("Succeeded", "Failed", "Canceled"):
                        all_done = False
                else:
                    statuses.append(f"{name}: no runs")
                    all_done = False

        elapsed = f"{waited // 60}m{waited % 60:02d}s"
        print(f"  [{elapsed}] {' | '.join(statuses)}")

        if all_done:
            break

        time.sleep(30)
        waited += 30

    if not all_done:
        warn("Scans still running — check back in a few minutes")

    # Final lineage check
    print()
    diagnose_lineage_entities(h)


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Fix Fabric Lineage in Purview")
    parser.add_argument("--diagnose", action="store_true", help="Diagnose only, don't fix")
    parser.add_argument("--no-wait", action="store_true", help="Don't wait for scans to complete")
    args = parser.parse_args()

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║        PURVIEW — FIX FABRIC LINEAGE                        ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    ph = get_purview_headers()
    fh = get_fabric_headers()

    # Always diagnose first
    scan_issues = diagnose_scans(ph)
    ws_issues = diagnose_workspace_access(fh)
    diagnose_lineage_entities(ph)

    all_issues = scan_issues + ws_issues

    if args.diagnose:
        sep("DIAGNOSIS SUMMARY")
        if all_issues:
            print(f"\n  Found {len(all_issues)} issues:")
            for issue in all_issues:
                print(f"    • {issue}")
            print(f"\n  Run without --diagnose to fix automatically")
        else:
            ok("No obvious issues found")
            info("If lineage still missing, check Fabric admin tenant settings:")
            info("  admin.fabric.microsoft.com → Tenant settings →")
            info("  'Allow service principals to use Fabric APIs'")
            info("  'Service principals can access read-only admin APIs'")
        return

    # Fix
    if any("wrong_kind" in i or "missing_types" in i or "no_scope" in i for i in scan_issues):
        fix_fabric_scans(ph)

    if any("no_ws_access" in i for i in ws_issues):
        fix_workspace_access(fh)

    # Even if no issues detected, re-apply scan config (idempotent)
    if not scan_issues:
        info("No scan issues detected, but re-applying optimal config anyway")
        fix_fabric_scans(ph)

    trigger_scans(ph)

    if not args.no_wait:
        monitor_scans(ph)

    sep("DONE")
    print("""
  Next steps if lineage still missing:
  
  1. Verify Fabric Admin tenant settings:
     → admin.fabric.microsoft.com → Tenant settings
     → "Allow service principals to use Fabric APIs" = Enabled
     → "Service principals can access read-only admin APIs" = Enabled  
     → "Allow XMLA endpoints and Analyze in Excel" = Enabled
     → "Enhance admin APIs responses with detailed metadata" = Enabled
  
  2. Verify Purview MSI has workspace access:
     → app.fabric.microsoft.com → Workspace → Manage access
     → Add 'prviewacc' as Admin
  
  3. Re-run scans after admin settings change:
     → python scripts/fix_fabric_lineage.py
  
  4. Lineage appears 5-15 min after successful scan
""")


if __name__ == "__main__":
    main()
