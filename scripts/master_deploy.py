"""
Master Deployment Orchestrator — Healthcare Analytics & BrainChild Demo Platform.

Verifies and deploys all components in correct order:
  Phase 1: Azure SQL (schema + data)
  Phase 2: Fabric Workspaces (lakehouses, notebooks, pipelines)
  Phase 3: Run Medallion Pipeline (Bronze → Silver → Gold)
  Phase 4: Purview Governance (17-step rebuild)
  Phase 5: End-to-end validation

Usage:
  python scripts/master_deploy.py                    # Full deploy
  python scripts/master_deploy.py --phase 4          # Only Purview
  python scripts/master_deploy.py --skip-pipeline    # Skip running notebooks
  python scripts/master_deploy.py --verify-only      # Just verify state
"""
import argparse
import json
import struct
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pyodbc
import pandas as pd
import requests
from azure.identity import AzureCliCredential

# ── CONFIG ─────────────────────────────────────────────────────────
SERVER = "sql-hca-demo.database.windows.net"
DATABASE = "HealthcareAnalyticsDB"
SUBSCRIPTION = "5b44c9f3-bbe7-464c-aa3e-562726a12004"
HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"
FABRIC_API = "https://api.fabric.microsoft.com/v1"

PURVIEW_ACCT = "https://prviewacc.purview.azure.com"
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"

SQL_TABLES = {
    "hca.patients": 10000,
    "hca.encounters": 17292,
    "hca.diagnoses": 30297,
    "hca.vitals_labs": 48131,
    "hca.medications": 60563,
}

EXPECTED_HCA_ITEMS = {
    "Lakehouse": ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse", "gold_omop"],
    "Notebook": ["01_bronze_ingestion", "02_silver_features", "03_ml_training", "04_omop_transformation"],
    "DataPipeline": ["healthcare_etl_pipeline"],
}

EXPECTED_BC_ITEMS = {
    "Lakehouse": ["lh_brainchild"],
    "Notebook": ["01_load_omop_tables", "02_validate_data", "03_ingest_fhir_bronze",
                  "04_fhir_to_silver", "05_ingest_dicom_bronze", "06_dicom_to_silver"],
    "DataPipeline": ["pl_fhir_ingestion", "pl_brainchild_ingestion"],
}

SCRIPTS_DIR = Path(__file__).resolve().parent

cred = AzureCliCredential(process_timeout=30)


# ── UTILITIES ──────────────────────────────────────────────────────
class DeployStatus:
    """Track phase status for final summary."""

    def __init__(self):
        self.phases = {}
        self.start_time = datetime.now()

    def set(self, phase, status, detail=""):
        self.phases[phase] = {"status": status, "detail": detail}

    def print_summary(self):
        elapsed = datetime.now() - self.start_time
        print(f"\n{'═' * 70}")
        print(f"  DEPLOYMENT SUMMARY — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"  Total time: {elapsed.seconds // 60}m {elapsed.seconds % 60}s")
        print(f"{'═' * 70}")
        for name, info in self.phases.items():
            icon = {"OK": "✅", "SKIP": "⏭️ ", "FAIL": "❌", "WARN": "⚠️"}.get(info["status"], "  ")
            detail = f" — {info['detail']}" if info["detail"] else ""
            print(f"  {icon} {name}: {info['status']}{detail}")
        print(f"{'═' * 70}\n")


status = DeployStatus()


def sep(title):
    print(f"\n{'━' * 70}")
    print(f"  {title}")
    print(f"{'━' * 70}")


def get_sql_connection():
    token = cred.get_token("https://database.windows.net/.default")
    tb = token.token.encode("UTF-16-LE")
    ts = struct.pack(f"<I{len(tb)}s", len(tb), tb)
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};",
        attrs_before={1256: ts},
    )


def get_fabric_token():
    return cred.get_token("https://api.fabric.microsoft.com/.default").token


def get_purview_token():
    return cred.get_token("https://purview.azure.net/.default").token


# ── PHASE 1: AZURE SQL ────────────────────────────────────────────
def phase1_sql():
    sep("PHASE 1: AZURE SQL — Schema & Data")
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()

        # Check each table
        all_ok = True
        for table, expected in SQL_TABLES.items():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                actual = cursor.fetchone()[0]
                if actual >= expected * 0.95:  # Allow 5% tolerance
                    print(f"  ✅ {table}: {actual:,} rows (expected ~{expected:,})")
                else:
                    print(f"  ⚠️  {table}: {actual:,} rows (expected ~{expected:,})")
                    all_ok = False
            except pyodbc.Error as e:
                print(f"  ❌ {table}: {e}")
                all_ok = False

        # Check view
        try:
            cursor.execute("SELECT COUNT(*) FROM hca.vw_ml_encounters")
            vw = cursor.fetchone()[0]
            print(f"  ✅ hca.vw_ml_encounters: {vw:,} rows")
        except pyodbc.Error:
            print(f"  ❌ hca.vw_ml_encounters: missing")
            all_ok = False

        conn.close()

        if all_ok:
            status.set("Phase 1: Azure SQL", "OK", f"{sum(SQL_TABLES.values()):,} rows across 5 tables + view")
            return True
        else:
            # Redeploy
            print("\n  📦 Redeploying SQL schema and data...")
            result = subprocess.run(
                [sys.executable, str(SCRIPTS_DIR / "deploy_sql_schema.py")],
                capture_output=True, text=True, cwd=str(SCRIPTS_DIR.parent)
            )
            if result.returncode == 0:
                status.set("Phase 1: Azure SQL", "OK", "Redeployed")
                return True
            else:
                print(f"  ❌ deploy_sql_schema.py failed:\n{result.stderr[:500]}")
                status.set("Phase 1: Azure SQL", "FAIL", "deploy_sql_schema.py failed")
                return False

    except Exception as e:
        print(f"  ❌ SQL connection failed: {e}")
        status.set("Phase 1: Azure SQL", "FAIL", str(e)[:100])
        return False


# ── PHASE 2: FABRIC WORKSPACES ────────────────────────────────────
def phase2_fabric():
    sep("PHASE 2: FABRIC WORKSPACES — Items & Configuration")
    token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}

    all_ok = True

    for ws_name, ws_id, expected_items in [
        ("Healthcare-Analytics", HCA_WS, EXPECTED_HCA_ITEMS),
        ("BrainChild-Demo", BC_WS, EXPECTED_BC_ITEMS),
    ]:
        print(f"\n  📂 {ws_name} ({ws_id[:8]}...)")
        r = requests.get(f"{FABRIC_API}/workspaces/{ws_id}/items", headers=headers)
        if r.status_code != 200:
            print(f"    ❌ Could not list items: {r.status_code}")
            all_ok = False
            continue

        items = r.json().get("value", [])
        item_index = {}
        for it in items:
            item_index.setdefault(it["type"], []).append(it["displayName"])

        for item_type, expected_names in expected_items.items():
            actual = item_index.get(item_type, [])
            for name in expected_names:
                if name in actual:
                    print(f"    ✅ {item_type}: {name}")
                else:
                    print(f"    ❌ {item_type}: {name} — MISSING")
                    all_ok = False

        # Show extra items (bonus)
        extra_types = {"SemanticModel", "MLExperiment", "MLModel", "SQLEndpoint"}
        for it in items:
            if it["type"] in extra_types:
                print(f"    🔹 {it['type']}: {it['displayName']}")

    if all_ok:
        status.set("Phase 2: Fabric Workspaces", "OK",
                    f"Healthcare ({len(EXPECTED_HCA_ITEMS)}+) + BrainChild ({len(EXPECTED_BC_ITEMS)}+)")
    else:
        status.set("Phase 2: Fabric Workspaces", "WARN", "Some items missing — may need manual fix")

    return all_ok


# ── PHASE 3: RUN MEDALLION PIPELINE ──────────────────────────────
def phase3_pipeline(skip=False):
    sep("PHASE 3: MEDALLION PIPELINE — Run Notebooks")

    if skip:
        print("  ⏭️  Skipped (--skip-pipeline)")
        status.set("Phase 3: Pipeline Run", "SKIP")
        return True

    # Check if lakehouses already have data
    token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}

    # Quick check: see if bronze_lakehouse has tables
    r = requests.get(
        f"{FABRIC_API}/workspaces/{HCA_WS}/lakehouses/e1f2c38d-3f87-48ed-9769-6d2c8de22595/tables",
        headers=headers
    )
    if r.status_code == 200:
        tables = r.json().get("data", [])
        if len(tables) >= 4:
            print(f"  ✅ Bronze lakehouse already has {len(tables)} tables — skipping pipeline run")
            print(f"     Tables: {', '.join(t['name'] for t in tables[:6])}")
            status.set("Phase 3: Pipeline Run", "SKIP", f"Data already present ({len(tables)} bronze tables)")
            return True

    # Run pipeline
    print("  🚀 Running notebooks sequentially...")
    result = subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / "run_pipeline.py")],
        capture_output=False, text=True, cwd=str(SCRIPTS_DIR.parent)
    )
    if result.returncode == 0:
        status.set("Phase 3: Pipeline Run", "OK")
        return True
    else:
        status.set("Phase 3: Pipeline Run", "FAIL", "run_pipeline.py failed")
        return False


# ── PHASE 4: PURVIEW GOVERNANCE ───────────────────────────────────
def phase4_purview():
    sep("PHASE 4: PURVIEW GOVERNANCE — Full Rebuild (17 Steps)")
    token = get_purview_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Check current state
    r = requests.post(
        f"{PURVIEW_ACCT}/catalog/api/search/query?api-version=2022-08-01-preview",
        headers=headers, json={"keywords": "*", "limit": 1}, timeout=30
    )
    assets = r.json().get("@search.count", 0) if r.status_code == 200 else -1

    r2 = requests.get(
        f"{SCAN_EP}/scan/datasources?api-version=2022-07-01-preview",
        headers=headers, timeout=30
    )
    sources = len(r2.json().get("value", [])) if r2.status_code == 200 else -1

    if assets > 50 and sources >= 2:
        print(f"  ✅ Purview already configured: {assets} assets, {sources} sources")
        status.set("Phase 4: Purview Governance", "SKIP", f"Already has {assets} assets")
        return True

    print(f"  Current state: {assets} assets, {sources} sources")
    print(f"  🚀 Running purview_rebuild.py (17 steps)...\n")

    result = subprocess.run(
        [sys.executable, str(SCRIPTS_DIR / "purview_rebuild.py")],
        capture_output=False, text=True, cwd=str(SCRIPTS_DIR.parent)
    )
    if result.returncode == 0:
        status.set("Phase 4: Purview Governance", "OK", "17-step rebuild completed")
        return True
    else:
        status.set("Phase 4: Purview Governance", "FAIL", "purview_rebuild.py error")
        return False


# ── PHASE 5: VALIDATION ──────────────────────────────────────────
def phase5_validate():
    sep("PHASE 5: END-TO-END VALIDATION")

    checks = {"sql": False, "fabric_hca": False, "fabric_bc": False, "purview": False}

    # SQL
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM hca.patients")
        cnt = cursor.fetchone()[0]
        checks["sql"] = cnt > 9000
        print(f"  {'✅' if checks['sql'] else '❌'} SQL: {cnt:,} patients")
        conn.close()
    except Exception as e:
        print(f"  ❌ SQL: {e}")

    # Fabric
    token = get_fabric_token()
    headers = {"Authorization": f"Bearer {token}"}
    for label, ws_id, key in [
        ("Healthcare-Analytics", HCA_WS, "fabric_hca"),
        ("BrainChild-Demo", BC_WS, "fabric_bc"),
    ]:
        r = requests.get(f"{FABRIC_API}/workspaces/{ws_id}/items", headers=headers)
        if r.status_code == 200:
            items = r.json().get("value", [])
            checks[key] = len(items) >= 5
            print(f"  {'✅' if checks[key] else '❌'} {label}: {len(items)} items")
        else:
            print(f"  ❌ {label}: API error {r.status_code}")

    # Purview
    ptk = get_purview_token()
    ph = {"Authorization": f"Bearer {ptk}", "Content-Type": "application/json"}
    r = requests.post(
        f"{PURVIEW_ACCT}/catalog/api/search/query?api-version=2022-08-01-preview",
        headers=ph, json={"keywords": "*", "limit": 1}, timeout=30
    )
    if r.status_code == 200:
        assets = r.json().get("@search.count", 0)
        # Also check glossary
        r2 = requests.get(f"{PURVIEW_ACCT}/catalog/api/atlas/v2/glossary", headers=ph, timeout=30)
        terms = 0
        if r2.status_code == 200:
            glossaries = r2.json()
            if isinstance(glossaries, list) and glossaries:
                terms = len(glossaries[0].get("terms", []))
            elif isinstance(glossaries, dict):
                terms = len(glossaries.get("terms", []))

        checks["purview"] = assets > 0 or terms > 0
        print(f"  {'✅' if checks['purview'] else '⏳'} Purview: {assets} assets, {terms} glossary terms")
        if assets == 0:
            print(f"     ℹ️  Assets appear after scans complete (2-5 min)")
    else:
        print(f"  ❌ Purview: API error {r.status_code}")

    ok = sum(1 for v in checks.values() if v)
    total = len(checks)
    if ok == total:
        status.set("Phase 5: Validation", "OK", f"{ok}/{total} checks passed")
    elif ok >= 3:
        status.set("Phase 5: Validation", "WARN", f"{ok}/{total} checks passed (Purview scans may be pending)")
    else:
        status.set("Phase 5: Validation", "FAIL", f"{ok}/{total} checks passed")

    return ok >= 3


# ── MAIN ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Master Deploy — Healthcare Analytics + BrainChild")
    parser.add_argument("--phase", type=int, help="Run only a specific phase (1-5)")
    parser.add_argument("--skip-pipeline", action="store_true", help="Skip pipeline execution")
    parser.add_argument("--verify-only", action="store_true", help="Only verify current state")
    args = parser.parse_args()

    print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║              MASTER DEPLOYMENT ORCHESTRATOR                        ║
║       Healthcare Analytics + BrainChild Demo Platform              ║
║              {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}                             ║
╚══════════════════════════════════════════════════════════════════════╝
""")

    if args.verify_only:
        phase1_sql()
        phase2_fabric()
        phase5_validate()
        status.print_summary()
        return

    phases = {
        1: lambda: phase1_sql(),
        2: lambda: phase2_fabric(),
        3: lambda: phase3_pipeline(skip=args.skip_pipeline),
        4: lambda: phase4_purview(),
        5: lambda: phase5_validate(),
    }

    if args.phase:
        if args.phase in phases:
            phases[args.phase]()
        else:
            print(f"Unknown phase {args.phase}. Valid: 1-5")
            sys.exit(1)
    else:
        for num, fn in phases.items():
            fn()

    status.print_summary()


if __name__ == "__main__":
    main()
