"""Validate the healthcare analytics deployment end-to-end."""
import json
import logging
import struct
import sys
from pathlib import Path

import pyodbc
import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DATABASE = "HealthcareAnalyticsDB"
FABRIC_WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
PURVIEW_SCAN_ENDPOINT = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"

EXPECTED_TABLES = {
    "hca.patients": 10000,
    "hca.encounters": 17292,
    "hca.diagnoses": 30297,
    "hca.vitals_labs": 48131,
    "hca.medications": 60563,
}

FABRIC_ITEMS = {
    "Lakehouse": ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse", "gold_omop"],
    "Notebook": ["01_bronze_ingestion", "02_silver_features", "03_ml_training", "04_omop_transformation"],
    "DataPipeline": ["healthcare_etl_pipeline"],
    "SemanticModel": ["OMOP_CDM_Semantic_Model"],
}


def check_sql(token=None):
    """Validate Azure SQL has data."""
    log.info("=== Azure SQL Validation ===")
    if token is None:
        cred = AzureCliCredential(process_timeout=30)
        token = cred.get_token("https://database.windows.net/.default").token
    token_bytes = token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};",
        attrs_before={1256: token_struct},
    )
    cursor = conn.cursor()
    ok = True
    for table, min_rows in EXPECTED_TABLES.items():
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        status = "✅" if count >= min_rows else "⚠️"
        if count == 0:
            status = "❌"
            ok = False
        log.info("  %s %s: %d rows (expected ≥%d)", status, table, count, min_rows)

    # Check view
    cursor.execute("SELECT COUNT(*) FROM hca.vw_ml_encounters")
    view_count = cursor.fetchone()[0]
    log.info("  %s hca.vw_ml_encounters: %d rows", "✅" if view_count > 0 else "❌", view_count)

    conn.close()
    return ok


def check_fabric(token=None):
    """Validate Fabric workspace items."""
    log.info("=== Fabric Workspace Validation ===")
    if token is None:
        cred = AzureCliCredential(process_timeout=30)
        token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {"Authorization": f"Bearer {token}"}

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{FABRIC_WORKSPACE_ID}/items"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        log.error("  ❌ Failed to list workspace items: %d", resp.status_code)
        return False

    items = resp.json().get("value", [])
    item_map = {}
    for item in items:
        item_type = item.get("type", "")
        item_name = item.get("displayName", "")
        item_map.setdefault(item_type, []).append(item_name)

    ok = True
    for item_type, expected_names in FABRIC_ITEMS.items():
        actual = item_map.get(item_type, [])
        for name in expected_names:
            found = name in actual
            status = "✅" if found else "❌"
            if not found:
                ok = False
            log.info("  %s %s/%s", status, item_type, name)

    return ok


def check_purview(token=None):
    """Validate Purview scan status."""
    log.info("=== Purview Validation ===")
    if token is None:
        cred = AzureCliCredential(process_timeout=30)
        token = cred.get_token("https://purview.azure.net/.default").token
    headers = {"Authorization": f"Bearer {token}"}

    # Check data source
    url = f"{PURVIEW_SCAN_ENDPOINT}/scan/datasources/sql-hca-demo?api-version=2023-09-01"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        log.info("  ✅ Data source: sql-hca-demo registered")
    else:
        log.error("  ❌ Data source not found")
        return False

    # Check scan
    scan_url = f"{PURVIEW_SCAN_ENDPOINT}/scan/datasources/sql-hca-demo/scans/healthcare-scan?api-version=2023-09-01"
    resp2 = requests.get(scan_url, headers=headers)
    if resp2.status_code == 200:
        log.info("  ✅ Scan: healthcare-scan configured")
    else:
        log.error("  ❌ Scan not found")
        return False

    # Check latest scan run
    runs_url = f"{PURVIEW_SCAN_ENDPOINT}/scan/datasources/sql-hca-demo/scans/healthcare-scan/runs?api-version=2023-09-01"
    resp3 = requests.get(runs_url, headers=headers)
    if resp3.status_code == 200:
        runs = resp3.json().get("value", [])
        if runs:
            latest = runs[0]
            status = latest.get("status", "Unknown")
            log.info("  ✅ Latest scan run: %s", status)
        else:
            log.info("  ⚠️ No scan runs found")

    return True


def main():
    log.info("Healthcare Analytics — Deployment Validation")
    log.info("=" * 50)

    # Pre-fetch all tokens with a single credential to avoid timeouts
    cred = AzureCliCredential(process_timeout=30)
    tokens = {}
    for scope_name, scope in [("sql", "https://database.windows.net/.default"),
                               ("fabric", "https://api.fabric.microsoft.com/.default"),
                               ("purview", "https://purview.azure.net/.default")]:
        try:
            tokens[scope_name] = cred.get_token(scope).token
            log.info("  Token acquired: %s", scope_name)
        except Exception as e:
            log.warning("  Token failed for %s: %s", scope_name, e)
            tokens[scope_name] = None

    results = {}
    results["sql"] = check_sql(token=tokens.get("sql"))
    results["fabric"] = check_fabric(token=tokens.get("fabric"))
    results["purview"] = check_purview(token=tokens.get("purview"))

    log.info("")
    log.info("=== SUMMARY ===")
    all_ok = True
    for component, ok in results.items():
        status = "✅ PASS" if ok else "❌ FAIL"
        log.info("  %s: %s", component.upper(), status)
        if not ok:
            all_ok = False

    if all_ok:
        log.info("\n🎉 All validations passed!")
    else:
        log.info("\n⚠️ Some validations had issues. Check logs above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
