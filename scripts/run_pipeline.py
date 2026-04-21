"""Run the Healthcare-Analytics pipeline in Fabric and monitor its progress."""
import json
import logging
import time

import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
PIPELINE_ID = "a163c4c5-376b-449a-9cad-50d45194370d"

# Notebook IDs for sequential execution
NOTEBOOKS = {
    "01_bronze_ingestion": "41378393-200e-4cb7-8887-2209104393d6",
    "02_silver_features": "a65f0278-9dc0-402c-a1aa-c49c3e424a8f",
    "03_ml_training": "094a9e43-55f0-4f11-a36f-fede8515d46c",
    "04_omop_transformation": "c2c2a2f7-3d71-490e-94a4-1b42a9787c25",
}

LAKEHOUSES = {
    "bronze_lakehouse": "e1f2c38d-3f87-48ed-9769-6d2c8de22595",
    "silver_lakehouse": "270a6614-2a07-463d-94de-0c55b26ec6de",
    "gold_lakehouse": "2960eef0-5de6-4117-80b1-6ee783cdaeec",
    "gold_omop": "8adebdbb-1cde-40e8-bcde-1691f0e7e2b2",
}


def get_token():
    cred = AzureCliCredential(process_timeout=30)
    for attempt in range(3):
        try:
            return cred.get_token("https://api.fabric.microsoft.com/.default").token
        except Exception as e:
            if attempt < 2:
                log.warning("Token attempt %d failed, retrying in 5s: %s", attempt + 1, e)
                time.sleep(5)
            else:
                raise


def run_notebook(notebook_name: str, notebook_id: str, token: str) -> bool:
    """Run a single notebook and wait for it to complete."""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Use the notebook run API
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{notebook_id}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(url, headers=headers)

    if resp.status_code not in (200, 202):
        log.error("  ❌ Failed to start %s: %s %s", notebook_name, resp.status_code, resp.text[:300])
        return False

    location = resp.headers.get("Location")
    if not location:
        log.warning("  No Location header, can't poll for status")
        return True

    log.info("  Started %s, polling for completion...", notebook_name)

    # Poll for completion
    max_wait = 1800  # 30 minutes
    poll_interval = 15
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(poll_interval)
        elapsed += poll_interval

        # Refresh token if needed (every 10 minutes)
        if elapsed % 600 == 0:
            token = get_token()
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        poll = requests.get(location, headers=headers)
        if poll.status_code != 200:
            log.warning("  Poll %s: HTTP %d", notebook_name, poll.status_code)
            continue

        body = poll.json()
        status = body.get("status", "Unknown")

        if status == "Completed":
            log.info("  ✅ %s completed successfully (%ds)", notebook_name, elapsed)
            return True
        elif status in ("Failed", "Cancelled", "Deduped"):
            error = body.get("failureReason", {}).get("message", "no details")
            log.error("  ❌ %s %s: %s", notebook_name, status, error)
            log.error("  Full response: %s", json.dumps(body, indent=2)[:2000])
            return False
        else:
            if elapsed % 60 == 0:
                log.info("  ⏳ %s: %s (%ds elapsed)", notebook_name, status, elapsed)

    log.error("  ❌ %s timed out after %ds", notebook_name, max_wait)
    return False


def check_lakehouse_tables(token: str):
    """Check which tables exist in each lakehouse."""
    headers = {"Authorization": f"Bearer {token}"}
    log.info("\n=== LAKEHOUSE TABLES ===")
    for lh_name, lh_id in LAKEHOUSES.items():
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/lakehouses/{lh_id}/tables"
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            tables = resp.json().get("data", [])
            table_names = [t["name"] for t in tables]
            log.info("  %s: %d tables %s", lh_name, len(tables), table_names)
        else:
            log.warning("  %s: HTTP %d", lh_name, resp.status_code)


def main():
    token = get_token()
    log.info("Token acquired")

    # Check lakehouse tables before
    check_lakehouse_tables(token)

    # Run notebooks sequentially
    log.info("\n=== RUNNING NOTEBOOKS ===")
    for nb_name, nb_id in NOTEBOOKS.items():
        log.info("Starting %s...", nb_name)
        token = get_token()  # Fresh token for each notebook
        success = run_notebook(nb_name, nb_id, token)
        if not success:
            log.error("Pipeline stopped — %s failed", nb_name)
            break
    else:
        log.info("\n✅ All notebooks completed successfully!")

    # Check lakehouse tables after
    token = get_token()
    check_lakehouse_tables(token)


if __name__ == "__main__":
    main()
