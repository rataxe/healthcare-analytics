"""Run a single notebook and get detailed error info."""
import json
import logging
import sys
import time

import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"

NOTEBOOKS = {
    "01_bronze_ingestion": "41378393-200e-4cb7-8887-2209104393d6",
    "02_silver_features": "a65f0278-9dc0-402c-a1aa-c49c3e424a8f",
    "03_ml_training": "094a9e43-55f0-4f11-a36f-fede8515d46c",
    "04_omop_transformation": "c2c2a2f7-3d71-490e-94a4-1b42a9787c25",
}


def get_token():
    cred = AzureCliCredential(process_timeout=30)
    return cred.get_token("https://api.fabric.microsoft.com/.default").token


def main():
    nb_name = sys.argv[1] if len(sys.argv) > 1 else "02_silver_features"
    nb_id = NOTEBOOKS[nb_name]
    log.info("Running %s (%s)", nb_name, nb_id)

    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Start notebook
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{nb_id}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(url, headers=headers)
    log.info("Start response: %d", resp.status_code)
    log.info("Start headers: %s", dict(resp.headers))
    if resp.text:
        log.info("Start body: %s", resp.text[:500])

    if resp.status_code not in (200, 202):
        log.error("Failed to start")
        return

    location = resp.headers.get("Location")
    if not location:
        log.error("No Location header")
        return

    log.info("Polling: %s", location)

    # Poll
    elapsed = 0
    while elapsed < 600:
        time.sleep(15)
        elapsed += 15

        if elapsed % 300 == 0:
            token = get_token()
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        poll = requests.get(location, headers=headers)
        body = poll.json() if poll.status_code == 200 else {}
        status = body.get("status", "Unknown")

        if elapsed % 30 == 0:
            log.info("  %ds: status=%s", elapsed, status)

        if status == "Completed":
            log.info("✅ %s completed in %ds", nb_name, elapsed)
            log.info("Full body: %s", json.dumps(body, indent=2)[:3000])
            return

        if status in ("Failed", "Cancelled"):
            log.error("❌ %s %s after %ds", nb_name, status, elapsed)
            log.error("Full body:\n%s", json.dumps(body, indent=2))

            # Try to get job details
            # Extract instance ID from location URL
            # Location is like: .../items/{id}/jobs/instances/{instanceId}
            try:
                instance_id = location.rstrip("/").split("/")[-1]
                detail_url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{nb_id}/jobs/instances/{instance_id}"
                detail_resp = requests.get(detail_url, headers=headers)
                log.info("Detail response (%d): %s", detail_resp.status_code, detail_resp.text[:3000])
            except Exception as e:
                log.warning("Could not get detail: %s", e)
            return

    log.error("Timed out after %ds", elapsed)


if __name__ == "__main__":
    main()
