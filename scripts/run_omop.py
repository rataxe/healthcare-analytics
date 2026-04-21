"""Run 04_omop_transformation notebook on Fabric and poll for completion."""
import logging
import time
import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
NOTEBOOK_ID = "c2c2a2f7-3d71-490e-94a4-1b42a9787c25"  # 04_omop_transformation


def get_token():
    return AzureCliCredential(process_timeout=30).get_token("https://api.fabric.microsoft.com/.default").token


def run_notebook():
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(url, headers=headers)
    log.info("Execute response: %d", resp.status_code)

    if resp.status_code not in (200, 202):
        log.error("Failed to start: %s", resp.text[:500])
        return

    location = resp.headers.get("Location")
    if not location:
        log.error("No Location header in response")
        return

    log.info("Job started, polling...")
    start = time.time()
    max_polls = 180  # 15 min max (OMOP is a big notebook)

    for i in range(max_polls):
        time.sleep(5)
        elapsed = int(time.time() - start)

        if i > 0 and i % 48 == 0:
            token = get_token()
            headers["Authorization"] = f"Bearer {token}"

        poll = requests.get(location, headers=headers)

        if poll.status_code == 200:
            body = poll.json()
            status = body.get("status", "Unknown")
            log.info("[%ds] Status: %s", elapsed, status)

            if status == "Completed":
                log.info("✅ 04_omop_transformation COMPLETED in %ds", elapsed)
                return True
            elif status == "Failed":
                error = body.get("failureReason", {})
                log.error("❌ FAILED at %ds: %s", elapsed, error)
                return False
            elif status in ("Cancelled", "Deduped"):
                log.error("❌ %s at %ds", status, elapsed)
                return False
        elif poll.status_code == 202:
            if i % 6 == 0:
                log.info("[%ds] InProgress...", elapsed)
        else:
            log.warning("[%ds] Poll returned %d", elapsed, poll.status_code)

    log.error("Timed out after %d polls", max_polls)
    return False


if __name__ == "__main__":
    run_notebook()
