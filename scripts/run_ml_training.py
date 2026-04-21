"""Run 03_ml_training notebook on Fabric and poll for completion."""
import logging
import time
import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
NOTEBOOK_ID = "094a9e43-55f0-4f11-a36f-fede8515d46c"  # 03_ml_training


def get_token():
    return AzureCliCredential(process_timeout=30).get_token("https://api.fabric.microsoft.com/.default").token


def run_notebook():
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Execute notebook
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
    max_polls = 120  # 10 min max

    for i in range(max_polls):
        time.sleep(5)
        elapsed = int(time.time() - start)

        # Refresh token every 4 minutes
        if i > 0 and i % 48 == 0:
            token = get_token()
            headers["Authorization"] = f"Bearer {token}"

        poll = requests.get(location, headers=headers)

        if poll.status_code == 200:
            body = poll.json()
            status = body.get("status", "Unknown")
            log.info("[%ds] Status: %s", elapsed, status)

            if status == "Completed":
                log.info("✅ 03_ml_training COMPLETED in %ds", elapsed)
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
