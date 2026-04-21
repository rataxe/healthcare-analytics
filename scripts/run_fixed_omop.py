"""Upload just the fixed 04_omop_transformation notebook and run it."""
import sys
sys.path.insert(0, "scripts")
from fix_notebooks import upload_notebook, get_token
import logging
import json
import time
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
NOTEBOOK_ID = "c2c2a2f7-3d71-490e-94a4-1b42a9787c25"

def main():
    token = get_token()

    # 1. Upload
    log.info("Uploading fixed 04_omop_transformation...")
    ok = upload_notebook(NOTEBOOK_ID, "04_omop_transformation", token)
    if not ok:
        log.error("Upload failed!")
        return

    time.sleep(3)

    # 2. Execute
    log.info("Executing notebook...")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    exec_url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(exec_url, headers=headers)
    log.info("Execute response: %d", resp.status_code)
    if resp.status_code != 202:
        log.error("Execute failed: %s", resp.text[:500])
        return

    location = resp.headers.get("Location", "")
    start = time.time()
    while time.time() - start < 900:  # 15 min max
        time.sleep(15)
        elapsed = int(time.time() - start)
        r = requests.get(location, headers=headers)
        if r.status_code == 200:
            body = r.json()
            status = body.get("status", "")
            log.info("[%ds] Status: %s", elapsed, status)
            if status in ("Completed", "Succeeded"):
                log.info("SUCCESS!")
                return
            if status == "Failed":
                log.error("FAILED: %s", json.dumps(body.get("failureReason", {})))
                return
        elif r.status_code == 202:
            body = r.json()
            log.info("[%ds] Status: %s", elapsed, body.get("status", "?"))
        else:
            log.info("[%ds] Poll: %d", elapsed, r.status_code)

if __name__ == "__main__":
    main()
