"""Upload diagnostic OMOP notebook, run it, then read the omop_diag table."""
import base64
import json
import logging
import re
import time
import requests
from pathlib import Path
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
NOTEBOOK_ID = "c2c2a2f7-3d71-490e-94a4-1b42a9787c25"  # 04_omop_transformation
GOLD_OMOP_LH_ID = "8adebdbb-1cde-40e8-bcde-1691f0e7e2b2"
BRONZE_LH_ID = "e1f2c38d-3f87-48ed-9769-6d2c8de22595"

CELL_MARKER = re.compile(r"^# ── (PARAMETERCELL|CELL).*$")

def get_token():
    cred = AzureCliCredential(process_timeout=30)
    return cred.get_token("https://api.fabric.microsoft.com/.default").token

def py_to_fabric(py_path):
    code = py_path.read_text(encoding="utf-8")
    lines = code.splitlines()
    cells = []
    current_lines = []
    is_parameter_cell = False
    first_cell = True

    for line in lines:
        m = CELL_MARKER.match(line)
        if m:
            if current_lines or not first_cell:
                cells.append({"code": "\n".join(current_lines), "is_parameter": is_parameter_cell})
            current_lines = []
            is_parameter_cell = m.group(1) == "PARAMETERCELL"
            first_cell = False
        else:
            current_lines.append(line)
    if current_lines:
        cells.append({"code": "\n".join(current_lines), "is_parameter": is_parameter_cell})
    if cells and not cells[0]["code"].strip():
        cells.pop(0)

    parts = [
        "# Fabric notebook source", "",
        "# METADATA ********************", "",
        "# META {",
        '# META   "kernel_info": {',
        '# META     "name": "synapse_pyspark"',
        "# META   },",
        '# META   "dependencies": {',
        '# META     "lakehouse": {',
        f'# META       "default_lakehouse": "{GOLD_OMOP_LH_ID}",',
        f'# META       "default_lakehouse_name": "gold_omop",',
        f'# META       "default_lakehouse_workspace_id": "{WORKSPACE_ID}",',
        f'# META       "known_lakehouses": [{{"id": "{BRONZE_LH_ID}"}}]',
        "# META     }",
        "# META   }",
        "# META }", "",
    ]
    for cell in cells:
        code = cell["code"].rstrip()
        if not code:
            continue
        parts.append("# CELL ********************")
        parts.append("")
        parts.append(code)
        parts.append("")
        parts.append("# METADATA ********************")
        parts.append("")
        if cell["is_parameter"]:
            parts += [
                "# META {", '# META   "language": "python",',
                '# META   "language_group": "synapse_pyspark",',
                '# META   "tags": [', '# META     "parameters"',
                "# META   ]", "# META }",
            ]
        else:
            parts += [
                "# META {", '# META   "language": "python",',
                '# META   "language_group": "synapse_pyspark"', "# META }",
            ]
        parts.append("")
    return "\n".join(parts)

def wait_op(location_url, headers, max_polls=30):
    for _ in range(max_polls):
        time.sleep(3)
        r = requests.get(location_url, headers=headers)
        if r.status_code == 200:
            body = r.json()
            if body.get("status") == "Succeeded":
                return True
            if body.get("status") == "Failed":
                log.error("Op failed: %s", body)
                return False
    return False

def main():
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # 1. Upload diagnostic notebook
    py_path = Path(__file__).resolve().parent.parent / "src" / "notebooks" / "04_omop_diag.py"
    log.info("Converting diagnostic notebook...")
    content = py_to_fabric(py_path)
    log.info("Content: %d chars, %d lines", len(content), len(content.splitlines()))

    payload_b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
    platform = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": "04_omop_transformation"},
        "config": {"version": "2.0", "logicalId": NOTEBOOK_ID},
    }, indent=2)
    platform_b64 = base64.b64encode(platform.encode("utf-8")).decode("ascii")
    body = {
        "definition": {
            "parts": [
                {"path": "notebook-content.py", "payload": payload_b64, "payloadType": "InlineBase64"},
                {"path": ".platform", "payload": platform_b64, "payloadType": "InlineBase64"},
            ]
        }
    }

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/updateDefinition"
    resp = requests.post(url, headers=headers, json=body)
    log.info("Upload response: %d", resp.status_code)
    if resp.status_code == 200:
        log.info("Upload complete (200)")
    elif resp.status_code == 202:
        loc = resp.headers.get("Location", "")
        if wait_op(loc, headers):
            log.info("Upload complete (202)")
        else:
            log.error("Upload failed")
            return
    else:
        log.error("Upload failed: %s", resp.text[:500])
        return

    time.sleep(2)

    # 2. Execute notebook
    log.info("Executing diagnostic notebook...")
    exec_url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(exec_url, headers=headers)
    log.info("Execute response: %d", resp.status_code)
    if resp.status_code != 202:
        log.error("Execute failed: %s", resp.text[:500])
        return

    location = resp.headers.get("Location", "")
    log.info("Polling job...")
    start = time.time()
    while time.time() - start < 600:  # 10 min max
        time.sleep(10)
        elapsed = int(time.time() - start)
        r = requests.get(location, headers=headers)
        if r.status_code == 200:
            body = r.json()
            status = body.get("status", "")
            log.info("[%ds] Status: %s", elapsed, status)
            if status in ("Completed", "Succeeded"):
                log.info("SUCCESS!")
                break
            if status == "Failed":
                log.error("FAILED: %s", json.dumps(body.get("failureReason", {})))
                break
        elif r.status_code == 202:
            body = r.json()
            status = body.get("status", body.get("percentComplete", "?"))
            log.info("[%ds] Status: %s", elapsed, status)
        else:
            log.info("[%ds] Poll response: %d", elapsed, r.status_code)

    # 3. Read omop_diag table via OneLake
    log.info("\nReading omop_diag table...")
    storage_token = AzureCliCredential(process_timeout=30).get_token("https://storage.azure.com/.default").token
    storage_headers = {"Authorization": f"Bearer {storage_token}"}

    # List files in the table
    list_url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{GOLD_OMOP_LH_ID}/Tables/omop_diag?resource=filesystem&recursive=true"
    resp = requests.get(list_url, headers=storage_headers)
    log.info("List omop_diag: %d", resp.status_code)
    if resp.status_code == 200:
        log.info("Files: %s", resp.text[:2000])

if __name__ == "__main__":
    main()
