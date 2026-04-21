"""Upload notebook content to Fabric workspace using item definition API.

Converts .py notebooks (with # ── CELL markers) to Fabric's native
notebook-content.py format (# CELL ******************** markers + META blocks).
"""
import base64
import json
import logging
import re
import time
from pathlib import Path

import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
NOTEBOOKS = {
    "41378393-200e-4cb7-8887-2209104393d6": "01_bronze_ingestion",
    "a65f0278-9dc0-402c-a1aa-c49c3e424a8f": "02_silver_features",
    "094a9e43-55f0-4f11-a36f-fede8515d46c": "03_ml_training",
    "c2c2a2f7-3d71-490e-94a4-1b42a9787c25": "04_omop_transformation",
}
LAKEHOUSES = {
    "bronze_lakehouse": "e1f2c38d-3f87-48ed-9769-6d2c8de22595",
    "silver_lakehouse": "270a6614-2a07-463d-94de-0c55b26ec6de",
    "gold_lakehouse": "2960eef0-5de6-4117-80b1-6ee783cdaeec",
    "gold_omop": "8adebdbb-1cde-40e8-bcde-1691f0e7e2b2",
}
LAKEHOUSE_FOR_NOTEBOOK = {
    "01_bronze_ingestion": "bronze_lakehouse",
    "02_silver_features": "silver_lakehouse",
    "03_ml_training": "gold_lakehouse",
    "04_omop_transformation": "gold_omop",
}
NOTEBOOKS_DIR = Path(__file__).resolve().parent.parent / "src" / "notebooks"

CELL_MARKER = re.compile(r"^# ── (PARAMETERCELL|CELL).*$")


def py_to_fabric_notebook(py_path: Path, default_lakehouse_name: str) -> str:
    """Convert a .py file to Fabric's native notebook-content.py format."""
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

    lh_id = LAKEHOUSES[default_lakehouse_name]
    parts = [
        "# Fabric notebook source", "",
        "# METADATA ********************", "",
        "# META {",
        '# META   "kernel_info": {',
        '# META     "name": "synapse_pyspark"',
        "# META   },",
        '# META   "dependencies": {',
        '# META     "lakehouse": {',
        f'# META       "default_lakehouse": "{lh_id}",',
        f'# META       "default_lakehouse_name": "{default_lakehouse_name}",',
        f'# META       "default_lakehouse_workspace_id": "{WORKSPACE_ID}"',
        "# META     }",
        "# META   }",
        "# META }", "",
    ]

    for cell in cells:
        code = cell["code"].rstrip()
        if not code:
            continue
        parts.extend(["# CELL ********************", "", code, "", "# METADATA ********************", ""])
        if cell["is_parameter"]:
            parts.extend([
                "# META {", '# META   "language": "python",',
                '# META   "language_group": "synapse_pyspark",',
                '# META   "tags": [', '# META     "parameters"', "# META   ]", "# META }",
            ])
        else:
            parts.extend([
                "# META {", '# META   "language": "python",',
                '# META   "language_group": "synapse_pyspark"', "# META }",
            ])
        parts.append("")

    return "\n".join(parts)


def get_token():
    cred = AzureCliCredential()
    token = cred.get_token("https://api.fabric.microsoft.com/.default")
    return token.token


def upload_notebook(notebook_id: str, notebook_name: str, token: str):
    """Upload notebook content using item definition update API."""
    py_path = NOTEBOOKS_DIR / f"{notebook_name}.py"
    if not py_path.exists():
        log.error("File not found: %s", py_path)
        return

    lh_name = LAKEHOUSE_FOR_NOTEBOOK[notebook_name]
    fabric_content = py_to_fabric_notebook(py_path, lh_name)
    content_b64 = base64.b64encode(fabric_content.encode("utf-8")).decode("ascii")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "definition": {
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payload": content_b64,
                    "payloadType": "InlineBase64",
                }
            ]
        }
    }

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{notebook_id}/updateDefinition"
    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code == 200:
        log.info("✅ %s: content uploaded (200)", notebook_name)
    elif resp.status_code == 202:
        log.info("✅ %s: update accepted (202), polling...", notebook_name)
        location = resp.headers.get("Location")
        if location:
            for _ in range(30):
                time.sleep(3)
                poll = requests.get(location, headers=headers)
                if poll.status_code == 200:
                    body = poll.json()
                    if body.get("status") == "Succeeded":
                        log.info("  ✅ %s: definition update complete", notebook_name)
                        break
                    elif body.get("status") == "Failed":
                        log.error("  ❌ %s: update failed: %s", notebook_name, body)
                        break
    else:
        log.error("❌ %s: %d — %s", notebook_name, resp.status_code, resp.text[:300])


def main():
    token = get_token()
    log.info("Token acquired")

    for nb_id, nb_name in NOTEBOOKS.items():
        upload_notebook(nb_id, nb_name, token)

    log.info("All notebooks uploaded!")


if __name__ == "__main__":
    main()
