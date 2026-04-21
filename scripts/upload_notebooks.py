"""Upload notebook content to Fabric workspace using item definition API."""
import base64
import json
import logging
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
}
LAKEHOUSES = {
    "bronze_lakehouse": "e1f2c38d-3f87-48ed-9769-6d2c8de22595",
    "silver_lakehouse": "270a6614-2a07-463d-94de-0c55b26ec6de",
    "gold_lakehouse": "2960eef0-5de6-4117-80b1-6ee783cdaeec",
}
LAKEHOUSE_FOR_NOTEBOOK = {
    "01_bronze_ingestion": "bronze_lakehouse",
    "02_silver_features": "silver_lakehouse",
    "03_ml_training": "gold_lakehouse",
}
NOTEBOOKS_DIR = Path(__file__).resolve().parent.parent / "src" / "notebooks"


def py_to_ipynb(py_path: Path) -> dict:
    """Convert a .py file to Jupyter notebook format."""
    code = py_path.read_text(encoding="utf-8")

    # Split into cells based on comment markers
    cells = []
    current_lines = []
    for line in code.splitlines():
        if line.startswith("# ── CELL") or line.startswith("# ── PARAMETERCELL"):
            if current_lines:
                cells.append("\n".join(current_lines))
                current_lines = []
        current_lines.append(line)
    if current_lines:
        cells.append("\n".join(current_lines))

    nb_cells = []
    for cell_code in cells:
        nb_cells.append({
            "cell_type": "code",
            "source": cell_code,
            "metadata": {},
            "outputs": [],
            "execution_count": None,
        })

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "kernelspec": {
                "name": "synapse_pyspark",
                "display_name": "Synapse PySpark",
            },
        },
        "cells": nb_cells,
    }


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

    ipynb = py_to_ipynb(py_path)
    ipynb_json = json.dumps(ipynb, ensure_ascii=False)
    ipynb_b64 = base64.b64encode(ipynb_json.encode("utf-8")).decode("ascii")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "definition": {
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payload": base64.b64encode(
                        py_path.read_text(encoding="utf-8").encode("utf-8")
                    ).decode("ascii"),
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
        # Poll for completion
        location = resp.headers.get("Location")
        if location:
            for _ in range(30):
                time.sleep(2)
                poll = requests.get(location, headers=headers)
                if poll.status_code == 200:
                    log.info("  ✅ %s: definition update complete", notebook_name)
                    break
    else:
        log.error("❌ %s: %d — %s", notebook_name, resp.status_code, resp.text[:300])


def set_default_lakehouse(notebook_id: str, notebook_name: str, token: str):
    """Attach default lakehouse to notebook."""
    lh_name = LAKEHOUSE_FOR_NOTEBOOK.get(notebook_name)
    if not lh_name:
        return
    lh_id = LAKEHOUSES[lh_name]

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Get current definition first
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{notebook_id}"
    resp = requests.get(url, headers=headers)
    log.info("Notebook %s details: %s", notebook_name, resp.status_code)


def main():
    token = get_token()
    log.info("Token acquired")

    for nb_id, nb_name in NOTEBOOKS.items():
        upload_notebook(nb_id, nb_name, token)

    log.info("All notebooks uploaded!")


if __name__ == "__main__":
    main()
