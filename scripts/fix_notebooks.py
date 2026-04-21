"""
Fix and re-upload ALL notebooks to Fabric.

Problems fixed:
1. Converts .py notebooks to Fabric's native notebook-content.py format
   (# CELL ******************** markers + # META JSON blocks)
2. Attaches default lakehouse to each notebook in metadata
3. Re-uploads all 4 notebooks using updateDefinition API
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

# Which default lakehouse each notebook should be attached to
LAKEHOUSE_FOR_NOTEBOOK = {
    "01_bronze_ingestion": "bronze_lakehouse",
    "02_silver_features": "silver_lakehouse",
    "03_ml_training": "gold_lakehouse",
    "04_omop_transformation": "gold_omop",
}

# Additional lakehouses each notebook needs access to (cross-lakehouse reads)
EXTRA_LAKEHOUSES_FOR_NOTEBOOK = {
    "01_bronze_ingestion": [],
    "02_silver_features": ["bronze_lakehouse"],
    "03_ml_training": ["silver_lakehouse"],
    "04_omop_transformation": ["bronze_lakehouse"],
}

NOTEBOOKS_DIR = Path(__file__).resolve().parent.parent / "src" / "notebooks"

# Cell marker regex: matches both "# ── CELL" and "# ── PARAMETERCELL"
CELL_MARKER = re.compile(r"^# ── (PARAMETERCELL|CELL).*$")


def py_to_fabric_notebook(py_path: Path, default_lakehouse_name: str, extra_lakehouses: list[str] | None = None) -> str:
    """
    Convert a .py notebook (with # ── CELL markers) to Fabric's native
    notebook-content.py format.
    """
    extra_lakehouses = extra_lakehouses or []
    code = py_path.read_text(encoding="utf-8")
    lines = code.splitlines()

    # Split into cells based on markers
    cells = []
    current_lines = []
    is_parameter_cell = False
    first_cell = True

    for line in lines:
        m = CELL_MARKER.match(line)
        if m:
            if current_lines or not first_cell:
                # Save previous cell
                cells.append({
                    "code": "\n".join(current_lines),
                    "is_parameter": is_parameter_cell,
                })
            current_lines = []
            is_parameter_cell = m.group(1) == "PARAMETERCELL"
            first_cell = False
        else:
            current_lines.append(line)

    # Save last cell
    if current_lines:
        cells.append({
            "code": "\n".join(current_lines),
            "is_parameter": is_parameter_cell,
        })

    # Handle case where first lines before any marker are header comments
    # If the first cell has no code (only comments before first marker), merge
    # it as a header comment cell
    if cells and not cells[0]["code"].strip():
        cells.pop(0)

    # Build Fabric notebook-content.py
    lh_id = LAKEHOUSES[default_lakehouse_name]
    parts = []

    # Global header
    parts.append("# Fabric notebook source")
    parts.append("")
    parts.append("# METADATA ********************")
    parts.append("")
    parts.append("# META {")
    parts.append('# META   "kernel_info": {')
    parts.append('# META     "name": "synapse_pyspark"')
    parts.append("# META   },")
    parts.append('# META   "dependencies": {')
    parts.append('# META     "lakehouse": {')
    parts.append(f'# META       "default_lakehouse": "{lh_id}",')
    parts.append(f'# META       "default_lakehouse_name": "{default_lakehouse_name}",')
    if extra_lakehouses:
        parts.append(f'# META       "default_lakehouse_workspace_id": "{WORKSPACE_ID}",')
        known = [{"id": LAKEHOUSES[lh_name]} for lh_name in extra_lakehouses]
        parts.append(f'# META       "known_lakehouses": {json.dumps(known)}')
    else:
        parts.append(f'# META       "default_lakehouse_workspace_id": "{WORKSPACE_ID}"')
    parts.append("# META     }")
    parts.append("# META   }")
    parts.append("# META }")
    parts.append("")

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
            parts.append("# META {")
            parts.append('# META   "language": "python",')
            parts.append('# META   "language_group": "synapse_pyspark",')
            parts.append('# META   "tags": [')
            parts.append('# META     "parameters"')
            parts.append("# META   ]")
            parts.append("# META }")
        else:
            parts.append("# META {")
            parts.append('# META   "language": "python",')
            parts.append('# META   "language_group": "synapse_pyspark"')
            parts.append("# META }")

        parts.append("")

    return "\n".join(parts)


def get_token():
    cred = AzureCliCredential()
    return cred.get_token("https://api.fabric.microsoft.com/.default").token


def wait_for_operation(location_url: str, headers: dict, max_polls: int = 30) -> bool:
    """Poll a long-running operation until completion."""
    for i in range(max_polls):
        time.sleep(3)
        resp = requests.get(location_url, headers=headers)
        if resp.status_code == 200:
            body = resp.json()
            status = body.get("status", "")
            if status == "Succeeded":
                return True
            if status == "Failed":
                log.error("  Operation failed: %s", body.get("error", ""))
                return False
        elif resp.status_code == 202:
            continue
    log.error("  Operation timed out after %d polls", max_polls)
    return False


def upload_notebook(notebook_id: str, notebook_name: str, token: str):
    """Upload notebook content using item updateDefinition API."""
    py_path = NOTEBOOKS_DIR / f"{notebook_name}.py"
    if not py_path.exists():
        log.error("File not found: %s", py_path)
        return False

    lh_name = LAKEHOUSE_FOR_NOTEBOOK[notebook_name]
    extra = EXTRA_LAKEHOUSES_FOR_NOTEBOOK.get(notebook_name, [])
    log.info("Converting %s (default lakehouse: %s, extra: %s)", notebook_name, lh_name, extra)

    fabric_content = py_to_fabric_notebook(py_path, lh_name, extra)
    content_b64 = base64.b64encode(
        fabric_content.encode("utf-8")
    ).decode("ascii")

    # Also create .platform file
    platform = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {
            "type": "Notebook",
            "displayName": notebook_name,
        },
        "config": {
            "version": "2.0",
            "logicalId": notebook_id,
        },
    }, indent=2)
    platform_b64 = base64.b64encode(platform.encode("utf-8")).decode("ascii")

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
                },
                {
                    "path": ".platform",
                    "payload": platform_b64,
                    "payloadType": "InlineBase64",
                },
            ]
        }
    }

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{notebook_id}/updateDefinition"
    log.info("Uploading %s (%d chars)...", notebook_name, len(fabric_content))

    resp = requests.post(url, headers=headers, json=payload)

    if resp.status_code == 200:
        log.info("  ✅ %s: content uploaded (200)", notebook_name)
        return True
    elif resp.status_code == 202:
        log.info("  %s: accepted (202), waiting for completion...", notebook_name)
        location = resp.headers.get("Location")
        if location:
            ok = wait_for_operation(location, headers)
            if ok:
                log.info("  ✅ %s: definition update complete", notebook_name)
                return True
            else:
                log.error("  ❌ %s: definition update failed", notebook_name)
                return False
    else:
        log.error("  ❌ %s: %d — %s", notebook_name, resp.status_code, resp.text[:500])
        return False


def verify_notebook(notebook_id: str, notebook_name: str, token: str):
    """Download notebook definition and verify it has real content."""
    headers = {"Authorization": f"Bearer {token}"}

    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/notebooks/{notebook_id}/getDefinition",
        headers=headers,
    )

    defn = None
    if resp.status_code == 200:
        defn = resp.json()
    elif resp.status_code == 202:
        location = resp.headers.get("Location")
        if location:
            for _ in range(20):
                time.sleep(3)
                poll = requests.get(location, headers=headers)
                if poll.status_code == 200:
                    body = poll.json()
                    if body.get("status") == "Succeeded":
                        result_loc = poll.headers.get("Location")
                        if result_loc:
                            result = requests.get(result_loc, headers=headers)
                            if result.status_code == 200:
                                defn = result.json()
                        break

    if defn:
        parts = defn.get("definition", {}).get("parts", [])
        for p in parts:
            if p["path"] == "notebook-content.py":
                raw = base64.b64decode(p["payload"]).decode("utf-8")
                cell_count = raw.count("# CELL ********************")
                has_lakehouse = "default_lakehouse" in raw
                has_real_code = "spark" in raw.lower() or "import" in raw.lower()
                log.info(
                    "  Verify %s: %d cells, lakehouse=%s, real_code=%s, size=%d chars",
                    notebook_name, cell_count, has_lakehouse, has_real_code, len(raw),
                )
                if cell_count > 0 and has_real_code:
                    log.info("  ✅ %s: VERIFIED OK", notebook_name)
                    return True
                else:
                    log.error("  ❌ %s: content looks empty or invalid", notebook_name)
                    return False
    log.error("  ❌ %s: could not download definition", notebook_name)
    return False


def main():
    token = get_token()
    log.info("Token acquired")

    # Step 1: Upload all notebooks
    log.info("=" * 60)
    log.info("STEP 1: Re-uploading all notebooks with Fabric format")
    log.info("=" * 60)

    results = {}
    for nb_id, nb_name in NOTEBOOKS.items():
        ok = upload_notebook(nb_id, nb_name, token)
        results[nb_name] = ok

    # Step 2: Verify all notebooks
    log.info("")
    log.info("=" * 60)
    log.info("STEP 2: Verifying notebook content in Fabric")
    log.info("=" * 60)

    # Refresh token in case it expired during uploads
    token = get_token()

    for nb_id, nb_name in NOTEBOOKS.items():
        verify_notebook(nb_id, nb_name, token)

    # Summary
    log.info("")
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    for name, ok in results.items():
        status = "✅" if ok else "❌"
        log.info("  %s %s", status, name)


if __name__ == "__main__":
    main()
