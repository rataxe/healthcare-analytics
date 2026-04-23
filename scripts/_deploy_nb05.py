"""Create + upload 05_batch_scoring notebook to Fabric HCA workspace."""
import base64, json, re, time, requests
from pathlib import Path
from azure.identity import AzureCliCredential

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
FABRIC_API   = "https://api.fabric.microsoft.com/v1"
NB_NAME      = "05_batch_scoring"
NB_FILE      = Path(__file__).resolve().parent.parent / "src" / "notebooks" / "05_batch_scoring.py"

LAKEHOUSES = {
    "bronze_lakehouse": "e1f2c38d-3f87-48ed-9769-6d2c8de22595",
    "silver_lakehouse": "270a6614-2a07-463d-94de-0c55b26ec6de",
    "gold_lakehouse":   "2960eef0-5de6-4117-80b1-6ee783cdaeec",
    "gold_omop":        "8adebdbb-1cde-40e8-bcde-1691f0e7e2b2",
}
DEFAULT_LH_NAME = "gold_lakehouse"
CELL_MARKER = re.compile(r"^# ── (PARAMETERCELL|CELL).*$")

cred = AzureCliCredential(process_timeout=30)

def headers():
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def py_to_fabric(py_path: Path, lh_name: str) -> str:
    code = py_path.read_text(encoding="utf-8")
    lines = code.splitlines()
    cells, current, is_param, first = [], [], False, True
    for line in lines:
        m = CELL_MARKER.match(line)
        if m:
            if current or not first:
                cells.append({"code": "\n".join(current), "is_parameter": is_param})
            current, is_param, first = [], m.group(1) == "PARAMETERCELL", False
        else:
            current.append(line)
    if current:
        cells.append({"code": "\n".join(current), "is_parameter": is_param})
    if cells and not cells[0]["code"].strip():
        cells.pop(0)

    lh_id = LAKEHOUSES[lh_name]
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
        f'# META       "default_lakehouse_name": "{lh_name}",',
        f'# META       "default_lakehouse_workspace_id": "{WORKSPACE_ID}"',
        "# META     }",
        "# META   }",
        "# META }", "",
    ]
    for cell in cells:
        c = cell["code"].rstrip()
        if not c:
            continue
        parts.extend(["# CELL ********************", "", c, "", "# METADATA ********************", ""])
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

def wait_operation(location, retry_after=5, max_wait=120):
    h = headers()
    for _ in range(max_wait // retry_after):
        time.sleep(retry_after)
        r = requests.get(location, headers=h, timeout=30)
        if r.status_code == 200:
            body = r.json()
            if body.get("status") == "Succeeded":
                return True
            if body.get("status") in ("Failed", "Cancelled"):
                print(f"  ✗ Operation {body.get('status')}: {body}")
                return False
    return False

def find_existing():
    h = headers()
    r = requests.get(f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/items?type=Notebook", headers=h, timeout=30)
    if r.status_code == 200:
        for item in r.json().get("value", []):
            if item["displayName"] == NB_NAME:
                return item["id"]
    return None

def create_notebook():
    # Step 1: Create empty notebook item
    body = {
        "displayName": NB_NAME,
        "type": "Notebook",
    }
    h = headers()
    r = requests.post(f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/items", headers=h, json=body, timeout=60)
    nb_id = None
    if r.status_code == 201:
        nb_id = r.json()["id"]
        print(f"✅ Created empty notebook: {nb_id}")
    elif r.status_code == 202:
        print("⏳ Async creation, polling...")
        loc = r.headers.get("Location", "")
        ra = int(r.headers.get("Retry-After", "5"))
        if wait_operation(loc, ra):
            nb_id = find_existing()
            if nb_id:
                print(f"✅ Created empty notebook: {nb_id}")
        if not nb_id:
            print("✗ Async creation failed")
            return None
    elif r.status_code == 409:
        nb_id = find_existing()
        print(f"Already exists: {nb_id}")
    else:
        print(f"✗ Create failed: {r.status_code} — {r.text[:300]}")
        return None

    # Step 2: Upload content via updateDefinition
    if nb_id:
        print("Uploading notebook content...")
        time.sleep(3)  # Brief pause before update
        update_notebook(nb_id)
    return nb_id

def update_notebook(nb_id):
    fabric_content = py_to_fabric(NB_FILE, DEFAULT_LH_NAME)
    payload_b64 = base64.b64encode(fabric_content.encode("utf-8")).decode("ascii")

    body = {
        "definition": {
            "parts": [{
                "path": "notebook-content.py",
                "payload": payload_b64,
                "payloadType": "InlineBase64",
            }]
        }
    }
    h = headers()
    url = f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/items/{nb_id}/updateDefinition"
    r = requests.post(url, headers=h, json=body, timeout=60)
    if r.status_code == 200:
        print(f"✅ Content uploaded (200)")
    elif r.status_code == 202:
        print("⏳ Upload accepted, polling...")
        loc = r.headers.get("Location", "")
        if wait_operation(loc):
            print("✅ Content uploaded")
        else:
            print("✗ Upload poll failed")
    else:
        print(f"✗ Upload failed: {r.status_code} — {r.text[:300]}")

def main():
    nb_id = find_existing()
    if nb_id:
        print(f"Notebook already exists: {nb_id} — updating content...")
        update_notebook(nb_id)
    else:
        print("Creating new notebook in Fabric...")
        nb_id = create_notebook()
        if not nb_id:
            print("Failed to create notebook")
    print("\nDone!")

if __name__ == "__main__":
    main()
