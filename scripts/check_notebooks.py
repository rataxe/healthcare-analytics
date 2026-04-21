"""Check notebook definitions in Fabric."""
import requests, base64, json, time
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
token = cred.get_token("https://api.fabric.microsoft.com/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
ws = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"

# List all notebooks
resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/items?type=Notebook", headers=h)
notebooks = resp.json().get("value", [])
for nb in notebooks:
    print(f"  {nb['displayName']}: {nb['id']}")

# List all lakehouses and their tables
print("\n=== LAKEHOUSES ===")
resp = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/lakehouses", headers=h)
for lh in resp.json().get("value", []):
    lh_id = lh["id"]
    lh_name = lh["displayName"]
    # Get tables
    resp_t = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/lakehouses/{lh_id}/tables",
        headers=h,
    )
    tables = resp_t.json().get("data", []) if resp_t.status_code == 200 else []
    table_names = [t["name"] for t in tables]
    print(f"  {lh_name} ({lh_id}): {len(tables)} tables -> {table_names}")

# Download each notebook definition
print("\n=== NOTEBOOK DEFINITIONS ===")
for nb in notebooks:
    nb_id = nb["id"]
    nb_name = nb["displayName"]
    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/notebooks/{nb_id}/getDefinition",
        headers=h,
    )
    if resp.status_code == 200:
        defn = resp.json()
    elif resp.status_code == 202:
        loc = resp.headers.get("Location")
        if isinstance(loc, list):
            loc = loc[0]
        defn = None
        for _ in range(15):
            time.sleep(2)
            poll = requests.get(loc, headers=h)
            if poll.status_code == 200:
                defn = poll.json()
                break
    else:
        print(f"  {nb_name}: error {resp.status_code}")
        continue

    if not defn:
        print(f"  {nb_name}: timeout")
        continue

    parts = defn.get("definition", {}).get("parts", [])
    for part in parts:
        payload_raw = base64.b64decode(part["payload"]).decode("utf-8")
        path = part["path"]
        print(f"\n  {nb_name} / {path} ({len(payload_raw)} chars):")
        # Check if it's JSON (ipynb) or raw python
        try:
            nb_json = json.loads(payload_raw)
            n_cells = len(nb_json.get("cells", []))
            kernel = nb_json.get("metadata", {}).get("kernelspec", {}).get("name", "?")
            print(f"    Format: ipynb, cells={n_cells}, kernel={kernel}")
            for i, cell in enumerate(nb_json.get("cells", [])[:3]):
                src = cell.get("source", "")
                if isinstance(src, list):
                    preview = "".join(src[:3])[:120]
                else:
                    preview = str(src)[:120]
                print(f"    Cell {i}: {cell['cell_type']} -> {preview}")
        except json.JSONDecodeError:
            print(f"    Format: raw python")
            print(f"    Preview: {payload_raw[:200]}")
