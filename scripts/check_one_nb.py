"""Download one notebook definition to check format."""
import requests, base64, json, time
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
token = cred.get_token("https://api.fabric.microsoft.com/.default").token
h = {"Authorization": f"Bearer {token}"}
ws = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
nb_id = "41378393-200e-4cb7-8887-2209104393d6"  # 01_bronze_ingestion

resp = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{ws}/notebooks/{nb_id}/getDefinition",
    headers=h,
)
print("Status:", resp.status_code)

defn = None
if resp.status_code == 200:
    defn = resp.json()
elif resp.status_code == 202:
    loc = resp.headers.get("Location")
    if isinstance(loc, list):
        loc = loc[0]
    print("Polling:", loc)
    for i in range(20):
        time.sleep(3)
        poll = requests.get(loc, headers=h)
        print(f"  Poll {i}: {poll.status_code}")
        if poll.status_code == 200:
            body = poll.json()
            status = body.get("status")
            print(f"  Status: {status}")
            print(f"  Body keys: {list(body.keys())}")
            print(f"  Headers: {dict(poll.headers)}")
            if status in ("Succeeded", None):
                # Check for result location header
                result_loc = poll.headers.get("Location")
                if result_loc:
                    print(f"  Result location: {result_loc}")
                    result_resp = requests.get(result_loc, headers=h)
                    print(f"  Result status: {result_resp.status_code}")
                    defn = result_resp.json()
                else:
                    defn = body
                break
            elif status == "Failed":
                print(f"  Error: {body}")
                break
    else:
        print("TIMEOUT")

if defn:
    parts = defn.get("definition", {}).get("parts", [])
    for p in parts:
        raw = base64.b64decode(p["payload"]).decode("utf-8")
        print(f"\nPath: {p['path']} ({len(raw)} chars)")
        # Check if JSON
        try:
            obj = json.loads(raw)
            print("  Format: JSON/ipynb")
            cells = obj.get("cells", [])
            print(f"  Cells: {len(cells)}")
            if cells:
                src = cells[0].get("source", "")
                if isinstance(src, list):
                    preview = "".join(src[:3])
                else:
                    preview = str(src)[:200]
                print(f"  Cell 0 preview: {preview[:200]}")
        except json.JSONDecodeError:
            print("  Format: raw text")
            print(f"  Preview: {raw[:300]}")
