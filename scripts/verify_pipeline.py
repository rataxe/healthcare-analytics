"""Verify pipeline definition from Fabric."""
import base64, json, time
import requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
token = cred.get_token("https://api.fabric.microsoft.com/.default").token
h = {"Authorization": f"Bearer {token}"}
wsId = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
plId = "a163c4c5-376b-449a-9cad-50d45194370d"

url = f"https://api.fabric.microsoft.com/v1/workspaces/{wsId}/items/{plId}/getDefinition"
resp = requests.post(url, headers=h)
print(f"Status: {resp.status_code}")

if resp.status_code == 200:
    parts = resp.json().get("definition", {}).get("parts", [])
    for p in parts:
        decoded = base64.b64decode(p["payload"]).decode("utf-8")
        d = json.loads(decoded)
        acts = d.get("properties", {}).get("activities", [])
        for a in acts:
            pol = a.get("policy", {})
            name = a["name"]
            print(f"  {name}: timeout={pol.get('timeout')}, retry={pol.get('retry')}")
elif resp.status_code == 202:
    print("Long-running op...")
    loc = resp.headers.get("Location")
    for _ in range(10):
        time.sleep(3)
        p = requests.get(loc, headers=h)
        if p.status_code == 200:
            data = p.json()
            st = data.get("status")
            print(f"Poll: {st}")
            if st == "Succeeded":
                break
