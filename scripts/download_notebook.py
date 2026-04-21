"""Download the full notebook content from Fabric and save locally for inspection."""
import base64
import json
import time
import requests
from azure.identity import AzureCliCredential

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
NOTEBOOK_ID = "c2c2a2f7-3d71-490e-94a4-1b42a9787c25"

def get_token():
    cred = AzureCliCredential(process_timeout=30)
    return cred.get_token("https://api.fabric.microsoft.com/.default").token

def main():
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/getDefinition"
    resp = requests.post(url, headers=headers)
    
    if resp.status_code == 202:
        location = resp.headers.get("Location", "")
        for _ in range(10):
            time.sleep(3)
            resp2 = requests.get(location, headers=headers)
            if resp2.status_code == 200:
                body = resp2.json()
                if body.get("status") == "Succeeded":
                    result_loc = resp2.headers.get("Location", "")
                    if result_loc:
                        resp = requests.get(result_loc, headers=headers)
                    break
    
    if resp.status_code == 200:
        data = resp.json()
        for part in data.get("definition", {}).get("parts", []):
            if part.get("path") == "notebook-content.py":
                content = base64.b64decode(part["payload"]).decode("utf-8")
                with open("scripts/omop_downloaded.py", "w", encoding="utf-8") as f:
                    f.write(content)
                print(f"Saved {len(content)} chars, {len(content.splitlines())} lines to scripts/omop_downloaded.py")

if __name__ == "__main__":
    main()
