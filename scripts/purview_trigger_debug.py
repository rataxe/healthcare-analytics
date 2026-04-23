"""Debug scan trigger."""
import requests, json
from azure.identity import AzureCliCredential

SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Try with scanLevel body
body = {"scanLevel": "Full"}
url = f"{SCAN_EP}/scan/datasources/sql-hca-demo/scans/healthcare-scan/runs/trigger-2?api-version=2022-07-01-preview"
r = requests.put(url, headers=h, json=body)
print(f"PUT with scanLevel: {r.status_code} {r.text[:300]}")

# Try POST to /run (no s)
url2 = f"{SCAN_EP}/scan/datasources/sql-hca-demo/scans/healthcare-scan/run?api-version=2022-07-01-preview"
r2 = requests.post(url2, headers=h, json=body)
print(f"POST /run: {r2.status_code} {r2.text[:300]}")

# Check existing runs
url3 = f"{SCAN_EP}/scan/datasources/sql-hca-demo/scans/healthcare-scan/runs?api-version=2022-07-01-preview"
r3 = requests.get(url3, headers=h)
print(f"GET runs: {r3.status_code}")
if r3.status_code == 200:
    runs = r3.json().get("value", [])
    if runs:
        latest = runs[0]
        print(f"  Latest: id={latest.get('id')} status={latest.get('status')}")
        print(f"  Start: {latest.get('startTime')}")

# Try az CLI to trigger
print("\nTrying az purview scan trigger...")
import subprocess
result = subprocess.run(
    ["az", "rest", "--method", "post",
     "--url", f"{SCAN_EP}/scan/datasources/sql-hca-demo/scans/healthcare-scan/runs?api-version=2022-07-01-preview",
     "--body", '{"scanLevel":"Full"}',
     "--resource", "https://purview.azure.net"],
    capture_output=True, text=True
)
print(f"  az rest POST: exit={result.returncode}")
print(f"  stdout: {result.stdout[:200]}")
print(f"  stderr: {result.stderr[:200]}")
