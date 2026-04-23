"""Trigger all 3 scans using correct endpoint."""
import requests, time
from azure.identity import AzureCliCredential

SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
API_VER = "2022-07-01-preview"

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

scans = [
    ("Fabric", "Scan-HCA"),
    ("Fabric", "Scan-BrainChild"),
]

print("Trigger Fabric scans")
print("=" * 50)
for ds, scan in scans:
    url = f"{SCAN_EP}/scan/datasources/{ds}/scans/{scan}/run?api-version={API_VER}"
    r = requests.post(url, headers=h, json={"scanLevel": "Full"})
    status = "✅" if r.status_code in (200, 201, 202) else "⚠️"
    print(f"  {status} {ds}/{scan}: {r.status_code}")
    if r.status_code in (200, 201, 202):
        d = r.json()
        print(f"     runId={d.get('scanResultId')} status={d.get('status')}")
    else:
        print(f"     {r.text[:200]}")
    time.sleep(2)

# Check SQL scan status
print("\nCheck all scan statuses")
print("=" * 50)
for ds, scan in [("sql-hca-demo", "healthcare-scan"), ("Fabric", "Scan-HCA"), ("Fabric", "Scan-BrainChild")]:
    url = f"{SCAN_EP}/scan/datasources/{ds}/scans/{scan}/runs?api-version={API_VER}"
    r = requests.get(url, headers=h)
    if r.status_code == 200:
        runs = r.json().get("value", [])
        if runs:
            latest = runs[0]
            print(f"  {ds}/{scan}: status={latest.get('status')} start={latest.get('startTime','?')[:19]}")
        else:
            print(f"  {ds}/{scan}: no runs")
    else:
        print(f"  {ds}/{scan}: GET failed {r.status_code}")

print("\nAll scans triggered! Check again in 2-3 minutes.")
