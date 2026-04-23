"""Check scan status."""
import requests
from azure.identity import AzureCliCredential

SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
API = "2022-07-01-preview"
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}"}

for ds, scan in [("sql-hca-demo", "healthcare-scan"), ("Fabric", "Scan-HCA"), ("Fabric", "Scan-BrainChild")]:
    url = f"{SCAN_EP}/scan/datasources/{ds}/scans/{scan}/runs?api-version={API}"
    r = requests.get(url, headers=h)
    if r.status_code == 200:
        runs = r.json().get("value", [])
        if runs:
            s = runs[0]
            print(f"{ds}/{scan}: {s.get('status')} start={str(s.get('startTime','?'))[:19]}")
            if s.get("error"):
                print(f"  error: {s['error'].get('message','')[:200]}")
        else:
            print(f"{ds}/{scan}: no runs")
