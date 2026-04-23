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
            run = runs[0]
            print(f"=== {ds}/{scan} ===")
            print(f"  Status: {run.get('status')}")
            print(f"  Start:  {str(run.get('startTime','?'))[:19]}")
            print(f"  End:    {str(run.get('endTime','?'))[:19]}")
            print(f"  Assets discovered: {run.get('assetsDiscovered', '?')}")
            print(f"  Assets classified: {run.get('assetsClassified', '?')}")
            diag = run.get("diagnostics", {})
            if diag:
                exc_map = diag.get("exceptionCountMap", {})
                if exc_map:
                    print(f"  Exceptions: {exc_map}")
                notifs = diag.get("notifications", [])
                for n in notifs[:5]:
                    msg = n.get("message", "")[:200]
                    print(f"  Notification: {msg}")
            print()
