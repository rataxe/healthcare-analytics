"""Check Fabric scan details."""
import requests, json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}"}

for scan_name in ["Scan-1fv", "Scan-ouL"]:
    url = f"{SCAN_EP}/scan/datasources/Fabric/scans/{scan_name}?api-version=2023-09-01"
    r = requests.get(url, headers=h)
    if r.status_code == 200:
        scan = r.json()
        props = scan.get("properties", {})
        print(f"--- {scan_name} ---")
        print(f"  kind: {scan.get('kind')}")
        coll = props.get("collection", {}).get("referenceName", "")
        print(f"  collection: {coll}")
        scope = props.get("scanScope", {})
        print(f"  scopeType: {scope.get('scopeType', '')}")
        items = scope.get("fabricItems", scope.get("workspaces", []))
        print(f"  items: {json.dumps(items)[:500]}")
        print()

# Latest scan run details
url2 = f"{SCAN_EP}/scan/datasources/Fabric/scans/Scan-1fv/runs?api-version=2023-09-01"
r2 = requests.get(url2, headers=h)
if r2.status_code == 200:
    runs = r2.json().get("value", [])
    if runs:
        latest = runs[0]
        print("Latest Scan-1fv run:")
        print(f"  status: {latest.get('status')}")
        err = latest.get("error", {})
        if err:
            print(f"  error: {err.get('message', '')[:200]}")
        diag = latest.get("scanDiagnostics", {})
        if diag:
            print(f"  diagnostics: {json.dumps(diag)[:300]}")

# Also check the Fabric data source detail
url3 = f"{SCAN_EP}/scan/datasources/Fabric?api-version=2023-09-01"
r3 = requests.get(url3, headers=h)
if r3.status_code == 200:
    ds = r3.json()
    print(f"\n--- Fabric data source ---")
    print(f"  kind: {ds.get('kind')}")
    props = ds.get("properties", {})
    print(f"  tenant: {props.get('tenant', '')}")
    print(f"  collection: {props.get('collection',{}).get('referenceName','')}")
