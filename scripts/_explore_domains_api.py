"""Explore governance domains API to link glossary terms."""
import requests, sys, os, json
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

ACCT = "https://prviewacc.purview.azure.com"
TENANT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
DG_BASE = f"{TENANT_EP}/datagovernance/catalog"

# 1. List governance domains
print("=== GOVERNANCE DOMAINS ===")
for api_ver in ["2025-09-15-preview", "2024-03-01-preview", "2023-10-01-preview"]:
    r = requests.get(f"{DG_BASE}/governanceDomains?api-version={api_ver}", headers=h, timeout=15)
    print(f"  governanceDomains ({api_ver}): {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"  Response keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
        items = data.get("value", data) if isinstance(data, dict) else data
        if isinstance(items, list):
            for d in items:
                print(f"    - {d.get('name','?')} | id={d.get('id','?')}")
        else:
            print(f"  Raw: {json.dumps(data, ensure_ascii=False)[:500]}")
        break

# 2. Try alternate endpoints
print("\n=== ALTERNATE ENDPOINTS ===")
for path in ["domains", "governanceDomains", "datadomains", "businessDomains"]:
    for base in [DG_BASE, f"{ACCT}/datagovernance/catalog"]:
        r = requests.get(f"{base}/{path}?api-version=2025-09-15-preview", headers=h, timeout=15)
        label = "tenant" if "purview-service" in base else "account"
        if r.status_code != 404:
            print(f"  {label}/{path}: {r.status_code}")
            if r.status_code == 200:
                print(f"    {json.dumps(r.json(), ensure_ascii=False)[:300]}")

# 3. Try the datagovernance root to discover available endpoints
print("\n=== API DISCOVERY ===")
for base in [DG_BASE, f"{ACCT}/datagovernance/catalog"]:
    label = "tenant" if "purview-service" in base else "account"
    r = requests.get(f"{base}?api-version=2025-09-15-preview", headers=h, timeout=15)
    print(f"  {label} root: {r.status_code}")
    if r.status_code == 200:
        print(f"    {json.dumps(r.json(), ensure_ascii=False)[:500]}")

# 4. Try unified governance API
print("\n=== UNIFIED GOVERNANCE ===")
for path in ["governanceDomains", "domains"]:
    for api_ver in ["2025-09-15-preview", "2024-03-01-preview"]:
        r = requests.get(f"{TENANT_EP}/datagovernance/{path}?api-version={api_ver}",
                         headers=h, timeout=15)
        if r.status_code != 404:
            print(f"  {path} ({api_ver}): {r.status_code}")
            if r.status_code == 200:
                print(f"    {json.dumps(r.json(), ensure_ascii=False)[:500]}")

print("\nDone!")
