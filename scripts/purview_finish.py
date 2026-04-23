"""Add remaining glossary terms + trigger scans."""
import time
import requests
from azure.identity import AzureCliCredential

ACCOUNT_EP = "https://prviewacc.purview.azure.com"
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
API_VER = "2023-09-01"

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

atlas_base = f"{ACCOUNT_EP}/catalog/api/atlas/v2"
glossary_guid = "51628c3a-2e45-41e6-b532-127ec6abc13a"
term_url = f"{atlas_base}/glossary/term"

# Remaining 3 terms
terms = [
    ("Bronze Layer", "Raw data ingestion — exact copies in Delta Lake"),
    ("Silver Layer", "Cleaned and enriched data — ML features"),
    ("Gold Layer", "Business-ready analytics — ML predictions plus OMOP CDM"),
]

print("1. Add remaining glossary terms")
print("=" * 50)
for name, desc in terms:
    body = {
        "name": name,
        "qualifiedName": f"{name.replace(' ', '_')}@Glossary",
        "shortDescription": desc,
        "anchor": {"glossaryGuid": glossary_guid},
    }
    r = requests.post(term_url, headers=h, json=body)
    if r.status_code in (200, 201):
        print(f"  ✅ {name}")
    elif r.status_code == 409:
        print(f"  ℹ️  {name} (already exists)")
    else:
        print(f"  ⚠️ {name}: {r.status_code}")

# Trigger scans
print("\n2. Trigger scans")
print("=" * 50)
for ds, scan in [("sql-hca-demo", "healthcare-scan"), ("Fabric", "Scan-HCA"), ("Fabric", "Scan-BrainChild")]:
    run_id = f"run-{int(time.time())}"
    url = f"{SCAN_EP}/scan/datasources/{ds}/scans/{scan}/runs/{run_id}?api-version={API_VER}"
    r = requests.put(url, headers=h, json={})
    status = "✅" if r.status_code in (200, 201, 202) else "⚠️"
    print(f"  {status} {ds}/{scan}: {r.status_code}")
    if r.status_code not in (200, 201, 202):
        print(f"     {r.text[:200]}")
    time.sleep(2)

print("\nDone! Check scan status in ~2 minutes.")
