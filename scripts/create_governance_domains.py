"""Probe and create Purview governance domains."""
import requests, time, json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

ACCT = "https://prviewacc.purview.azure.com"

print("=" * 60)
print("1. PROBING GOVERNANCE DOMAIN ENDPOINTS")
print("=" * 60)

endpoints = [
    ("Unified Catalog domains 2025-09", f"{ACCT}/datagovernance/catalog/domains", "2025-09-15-preview"),
    ("Unified Catalog domains 2025-02", f"{ACCT}/datagovernance/catalog/domains", "2025-02-01-preview"),
    ("Unified Catalog domains 2024-11", f"{ACCT}/datagovernance/catalog/domains", "2024-11-01-preview"),
    ("DataMap governance-domains 2023-10", f"{ACCT}/datamap/api/governance-domains", "2023-10-01-preview"),
    ("DataMap governance-domains 2024-03", f"{ACCT}/datamap/api/governance-domains", "2024-03-01"),
    ("DataMap governance-domains 2023-02", f"{ACCT}/datamap/api/governance-domains", "2023-02-01-preview"),
    ("DataGovernance domains no ver", f"{ACCT}/datagovernance/catalog/domains", "2024-05-01-preview"),
]

working_endpoint = None
working_ver = None

for label, url, ver in endpoints:
    full = f"{url}?api-version={ver}"
    r = sess.get(full, headers=h, timeout=30)
    print(f"  {label}: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        items = data.get("value", []) if isinstance(data, dict) else data
        print(f"    -> {len(items)} domains found")
        for item in items[:5]:
            print(f"       - {item.get('name', item.get('id', '?'))}")
        if working_endpoint is None:
            working_endpoint = url
            working_ver = ver
    elif r.status_code != 404:
        txt = r.text[:150]
        print(f"    -> {txt}")
    time.sleep(0.5)

if not working_endpoint:
    print("\nNo working GET endpoint found. Trying POST to create directly...")
    
    # Try creating with POST on each endpoint
    test_domain = {
        "name": "Klinisk Vård",
        "description": "Domän för klinisk patientdata",
        "type": "governance-domain",
    }
    
    create_endpoints = [
        (f"{ACCT}/datagovernance/catalog/domains", "2025-09-15-preview"),
        (f"{ACCT}/datagovernance/catalog/domains", "2025-02-01-preview"),
        (f"{ACCT}/datagovernance/catalog/domains", "2024-11-01-preview"),
        (f"{ACCT}/datamap/api/governance-domains", "2023-10-01-preview"),
    ]
    
    for url, ver in create_endpoints:
        full = f"{url}?api-version={ver}"
        r = sess.post(full, headers=h, json=test_domain, timeout=30)
        print(f"\n  POST {url} (v={ver}): {r.status_code}")
        if r.status_code in (200, 201):
            print(f"    -> CREATED: {json.dumps(r.json(), indent=2, ensure_ascii=False)[:300]}")
            working_endpoint = url
            working_ver = ver
            break
        elif r.status_code == 409:
            print(f"    -> Already exists!")
            working_endpoint = url
            working_ver = ver
            break
        else:
            print(f"    -> {r.text[:200]}")
        time.sleep(0.5)

print(f"\nWorking endpoint: {working_endpoint}")
print(f"Working version: {working_ver}")

if working_endpoint:
    print("\n" + "=" * 60)
    print("2. CREATING GOVERNANCE DOMAINS")
    print("=" * 60)
    
    domains = [
        {
            "name": "Klinisk Vård",
            "description": (
                "Domän för klinisk patientdata — vårdbesök, diagnoser, medicinering, "
                "labresultat och ML-prediktioner. Källa: Azure SQL + Fabric Lakehouse "
                "(Healthcare-Analytics). Standarder: ICD-10, ATC, FHIR R4, OMOP CDM."
            ),
        },
        {
            "name": "Barncancerforskning",
            "description": (
                "Domän för barncancerforskningsdata — FHIR-resurser, DICOM-bilder, "
                "genomik (VCF), biobanksdata (BTB), GMS och SBCR-register. "
                "Källa: Fabric Lakehouse (BrainChild-Demo). Standarder: FHIR R4, DICOM, VCF, GMS."
            ),
        },
    ]
    
    for domain in domains:
        url = f"{working_endpoint}?api-version={working_ver}"
        r = sess.post(url, headers=h, json=domain, timeout=30)
        if r.status_code in (200, 201):
            resp = r.json()
            print(f"  OK  Created '{domain['name']}' (id={resp.get('id', resp.get('guid', '?'))})")
        elif r.status_code == 409:
            print(f"  OK  '{domain['name']}' already exists")
        else:
            print(f"  FAIL  '{domain['name']}': {r.status_code} — {r.text[:200]}")
        time.sleep(0.5)
    
    # Verify
    print("\n" + "=" * 60)
    print("3. VERIFY DOMAINS")
    print("=" * 60)
    r = sess.get(f"{working_endpoint}?api-version={working_ver}", headers=h, timeout=30)
    if r.status_code == 200:
        data = r.json()
        items = data.get("value", []) if isinstance(data, dict) else data
        for item in items:
            print(f"  - {item.get('name', '?')}: {item.get('description', '')[:80]}")
    else:
        print(f"  Verify failed: {r.status_code}")
else:
    print("\n  !! No working API endpoint found")
    print("  Governance domains must be created manually in Purview portal:")
    print("  1. Go to https://purview.microsoft.com")
    print("  2. Data Catalog -> Governance domains")
    print("  3. Create 'Klinisk Vård' and 'Barncancerforskning'")
