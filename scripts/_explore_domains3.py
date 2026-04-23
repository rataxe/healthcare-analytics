"""Try linking glossary terms to governance domains via various API patterns."""
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
DG = f"{ACCT}/datagovernance/catalog"
DG_T = f"{TENANT_EP}/datagovernance/catalog"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
API = "api-version=2025-09-15-preview"

KLINISK_VARD_ID = "0a57cab0-2b5e-4e0d-bb9f-1643902355ec"

# Get a sample term GUID for testing (Personnummer = clearly Klinisk Data)
r = requests.get(f"{ATLAS}/glossary/d939ea20-9c67-48af-98d9-b66965f7cde1/terms?limit=200&offset=0",
                 headers=h, timeout=15)
terms = r.json()
test_term = next((t for t in terms if t["name"] == "Personnummer"), None)
if not test_term:
    print("Test term not found!")
    sys.exit(1)
term_guid = test_term["guid"]
print(f"Test term: {test_term['name']} ({term_guid})")

# 1. Try PATCH domain to add glossaryTerms
print("\n=== 1. PATCH domain with glossaryTerms ===")
patch_body = {"glossaryTerms": [{"termGuid": term_guid}]}
r1 = requests.patch(f"{DG}/businessDomains/{KLINISK_VARD_ID}?{API}",
                    headers=h, json=patch_body, timeout=15)
print(f"  PATCH: {r1.status_code} {r1.text[:300]}")

# 2. Try adding domain ref to the glossary term via Atlas
print("\n=== 2. Add businessDomain to glossary term ===")
r2 = requests.get(f"{ATLAS}/glossary/term/{term_guid}", headers=h, timeout=15)
full_term = r2.json()
# Check what fields are available
interesting = {k: v for k, v in full_term.items() if k not in ("guid", "qualifiedName", "name",
               "shortDescription", "longDescription", "anchor", "categories", "resources",
               "createTime", "updateTime", "createdBy", "updatedBy", "status")}
print(f"  Term extra fields: {list(interesting.keys())}")
print(f"  Full keys: {list(full_term.keys())}")

# 3. Try POST to link endpoint
print("\n=== 3. POST link term to domain ===")
for base in [DG, DG_T]:
    label = "account" if base == DG else "tenant"
    link_body = {"termId": term_guid}
    r3 = requests.post(f"{base}/businessDomains/{KLINISK_VARD_ID}/glossaryTerms?{API}",
                       headers=h, json=link_body, timeout=15)
    print(f"  {label} POST glossaryTerms: {r3.status_code} {r3.text[:200]}")

    link_body2 = {"glossaryTermId": term_guid}
    r4 = requests.post(f"{base}/businessDomains/{KLINISK_VARD_ID}/terms?{API}",
                       headers=h, json=link_body2, timeout=15)
    print(f"  {label} POST terms: {r4.status_code} {r4.text[:200]}")

# 4. Try adding businessDomainId to term
print("\n=== 4. PUT term with businessDomainId ===")
full_term["businessDomainId"] = KLINISK_VARD_ID
r5 = requests.put(f"{ATLAS}/glossary/term/{term_guid}", headers=h, json=full_term, timeout=15)
print(f"  PUT: {r5.status_code}")
if r5.status_code == 200:
    result = r5.json()
    bd = result.get("businessDomainId")
    print(f"  businessDomainId in response: {bd}")

# 5. Check datagovernance glossaryTerms endpoint
print("\n=== 5. datagovernance glossaryTerms ===")
for base in [DG, DG_T]:
    label = "account" if base == DG else "tenant"
    r6 = requests.get(f"{base}/glossaryTerms?{API}", headers=h, timeout=15)
    if r6.status_code != 404:
        print(f"  {label} glossaryTerms: {r6.status_code} {r6.text[:300]}")
    r7 = requests.get(f"{base}/glossaryTerms/{term_guid}?{API}", headers=h, timeout=15)
    if r7.status_code != 404:
        print(f"  {label} glossaryTerms/{term_guid}: {r7.status_code} {r7.text[:300]}")

# 6. Check the new unified term API
print("\n=== 6. Unified term API ===")
for base in [DG, DG_T]:
    label = "account" if base == DG else "tenant"
    r8 = requests.get(f"{base}/terms/{term_guid}?{API}", headers=h, timeout=15)
    if r8.status_code != 404:
        print(f"  {label} terms/{term_guid}: {r8.status_code}")
        if r8.status_code == 200:
            data = r8.json()
            print(f"    Keys: {list(data.keys())}")
            bd = data.get("businessDomain") or data.get("governanceDomain") or data.get("domain")
            print(f"    domain ref: {bd}")
            print(f"    Full: {json.dumps(data, ensure_ascii=False)[:500]}")

print("\nDone!")
