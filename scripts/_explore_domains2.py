"""Explore governance domains detail and linking APIs."""
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
DG = f"{ACCT}/datagovernance/catalog"
API = "api-version=2025-09-15-preview"

# 1. List all domains with full detail
print("=== ALL GOVERNANCE DOMAINS ===")
r = requests.get(f"{DG}/businessDomains?{API}", headers=h, timeout=15)
domains = r.json().get("value", [])
for d in domains:
    print(f"\n  Name: {d['name']}")
    print(f"  ID:   {d['id']}")
    print(f"  Type: {d.get('type','?')}")
    print(f"  Status: {d.get('status','?')}")
    # Show all keys
    for k, v in d.items():
        if k not in ("name", "id", "type", "status", "systemData"):
            val_str = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)
            if len(val_str) > 200:
                val_str = val_str[:200] + "..."
            print(f"  {k}: {val_str}")

# 2. Get details of first domain
if domains:
    dom_id = domains[0]["id"]
    print(f"\n=== DOMAIN DETAIL: {domains[0]['name']} ===")
    r2 = requests.get(f"{DG}/businessDomains/{dom_id}?{API}", headers=h, timeout=15)
    print(f"  Status: {r2.status_code}")
    if r2.status_code == 200:
        print(f"  Full: {json.dumps(r2.json(), ensure_ascii=False, indent=2)[:1500]}")

    # 3. Try sub-resources
    print(f"\n=== SUB-RESOURCES ===")
    for sub in ["glossaryTerms", "terms", "assets", "dataProducts", "dataAssets",
                "children", "members", "policies"]:
        r3 = requests.get(f"{DG}/businessDomains/{dom_id}/{sub}?{API}", headers=h, timeout=15)
        if r3.status_code != 404:
            resp = r3.text[:300] if r3.status_code == 200 else ""
            print(f"  {sub}: {r3.status_code} {resp}")

    # 4. Try PATCH/PUT to add glossary term link
    print(f"\n=== LINK API EXPLORATION ===")
    # Check if domain has any link-related fields
    for sub in ["links", "relationships", "associations"]:
        r4 = requests.get(f"{DG}/businessDomains/{dom_id}/{sub}?{API}", headers=h, timeout=15)
        if r4.status_code != 404:
            print(f"  {sub}: {r4.status_code} {r4.text[:300]}")

print("\nDone!")
