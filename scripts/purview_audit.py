"""Audit current Purview configuration — data sources, collections, rulesets, scans."""
import requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ACCOUNT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb.purview.azure.com"

token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}"}

# 1. Data sources
print("=== Data Sources ===")
r = requests.get(f"{SCAN_EP}/scan/datasources?api-version=2023-09-01", headers=h)
if r.status_code == 200:
    for ds in r.json().get("value", []):
        kind = ds.get("kind", "")
        props = ds.get("properties", {})
        endpoint = props.get("serverEndpoint", props.get("endpoint", ""))
        coll = props.get("collection", {}).get("referenceName", "")
        print(f"  {ds['name']:35s} | kind={kind:25s} | collection={coll} | endpoint={endpoint}")
else:
    print(f"  Error: {r.status_code}")

# 2. Collections
print("\n=== Collections ===")
r2 = requests.get(f"{ACCOUNT_EP}/collections?api-version=2019-11-01-preview", headers=h)
if r2.status_code == 200:
    for c in r2.json().get("value", []):
        parent = c.get("parentCollection", {}).get("referenceName", "-")
        print(f"  {c['name']:35s} | friendly={c.get('friendlyName',''):20s} | parent={parent}")
else:
    print(f"  Error: {r2.status_code} {r2.text[:300]}")

# 3. Scan rulesets
print("\n=== Scan Rulesets (custom) ===")
r3 = requests.get(f"{SCAN_EP}/scan/scanrulesets?api-version=2023-09-01", headers=h)
if r3.status_code == 200:
    for rs in r3.json().get("value", []):
        if rs.get("scanRulesetType") == "Custom":
            print(f"  {rs['name']:35s} | kind={rs.get('kind','')} | type={rs.get('scanRulesetType','')}")
else:
    print(f"  Error: {r3.status_code}")

# 4. Custom classification rules
print("\n=== Custom Classification Rules ===")
r4 = requests.get(f"{SCAN_EP}/scan/classificationrules?api-version=2023-09-01", headers=h)
if r4.status_code == 200:
    rules = [x for x in r4.json().get("value", []) if x.get("classificationRuleType") == "Custom"]
    if rules:
        for rule in rules:
            print(f"  {rule['name']:35s} | kind={rule.get('kind','')}")
    else:
        print("  (none)")
else:
    print(f"  Error: {r4.status_code}")

# 5. Scans per datasource
print("\n=== Scans ===")
for ds in (r.json().get("value", []) if r.status_code == 200 else []):
    ds_name = ds["name"]
    r5 = requests.get(f"{SCAN_EP}/scan/datasources/{ds_name}/scans?api-version=2023-09-01", headers=h)
    if r5.status_code == 200:
        scans = r5.json().get("value", [])
        for scan in scans:
            print(f"  {ds_name}/{scan['name']:25s} | kind={scan.get('kind','')}")
            # Get latest run
            r6 = requests.get(
                f"{SCAN_EP}/scan/datasources/{ds_name}/scans/{scan['name']}/runs?api-version=2023-09-01",
                headers=h,
            )
            if r6.status_code == 200:
                runs = r6.json().get("value", [])
                if runs:
                    latest = runs[0]
                    print(f"    Latest run: status={latest.get('status')} start={latest.get('startTime','')[:19]}")
                else:
                    print("    No runs yet")
    else:
        print(f"  {ds_name}: no scans ({r5.status_code})")

# 6. Glossary
print("\n=== Glossary Terms ===")
r7 = requests.get(f"{ACCOUNT_EP}/catalog/api/glossary?api-version=2022-03-01-preview", headers=h)
if r7.status_code == 200:
    glossaries = r7.json()
    if isinstance(glossaries, list):
        for g in glossaries:
            count = len(g.get("terms", []))
            print(f"  {g.get('name',''):35s} | terms={count}")
    else:
        print(f"  {glossaries}")
else:
    print(f"  Error: {r7.status_code}")
