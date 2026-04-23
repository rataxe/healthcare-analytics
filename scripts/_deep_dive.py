"""Deep-dive into collection assignment & portal config."""
import json, sys, os, requests
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try: sys.stdout.reconfigure(encoding="utf-8"); sys.stderr.reconfigure(encoding="utf-8")
    except: pass

from azure.identity import AzureCliCredential
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

ACCT = "https://prviewacc.purview.azure.com"
ATLAS = ACCT + "/catalog/api/atlas/v2"
SEARCH = ACCT + "/catalog/api/search/query?api-version=2022-08-01-preview"
TENANT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"

# 1. Root collection entities
print("=== ROOT COLLECTION ENTITIES (prviewacc) ===")
body = {"keywords": "*", "limit": 50, "filter": {"collectionId": "prviewacc"}}
r = requests.post(SEARCH, headers=h, json=body, timeout=30)
if r.status_code == 200:
    vals = r.json().get("value", [])
    total = r.json().get("@search.count", 0)
    print("Total:", total)
    type_counts = {}
    for v in vals:
        t = v.get("entityType", "?")
        type_counts[t] = type_counts.get(t, 0) + 1
        nm = v.get("name", "?")
        qn = v.get("qualifiedName", "")[:80]
        print("  {} | type={} | qn={}".format(nm, t, qn))
    print("\nType distribution (first 50):")
    for t, c in sorted(type_counts.items()):
        print("  {}: {}".format(t, c))

# 2. Process entity collection
print("\n=== PROCESS ENTITIES COLLECTION ===")
body2 = {"keywords": "*", "limit": 10, "filter": {"entityType": "Process"}}
r2 = requests.post(SEARCH, headers=h, json=body2, timeout=30)
if r2.status_code == 200:
    for v in r2.json().get("value", [])[:5]:
        guid = v.get("id", "")
        nm = v.get("name", "?")
        search_coll = v.get("collectionId", "?")
        r3 = requests.get(ATLAS + "/entity/guid/" + guid, headers=h, timeout=15)
        if r3.status_code == 200:
            ent = r3.json().get("entity", {})
            ent_coll = ent.get("collectionId", "NONE")
            print("  {} => search_coll={}, entity_coll={}".format(nm, search_coll, ent_coll))

# 3. Entity types in root
print("\n=== ENTITY TYPES IN ROOT ===")
for etype in ["Process", "healthcare_fhir_service", "healthcare_fhir_resource_type",
              "healthcare_dicom_service", "healthcare_dicom_modality", "healthcare_data_product",
              "azure_sql_table", "azure_sql_view", "fabric_lakehouse_table", "fabric_lake_warehouse"]:
    body3 = {"keywords": "*", "limit": 1, "filter": {"and": [{"entityType": etype}, {"collectionId": "prviewacc"}]}}
    r3 = requests.post(SEARCH, headers=h, json=body3, timeout=30)
    if r3.status_code == 200:
        cnt = r3.json().get("@search.count", 0)
        if cnt > 0:
            print("  {} in root: {}".format(etype, cnt))

# 4. Collections hierarchy
print("\n=== COLLECTIONS HIERARCHY ===")
r = requests.get(ACCT + "/account/collections?api-version=2019-11-01-preview", headers=h, timeout=15)
if r.status_code == 200:
    for c in r.json().get("value", []):
        nm = c.get("name", "?")
        fn = c.get("friendlyName", "?")
        parent = c.get("parentCollection", {}).get("referenceName", "ROOT")
        print("  {} ({}) -> parent={}".format(nm, fn, parent))
else:
    print("  HTTP", r.status_code)

# 5. Account details
print("\n=== ACCOUNT DETAILS ===")
r = requests.get(ACCT + "/account/?api-version=2019-11-01-preview", headers=h, timeout=15)
if r.status_code == 200:
    d = r.json()
    print("  name:", d.get("name","?"))
    props = d.get("properties", {})
    print("  provisioningState:", props.get("provisioningState","?"))
    print("  publicNetworkAccess:", props.get("publicNetworkAccess","?"))
    endpoints = props.get("endpoints", {})
    for k, v in endpoints.items():
        print("  endpoint {}: {}".format(k, v))
else:
    print("  HTTP", r.status_code)

# 6. New Purview portal 
print("\n=== NEW PURVIEW PORTAL / DATA GOVERNANCE ===")
for path in [
    "/datagovernance/catalog/glossaries",
    "/datagovernance/catalog/health",
]:
    r = requests.get(TENANT_EP + path + "?api-version=2025-09-15-preview", headers=h, timeout=15)
    print("  {} => {} {}".format(path, r.status_code, r.text[:200]))

# 7. Check Fabric scan exceptions detail
print("\n=== FABRIC SCAN EXCEPTIONS ===")
r = requests.get(ACCT + "/scan/datasources/Fabric/scans/Scan-IzR/runs?api-version=2022-07-01-preview", headers=h, timeout=15)
if r.status_code == 200:
    runs = r.json().get("value", [])
    if runs:
        latest = runs[0]
        diag = latest.get("diagnostics", {})
        notifications = diag.get("notifications", [])
        exc_map = diag.get("exceptionCountMap", {})
        print("  Status:", latest.get("status"))
        print("  Exceptions:", json.dumps(exc_map, indent=2)[:500])
        for n in notifications[:5]:
            print("  NOTIF:", n.get("message", "?")[:300])

# 8. Permissions - try correct API
print("\n=== PERMISSIONS (metadata roles) ===")
r = requests.get(ACCT + "/policystore/collections/prviewacc/metadataPolicy?api-version=2021-07-01", headers=h, timeout=15)
if r.status_code == 200:
    pol = r.json()
    rules = pol.get("properties", {}).get("attributeRules", [])
    for rule in rules:
        name = rule.get("name", "?")
        kind = rule.get("kind", "?")
        # Get member IDs
        members = []
        for cg in rule.get("dnfCondition", []):
            for cond in cg:
                vals = cond.get("attributeValueIncludedIn", [])
                members.extend(vals)
        print("  {} ({}): {}".format(name, kind, members[:3]))
else:
    print("  HTTP {} {}".format(r.status_code, r.text[:200]))

# 9. Check the custom classifications applied
print("\n=== CUSTOM CLASSIFICATIONS APPLIED ===")
for nm in ["ICD10_Diagnosis_Code", "Swedish_Personnummer", "FHIR_Resource_ID"]:
    body = {"keywords": "*", "limit": 5, "filter": {"classification": nm}}
    r = requests.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        cnt = r.json().get("@search.count", 0)
        print("  {}: {} entities".format(nm, cnt))
    else:
        print("  {}: HTTP {}".format(nm, r.status_code))

print("\n=== DONE ===")
