"""
Complete Purview Verification
==============================
Verifies all aspects of the Purview governance setup.
"""
import sys
import requests
from azure.identity import AzureCliCredential

PURVIEW_ACCOUNT = "prviewacc"
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
ATLAS_API = f"{PURVIEW_ENDPOINT}/catalog/api/atlas/v2"

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def section(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print("=" * 70)

def check(label, condition):
    status = "OK" if condition else "MISSING"
    symbol = "[+]" if condition else "[!]"
    print(f"  {symbol} {label}: {status}")
    return condition

# ══════════════════════════════════════════════════════════
section("1. GLOSSARY STATUS")
# ══════════════════════════════════════════════════════════
r = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=30)
if r.status_code == 200:
    data = r.json()
    glist = data if isinstance(data, list) else [data]
    glossary = glist[0]
    g_guid = glossary["guid"]
    g_name = glossary.get("name", "?")
    
    print(f"  Glossary: {g_name} ({g_guid})")
    
    # Get categories
    r2 = requests.get(f"{ATLAS_API}/glossary/{g_guid}", headers=headers, timeout=30)
    if r2.status_code == 200:
        cats = r2.json().get("categories", [])
        check(f"Categories", len(cats) >= 5)
        for cat in cats:
            print(f"    - {cat.get('displayText', '?')}")
    
    # Get terms count
    r3 = requests.get(f"{ATLAS_API}/glossary/{g_guid}/terms?limit=200&offset=0", headers=headers, timeout=30)
    if r3.status_code == 200:
        terms = r3.json()
        check(f"Terms", len(terms) >= 145)
else:
    print(f"  [!] Glossary check failed: {r.status_code}")

# ══════════════════════════════════════════════════════════
section("2. CUSTOM CLASSIFICATIONS")
# ══════════════════════════════════════════════════════════
custom_classifications = [
    "Swedish Personnummer",
    "SNOMED CT Code",
    "OMOP Concept ID",
    "ICD10Code",
    "ATCCode",
    "LOINCCode"
]

found_count = 0
for class_name in custom_classifications:
    r = requests.get(f"{ATLAS_API}/types/typedef/name/{class_name}", headers=headers, timeout=15)
    if r.status_code == 200:
        print(f"  [+] {class_name}: OK")
        found_count += 1
    else:
        print(f"  [!] {class_name}: MISSING")

check("Custom classifications", found_count == len(custom_classifications))

# ══════════════════════════════════════════════════════════
section("3. COLLECTIONS")
# ══════════════════════════════════════════════════════════
r = requests.get(f"{PURVIEW_ENDPOINT}/account/collections?api-version=2019-11-01-preview",
                headers=headers, timeout=30)
if r.status_code == 200:
    colls = r.json().get("value", [])
    print(f"  Total collections: {len(colls)}")
    
    expected_colls = ["brainchild-fhir", "barncancer", "omop-data", "fabric-data"]
    found_colls = []
    
    for coll in colls:
        cname = coll.get("friendlyName", coll.get("name", "?"))
        print(f"    - {cname}")
        for exp in expected_colls:
            if exp.lower() in cname.lower():
                found_colls.append(exp)
    
    check("Expected collections", len(set(found_colls)) >= 4)
else:
    print(f"  [!] Collections check failed: {r.status_code}")

# ══════════════════════════════════════════════════════════
section("4. DATA PRODUCTS")
# ══════════════════════════════════════════════════════════
# Try to search for data product entities
body = {
    "keywords": "*",
    "limit": 50,
    "filter": {
        "or": [
            {"entityType": "healthcare_data_product"},
            {"entityType": "data_product"},
            {"entityType": "DataProduct"}
        ]
    }
}

r = requests.post(f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
                 headers=headers, json=body, timeout=30)

if r.status_code == 200:
    results = r.json().get("value", [])
    print(f"  Data products found: {len(results)}")
    
    for dp in results[:10]:  # Show first 10
        name = dp.get("name", "?")
        etype = dp.get("entityType", "?")
        print(f"    - {name} ({etype})")
    
    check("Data products", len(results) >= 4)
else:
    print(f"  [!] Data products search failed: {r.status_code}")
    # Try alternate search
    body2 = {"keywords": "data product", "limit": 50}
    r2 = requests.post(f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
                      headers=headers, json=body2, timeout=30)
    if r2.status_code == 200:
        results2 = r2.json().get("value", [])
        print(f"  Alternate search found: {len(results2)} results")

# ══════════════════════════════════════════════════════════
section("5. GOVERNANCE DOMAINS")
# ══════════════════════════════════════════════════════════
# Try to search for domain entities
body = {"keywords": "domain", "limit": 50}
r = requests.post(f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
                 headers=headers, json=body, timeout=30)

if r.status_code == 200:
    results = r.json().get("value", [])
    
    # Filter for actual domains
    domains = [x for x in results if "domain" in x.get("entityType", "").lower() or 
               "domain" in x.get("name", "").lower()]
    
    print(f"  Domain entities found: {len(domains)}")
    for dom in domains[:10]:
        name = dom.get("name", "?")
        etype = dom.get("entityType", "?")
        print(f"    - {name} ({etype})")
    
    check("Governance domains", len(domains) >= 4)
else:
    print(f"  [!] Domain search failed: {r.status_code}")

# ══════════════════════════════════════════════════════════
section("6. LINEAGE PROCESSES")
# ══════════════════════════════════════════════════════════
body = {"keywords": "*", "limit": 100, "filter": {"entityType": "Process"}}
r = requests.post(f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
                 headers=headers, json=body, timeout=30)

if r.status_code == 200:
    results = r.json().get("value", [])
    print(f"  Lineage processes found: {len(results)}")
    
    # Show sample processes
    for proc in results[:5]:
        name = proc.get("name", "?")
        qname = proc.get("qualifiedName", "?")
        print(f"    - {name}")
    
    check("Lineage processes", len(results) >= 20)
else:
    print(f"  [!] Lineage search failed: {r.status_code}")

# ══════════════════════════════════════════════════════════
section("7. SCANNED ASSETS")
# ══════════════════════════════════════════════════════════
# Search for Fabric lakehouses
body = {"keywords": "*", "limit": 100, "filter": {"entityType": "microsoft_fabric_lakehouse"}}
r = requests.post(f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
                 headers=headers, json=body, timeout=30)

if r.status_code == 200:
    lakehouses = r.json().get("value", [])
    print(f"  Fabric Lakehouses: {len(lakehouses)}")
    for lh in lakehouses:
        print(f"    - {lh.get('name', '?')}")
    
    check("Fabric lakehouses scanned", len(lakehouses) >= 3)
else:
    print(f"  [!] Lakehouse search failed: {r.status_code}")

# Search for SQL tables
body = {"keywords": "*", "limit": 100, "filter": {"entityType": "azure_sql_table"}}
r = requests.post(f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
                 headers=headers, json=body, timeout=30)

if r.status_code == 200:
    tables = r.json().get("value", [])
    print(f"  SQL Tables: {len(tables)}")
    check("SQL tables scanned", len(tables) >= 10)
else:
    print(f"  [!] SQL table search failed: {r.status_code}")

# ══════════════════════════════════════════════════════════
section("8. SUMMARY")
# ══════════════════════════════════════════════════════════
print("\n  Purview governance verification complete!")
print("  Review results above for any MISSING components.")
