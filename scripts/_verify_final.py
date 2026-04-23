"""Final verification of complete Purview state after all fixes."""
import requests, sys, os, time
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
SEARCH = ACCT + "/catalog/api/search/query?api-version=2022-08-01-preview"

print("=" * 60)
print("  PURVIEW FINAL STATE VERIFICATION")
print("=" * 60)

# 1. Collection counts
print("\n1. COLLECTION ENTITY COUNTS:")
collections = {
    "prviewacc": "ROOT",
    "halsosjukvard": "Hälso & Sjukvård",
    "sql-databases": "SQL Databases",
    "fabric-analytics": "Fabric Analytics",
    "barncancer": "Barncancer",
    "fabric-brainchild": "Fabric BrainChild",
}
for cid, label in collections.items():
    r = requests.post(SEARCH, headers=h, json={"filter": {"collectionId": cid}, "limit": 1}, timeout=30)
    cnt = r.json().get("@search.count", 0)
    print(f"   {label:25s} ({cid:20s}): {cnt}")
    time.sleep(0.3)

# 2. Root breakdown - only glossary items should remain
print("\n2. ROOT ENTITY TYPE BREAKDOWN:")
r = requests.post(SEARCH, headers=h, json={
    "filter": {"collectionId": "prviewacc"},
    "limit": 200,
    "facets": [{"facet": "entityType", "count": 20}]
}, timeout=30)
facets = r.json().get("@search.facets", {}).get("entityType", [])
for f in facets:
    print(f"   {f.get('value', '?'):35s}: {f.get('count', 0)}")

# 3. Classifications
print("\n3. CLASSIFICATION COUNTS:")
for cls in ["Swedish Personnummer", "Patient Name PHI", "ICD10 Diagnosis Code",
            "SNOMED CT Code", "FHIR Resource ID", "OMOP Concept ID"]:
    r = requests.post(SEARCH, headers=h, json={
        "filter": {"classification": cls}, "limit": 1
    }, timeout=30)
    cnt = r.json().get("@search.count", 0)
    print(f"   {cls:25s}: {cnt}")
    time.sleep(0.3)

# 4. Glossary terms count
print("\n4. GLOSSARY:")
r = requests.post(SEARCH, headers=h, json={
    "filter": {"entityType": "AtlasGlossaryTerm"}, "limit": 1
}, timeout=30)
terms = r.json().get("@search.count", 0)
print(f"   Terms: {terms}")

# 5. Lineage (Process entities)
print("\n5. LINEAGE (Process entities):")
r = requests.post(SEARCH, headers=h, json={
    "filter": {"entityType": "Process"}, "limit": 1
}, timeout=30)
procs = r.json().get("@search.count", 0)
print(f"   Processes: {procs}")

# Check where they are now
r = requests.post(SEARCH, headers=h, json={
    "filter": {"and": [{"entityType": "Process"}, {"collectionId": "halsosjukvard"}]},
    "limit": 1
}, timeout=30)
hprocs = r.json().get("@search.count", 0)
print(f"   In halsosjukvard: {hprocs}")

# 6. Custom entity types
print("\n6. CUSTOM ENTITY TYPES:")
for et in ["healthcare_fhir_service", "healthcare_fhir_resource_type",
           "healthcare_dicom_service", "healthcare_dicom_modality",
           "healthcare_data_product"]:
    r = requests.post(SEARCH, headers=h, json={
        "filter": {"entityType": et}, "limit": 1
    }, timeout=30)
    cnt = r.json().get("@search.count", 0)
    print(f"   {et:35s}: {cnt}")
    time.sleep(0.3)

# 7. Total asset count
print("\n7. TOTAL ASSETS:")
r = requests.post(SEARCH, headers=h, json={"filter": {}, "limit": 1}, timeout=30)
total = r.json().get("@search.count", 0)
print(f"   Total searchable entities: {total}")

print("\n" + "=" * 60)
print("  VERIFICATION COMPLETE")
print("=" * 60)
