"""
Move all non-glossary root entities to correct collections using moveTo API.
Correct endpoint: POST /datamap/api/entity/moveTo?collectionId={target}&api-version=2023-09-01
Body: {"entityGuids": [...]}
"""
import requests, sys, os, time, json
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

# Mapping: entity type -> target collection
TYPE_COLLECTION = {
    "Process": "halsosjukvard",
    "healthcare_fhir_resource_type": "fabric-brainchild",
    "healthcare_fhir_service": "fabric-brainchild",
    "healthcare_dicom_service": "fabric-brainchild",
    "healthcare_dicom_modality": "fabric-brainchild",
    "healthcare_data_product": "halsosjukvard",
    "azure_sql_table": "sql-databases",
    "azure_sql_schema": "sql-databases",
    "azure_sql_db": "sql-databases",
    "azure_sql_view": "sql-databases",
}

SKIP_TYPES = {"AtlasGlossaryTerm", "AtlasGlossaryCategory", "AtlasGlossary"}

# Step 1: Gather all root entities
print("=== Gathering root entities ===")
all_root = []
offset = 0
while True:
    r = requests.post(SEARCH, headers=h, json={
        "filter": {"collectionId": "prviewacc"},
        "limit": 50,
        "offset": offset
    }, timeout=30)
    data = r.json()
    vals = data.get("value", [])
    total = data.get("@search.count", 0)
    all_root.extend(vals)
    if len(all_root) >= total or not vals:
        break
    offset += len(vals)
    time.sleep(0.5)

print(f"Total root entities: {len(all_root)}")

# Step 2: Group by target collection
by_collection = {}
skipped = 0
unknown_types = []

for ent in all_root:
    etype = ent.get("entityType", "unknown")
    guid = ent["id"]
    name = ent.get("name", "?")

    if etype in SKIP_TYPES or etype == "unknown":
        skipped += 1
        continue

    target = TYPE_COLLECTION.get(etype)
    if not target:
        unknown_types.append((etype, name, guid))
        continue

    if target not in by_collection:
        by_collection[target] = []
    by_collection[target].append((guid, etype, name))

print(f"Skipped (glossary/unknown): {skipped}")
if unknown_types:
    print(f"Unrecognized types: {unknown_types}")

for coll, entities in by_collection.items():
    types = {}
    for _, et, _ in entities:
        types[et] = types.get(et, 0) + 1
    print(f"  -> {coll}: {len(entities)} entities {types}")

# Step 3: Move in batches per collection (max 25 per batch)
BATCH_SIZE = 25

for coll, entities in by_collection.items():
    guids = [g for g, _, _ in entities]
    print(f"\n=== Moving {len(guids)} entities to {coll} ===")

    for i in range(0, len(guids), BATCH_SIZE):
        batch = guids[i:i + BATCH_SIZE]
        time.sleep(1)
        url = f"{ACCT}/datamap/api/entity/moveTo?collectionId={coll}&api-version=2023-09-01"
        r = requests.post(url, headers=h, json={"entityGuids": batch}, timeout=60)
        if r.status_code == 200:
            mutated = r.json().get("mutatedEntities", {}).get("UPDATE", [])
            print(f"  Batch {i // BATCH_SIZE + 1}: OK, {len(mutated)} updated")
        else:
            print(f"  Batch {i // BATCH_SIZE + 1}: FAILED {r.status_code}")
            print(f"  {r.text[:200]}")

# Step 4: Verify
print("\n=== Verification (waiting 5s for index) ===")
time.sleep(5)

collections = ["prviewacc", "halsosjukvard", "sql-databases", "fabric-analytics", "fabric-brainchild", "barncancer"]
for coll in collections:
    r = requests.post(SEARCH, headers=h, json={"filter": {"collectionId": coll}, "limit": 1}, timeout=30)
    cnt = r.json().get("@search.count", 0)
    print(f"  {coll}: {cnt}")
    time.sleep(0.3)

print("\nDone!")
