"""Investigate SQL entity details + column attributes for classification targets."""
import requests, json, sys, os
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
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"

# ============================================================
# 1. ALL SQL ENTITIES
# ============================================================
print("=" * 70)
print("1. SQL ENTITIES IN sql-databases COLLECTION")
print("=" * 70)
body = {"filter": {"collectionId": "sql-databases"}, "limit": 50}
r = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r.status_code == 200:
    ents = r.json().get("value", [])
    print(f"  Total: {r.json().get('@search.count', 0)}")
    for e in sorted(ents, key=lambda x: x.get("entityType", "")):
        raw_cls = e.get("classification", [])
        cls = []
        if isinstance(raw_cls, list):
            cls = [c.get("typeName", "?") if isinstance(c, dict) else str(c) for c in raw_cls]
        raw_terms = e.get("term", [])
        terms = []
        if isinstance(raw_terms, list):
            terms = [t.get("displayText", "?") if isinstance(t, dict) else str(t) for t in raw_terms]
        print(f"  [{e.get('entityType')}] {e.get('name','?')}")
        if cls:
            print(f"       classifications: {cls}")
        if terms:
            print(f"       terms: {terms}")

# ============================================================
# 2. GET FULL ENTITY DETAILS FOR OMOP TABLES
# ============================================================
print("\n" + "=" * 70)
print("2. OMOP TABLE ENTITIES - columns and classifications")
print("=" * 70)
omop_tables = ["condition_occurrence", "drug_exposure", "measurement", "person",
               "specimen", "visit_occurrence", "diagnoses", "medications",
               "lab_results", "vital_signs", "encounters"]
for tbl in omop_tables:
    body = {"keywords": tbl, "filter": {"entityType": "azure_sql_table"}, "limit": 3}
    r2 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if r2.status_code == 200:
        results = r2.json().get("value", [])
        for e in results:
            guid = e.get("id")
            print(f"\n  TABLE: {e.get('name')} (guid={guid})")
            # Get full entity to see columns
            r3 = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
            if r3.status_code == 200:
                ent = r3.json().get("entity", {})
                attrs = ent.get("attributes", {})
                # Check for columns in relationshipAttributes
                rel_attrs = ent.get("relationshipAttributes", {})
                columns = rel_attrs.get("columns", [])
                print(f"    columns count: {len(columns)}")
                for col in columns[:20]:
                    col_name = col.get("displayText", "?")
                    col_guid = col.get("guid", "?")
                    print(f"      - {col_name} (guid={col_guid})")
                # Check existing classifications
                cls_list = ent.get("classifications", [])
                if cls_list:
                    print(f"    classifications: {[c.get('typeName') for c in cls_list]}")

# ============================================================
# 3. FABRIC-BRAINCHILD ENTITIES - candidates for barncancer
# ============================================================
print("\n" + "=" * 70)
print("3. ENTITIES THAT COULD MOVE TO BARNCANCER")
print("=" * 70)
# Data product "BrainChild Barncancerforskning" is in halsosjukvard - should it be in barncancer?
body = {"keywords": "brainchild", "limit": 20}
r4 = requests.post(SEARCH, headers=h, json=body, timeout=15)
if r4.status_code == 200:
    for e in r4.json().get("value", []):
        print(f"  [{e.get('entityType')}] {e.get('name','?')} -> coll={e.get('collectionId')}")

# ============================================================
# 4. FABRIC ENTITY TYPE FULL COUNTS
# ============================================================
print("\n" + "=" * 70)
print("4. FABRIC ENTITY TYPES - full counts")
print("=" * 70)
for ft in ["fabric_lakehouse_table", "fabric_lakehouse_path", "fabric_lake_warehouse",
           "powerbi_dataset", "powerbi_report", "powerbi_workspace", "powerbi_dashboard",
           "powerbi_dataflow", "powerbi_measure", "powerbi_page", "powerbi_tile",
           "powerbi_column", "powerbi_table",
           "azure_datalake_gen2_path", "azure_datalake_gen2_filesystem",
           "azure_datalake_gen2_service", "azure_datalake_gen2_resource_set"]:
    body = {"filter": {"entityType": ft}, "limit": 1}
    r5 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if r5.status_code == 200:
        cnt = r5.json().get("@search.count", 0)
        if cnt > 0:
            print(f"  {ft}: {cnt}")

# ============================================================
# 5. CHECK WHICH COLUMNS HAVE concept_id IN OMOP TABLES
# ============================================================
print("\n" + "=" * 70)
print("5. COLUMNS WITH concept_id / snomed IN OMOP TABLES")
print("=" * 70)
for tbl in ["condition_occurrence", "drug_exposure", "measurement", "person",
            "specimen", "visit_occurrence"]:
    body = {"keywords": tbl, "filter": {"entityType": "azure_sql_table"}, "limit": 1}
    r6 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if r6.status_code == 200:
        results = r6.json().get("value", [])
        if results:
            guid = results[0].get("id")
            r7 = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
            if r7.status_code == 200:
                ent = r7.json().get("entity", {})
                columns = ent.get("relationshipAttributes", {}).get("columns", [])
                concept_cols = [c for c in columns if "concept_id" in c.get("displayText", "").lower()]
                snomed_cols = [c for c in columns if "snomed" in c.get("displayText", "").lower()]
                if concept_cols:
                    print(f"  {tbl}: concept_id columns:")
                    for c in concept_cols:
                        print(f"    - {c['displayText']} (guid={c['guid']})")
                if snomed_cols:
                    print(f"  {tbl}: snomed columns:")
                    for c in snomed_cols:
                        print(f"    - {c['displayText']} (guid={c['guid']})")

# Also check diagnoses table which might have ICD/SNOMED codes
for tbl in ["diagnoses"]:
    body = {"keywords": tbl, "filter": {"entityType": "azure_sql_table"}, "limit": 1}
    r8 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if r8.status_code == 200:
        results = r8.json().get("value", [])
        if results:
            guid = results[0].get("id")
            r9 = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
            if r9.status_code == 200:
                ent = r9.json().get("entity", {})
                columns = ent.get("relationshipAttributes", {}).get("columns", [])
                print(f"\n  diagnoses table all columns:")
                for c in columns:
                    print(f"    - {c['displayText']} (guid={c['guid']})")

print("\nInvestigation complete!")
