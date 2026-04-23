"""
Fix Purview configuration issues:
1. Create classification DEFINITIONS in catalog (required before rules)
2. Fix Fabric scan ruleset kind (PowerBI not Fabric)
3. Update SQL scan without moving collection
4. Create glossary via Atlas API v2
"""
import json
import time

import requests
from azure.identity import AzureCliCredential

SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ACCOUNT_EP = "https://prviewacc.purview.azure.com"
API_VER = "2023-09-01"


def get_headers():
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token("https://purview.azure.net/.default").token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


h = get_headers()

# ═══════════════════════════════════════════════════════════
# FIX 1: Create classification DEFINITIONS in catalog
# ═══════════════════════════════════════════════════════════
print("FIX 1: Create classification definitions (Atlas typeDefs)")
print("=" * 60)

classification_names = [
    ("ICD10-DiagnosisCode", "ICD-10 diagnosis codes — e.g. I25.1, E11.9, C71.0"),
    ("Swedish-Personnummer", "Swedish personal identity number (personnummer)"),
    ("OMOP-ConceptId", "OMOP CDM concept identifiers — numeric standard vocabulary IDs"),
    ("FHIR-ResourceId", "FHIR R4 resource identifiers — UUID format"),
    ("SNOMED-CT-Code", "SNOMED CT clinical terminology codes"),
    ("PatientName-PHI", "Patient name fields — Protected Health Information"),
]

# Use Atlas typeDefs API to create classification types
typedef_url = f"{ACCOUNT_EP}/catalog/api/atlas/v2/types/typedefs"

# First check existing types
existing_r = requests.get(typedef_url, headers=h)
existing_names = set()
if existing_r.status_code == 200:
    for ct in existing_r.json().get("classificationDefs", []):
        existing_names.add(ct["name"])

new_defs = []
for name, desc in classification_names:
    if name in existing_names:
        print(f"  ℹ️  {name}: already exists")
    else:
        new_defs.append({
            "name": name,
            "description": desc,
            "category": "CLASSIFICATION",
            "typeVersion": "1.0",
            "attributeDefs": [],
            "superTypes": [],
        })

if new_defs:
    body = {"classificationDefs": new_defs}
    r = requests.post(typedef_url, headers=h, json=body)
    if r.status_code in (200, 201):
        created = r.json().get("classificationDefs", [])
        for cd in created:
            print(f"  ✅ Created: {cd['name']}")
    else:
        print(f"  ⚠️ Error: {r.status_code} — {r.text[:300]}")
else:
    print("  All definitions already exist")

# ═══════════════════════════════════════════════════════════
# FIX 1b: Now create the classification RULES (scan API)
# ═══════════════════════════════════════════════════════════
print(f"\nFIX 1b: Create classification rules (scan API)")
print("=" * 60)

rules = [
    {
        "name": "ICD10-DiagnosisCode",
        "kind": "Custom",
        "properties": {
            "description": "ICD-10 diagnosis codes (e.g., I25.1, E11.9, C71.0)",
            "classificationName": "ICD10-DiagnosisCode",
            "ruleStatus": "Enabled",
            "classificationAction": "Keep",
            "columnPatterns": [
                {"kind": "Regex", "pattern": "(?i)(icd|diag|diagnosis|condition).*code"},
            ],
            "dataPatterns": [
                {"kind": "Regex", "pattern": "[A-Z]\\d{2}(\\.\\d{1,4})?"},
            ],
        },
    },
    {
        "name": "Swedish-Personnummer",
        "kind": "Custom",
        "properties": {
            "description": "Swedish personal identity number (personnummer)",
            "classificationName": "Swedish-Personnummer",
            "ruleStatus": "Enabled",
            "classificationAction": "Keep",
            "columnPatterns": [
                {"kind": "Regex", "pattern": "(?i)(person|ssn|pnr|personnummer|national.?id)"},
            ],
            "dataPatterns": [
                {"kind": "Regex", "pattern": "(19|20)\\d{6}[-]?\\d{4}"},
            ],
        },
    },
    {
        "name": "OMOP-ConceptId",
        "kind": "Custom",
        "properties": {
            "description": "OMOP CDM concept identifiers",
            "classificationName": "OMOP-ConceptId",
            "ruleStatus": "Enabled",
            "classificationAction": "Keep",
            "columnPatterns": [
                {"kind": "Regex", "pattern": "(?i).*concept_id$"},
            ],
            "dataPatterns": [
                {"kind": "Regex", "pattern": "\\d{5,10}"},
            ],
        },
    },
    {
        "name": "FHIR-ResourceId",
        "kind": "Custom",
        "properties": {
            "description": "FHIR R4 resource identifiers — UUID format",
            "classificationName": "FHIR-ResourceId",
            "ruleStatus": "Enabled",
            "classificationAction": "Keep",
            "columnPatterns": [
                {"kind": "Regex", "pattern": "(?i)(resource.?id|fhir.?id|patient.?id|encounter.?id)"},
            ],
            "dataPatterns": [
                {"kind": "Regex", "pattern": "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"},
            ],
        },
    },
    {
        "name": "SNOMED-CT-Code",
        "kind": "Custom",
        "properties": {
            "description": "SNOMED CT clinical terminology codes",
            "classificationName": "SNOMED-CT-Code",
            "ruleStatus": "Enabled",
            "classificationAction": "Keep",
            "columnPatterns": [
                {"kind": "Regex", "pattern": "(?i)(snomed|sct|clinical.?code|finding.?code)"},
            ],
            "dataPatterns": [
                {"kind": "Regex", "pattern": "\\d{6,18}"},
            ],
        },
    },
    {
        "name": "PatientName-PHI",
        "kind": "Custom",
        "properties": {
            "description": "Patient name fields — Protected Health Information",
            "classificationName": "PatientName-PHI",
            "ruleStatus": "Enabled",
            "classificationAction": "Keep",
            "columnPatterns": [
                {"kind": "Regex", "pattern": "(?i)(patient.?name|first.?name|last.?name|family.?name|given.?name)"},
            ],
            "dataPatterns": [],
        },
    },
]

for rule in rules:
    url = f"{SCAN_EP}/scan/classificationrules/{rule['name']}?api-version={API_VER}"
    r = requests.put(url, headers=h, json=rule)
    status = "✅" if r.status_code in (200, 201) else "⚠️"
    print(f"  {status} Rule: {rule['name']}: {r.status_code}")
    if r.status_code not in (200, 201):
        print(f"     {r.text[:200]}")

# ═══════════════════════════════════════════════════════════
# FIX 2: Create Fabric scan ruleset with kind=PowerBI
# ═══════════════════════════════════════════════════════════
print(f"\nFIX 2: Create Fabric scan ruleset (kind=PowerBI)")
print("=" * 60)

custom_rules = [
    "ICD10-DiagnosisCode", "Swedish-Personnummer", "OMOP-ConceptId",
    "FHIR-ResourceId", "SNOMED-CT-Code", "PatientName-PHI",
]

url = f"{SCAN_EP}/scan/scanrulesets/healthcare-fabric-ruleset?api-version={API_VER}"
body = {
    "kind": "PowerBI",
    "properties": {
        "description": "Healthcare Fabric scan ruleset — PHI, ICD-10, OMOP, FHIR, SNOMED",
        "excludedSystemClassifications": [],
        "includedCustomClassificationRuleNames": custom_rules,
    },
}
r = requests.put(url, headers=h, json=body)
print(f"  {'✅' if r.status_code in (200,201) else '⚠️'} Ruleset: {r.status_code}")
if r.status_code not in (200, 201):
    print(f"     {r.text[:300]}")

# Also update existing healthcare-sql-ruleset with custom rules
url2 = f"{SCAN_EP}/scan/scanrulesets/healthcare-sql-ruleset?api-version={API_VER}"
body2 = {
    "kind": "AzureSqlDatabase",
    "properties": {
        "description": "Healthcare SQL scan ruleset — PHI, ICD-10, OMOP, SNOMED, personnummer",
        "excludedSystemClassifications": [],
        "includedCustomClassificationRuleNames": custom_rules,
    },
}
r2 = requests.put(url2, headers=h, json=body2)
print(f"  {'✅' if r2.status_code in (200,201) else '⚠️'} SQL ruleset update: {r2.status_code}")

# ═══════════════════════════════════════════════════════════
# FIX 3: Update SQL scan — keep in same collection (prviewacc)
# ═══════════════════════════════════════════════════════════
print(f"\nFIX 3: Update SQL scan with healthcare-sql-ruleset (same collection)")
print("=" * 60)

url = f"{SCAN_EP}/scan/datasources/sql-hca-demo/scans/healthcare-scan?api-version={API_VER}"
body = {
    "kind": "AzureSqlDatabaseMsi",
    "properties": {
        "databaseName": "HealthcareAnalyticsDB",
        "serverEndpoint": "sql-hca-demo.database.windows.net",
        "scanRulesetName": "healthcare-sql-ruleset",
        "collection": {"referenceName": "prviewacc", "type": "CollectionReference"},
    },
}
r = requests.put(url, headers=h, json=body)
print(f"  {'✅' if r.status_code in (200,201) else '⚠️'} SQL scan update: {r.status_code}")
if r.status_code not in (200, 201):
    print(f"     {r.text[:300]}")

# ═══════════════════════════════════════════════════════════
# FIX 4: Create glossary via Atlas API v2
# ═══════════════════════════════════════════════════════════
print(f"\nFIX 4: Create glossary and terms (Atlas API v2)")
print("=" * 60)

# List existing glossaries
atlas_base = f"{ACCOUNT_EP}/catalog/api/atlas/v2"
glos_url = f"{atlas_base}/glossary"
r = requests.get(glos_url, headers=h)
glossary_guid = None
if r.status_code == 200:
    glossaries = r.json()
    if isinstance(glossaries, list) and glossaries:
        glossary_guid = glossaries[0].get("guid")
        print(f"  Using existing glossary: {glossaries[0].get('name')} ({glossary_guid})")

if not glossary_guid:
    body = {
        "name": "Healthcare Data Catalog",
        "qualifiedName": "healthcare-data-catalog",
        "shortDescription": "Glossary for healthcare data terms — OMOP CDM, FHIR R4, clinical codes",
    }
    r2 = requests.post(glos_url, headers=h, json=body)
    if r2.status_code in (200, 201):
        glossary_guid = r2.json().get("guid")
        print(f"  ✅ Created glossary: {glossary_guid}")
    else:
        print(f"  ⚠️ Create glossary: {r2.status_code} {r2.text[:200]}")

if glossary_guid:
    terms = [
        ("OMOP CDM", "Standardized schema for observational healthcare data (v5.4)"),
        ("Person (OMOP)", "Core demographic table — one row per patient"),
        ("Condition Occurrence", "OMOP table for diagnoses and clinical conditions"),
        ("Drug Exposure", "OMOP table for medication prescriptions"),
        ("Measurement", "OMOP table for labs and vitals (LOINC mapped)"),
        ("Visit Occurrence", "OMOP table for patient encounters"),
        ("FHIR R4", "HL7 standard for healthcare data exchange — Patient, Encounter, Condition, Observation"),
        ("ICD-10", "WHO standard for diagnosis codes — e.g. I25.1, E11.9, C71.0"),
        ("DICOM", "Standard for medical imaging — MRI, pathology with StudyInstanceUID"),
        ("GMS (Genomic Medicine Sweden)", "Swedish national infrastructure for genomic diagnostics"),
        ("Protected Health Information", "Individually identifiable health data subject to GDPR/PDL"),
        ("Bronze Layer", "Raw data ingestion layer — exact copies of source data in Delta Lake"),
        ("Silver Layer", "Cleaned and enriched data layer — engineered ML features"),
        ("Gold Layer", "Business-ready analytics — ML predictions + OMOP CDM tables"),
    ]

    terms_url = f"{atlas_base}/glossary/{glossary_guid}/terms"
    for name, desc in terms:
        body = {
            "name": name,
            "qualifiedName": f"healthcare-data-catalog@{name.replace(' ', '_')}",
            "shortDescription": desc,
            "anchor": {"glossaryGuid": glossary_guid},
        }
        r = requests.post(terms_url, headers=h, json=body)
        if r.status_code in (200, 201):
            print(f"  ✅ {name}")
        elif r.status_code == 409:
            print(f"  ℹ️  {name} (already exists)")
        else:
            print(f"  ⚠️ {name}: {r.status_code} — {r.text[:150]}")

# ═══════════════════════════════════════════════════════════
# Update Fabric scans to use healthcare-fabric-ruleset
# ═══════════════════════════════════════════════════════════
print(f"\nUpdate Fabric scans with healthcare-fabric-ruleset")
print("=" * 60)

for scan_name, ws_id, coll in [
    ("Scan-HCA", "afda4639-34ce-4ee9-a82f-ab7b5cfd7334", "hca-analytics"),
    ("Scan-BrainChild", "5c9b06e2-1c7f-4671-a902-46d0372bf0fd", "brainchild-fhir"),
]:
    url = f"{SCAN_EP}/scan/datasources/Fabric/scans/{scan_name}?api-version={API_VER}"
    body = {
        "kind": "FabricMsi",
        "properties": {
            "scanRulesetName": "healthcare-fabric-ruleset",
            "collection": {"referenceName": coll, "type": "CollectionReference"},
            "scanScope": {
                "fabricItems": [
                    {"workspaceId": ws_id, "resourceTypes": ["Lakehouse", "SemanticModel"]}
                ],
            },
        },
    }
    r = requests.put(url, headers=h, json=body)
    status = "✅" if r.status_code in (200, 201) else "⚠️"
    print(f"  {status} {scan_name}: {r.status_code}")
    if r.status_code not in (200, 201):
        print(f"     {r.text[:200]}")

print("\nAll fixes applied!")
