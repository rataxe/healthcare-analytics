"""Fix remaining: classification names (no hyphens) + glossary terms (singular endpoint)."""
import requests
from azure.identity import AzureCliCredential

SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ACCOUNT_EP = "https://prviewacc.purview.azure.com"
API_VER = "2023-09-01"

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ═══════════════════════════════════════════════════════════
# 1. Create classification DEFINITIONS (underscores, no hyphens)
# ═══════════════════════════════════════════════════════════
print("1. Create classification definitions")
print("=" * 60)

atlas_base = f"{ACCOUNT_EP}/catalog/api/atlas/v2"
typedef_url = f"{atlas_base}/types/typedefs"

# Check existing
existing_r = requests.get(typedef_url, headers=h)
existing_names = set()
if existing_r.status_code == 200:
    for ct in existing_r.json().get("classificationDefs", []):
        existing_names.add(ct["name"])
    print(f"  Existing custom classifications: {len(existing_names)}")

# Names: letters, numbers, spaces, underscores ONLY
classifications = [
    ("ICD10 Diagnosis Code", "ICD-10 diagnosis codes — e.g. I25.1, E11.9, C71.0"),
    ("Swedish Personnummer", "Swedish personal identity number (personnummer) — YYYYMMDD-XXXX"),
    ("OMOP Concept ID", "OMOP CDM concept identifiers — numeric standard vocabulary IDs"),
    ("FHIR Resource ID", "FHIR R4 resource identifiers — UUID format"),
    ("SNOMED CT Code", "SNOMED CT clinical terminology codes"),
    ("Patient Name PHI", "Patient name fields — Protected Health Information"),
]

new_defs = []
for name, desc in classifications:
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
        for cd in r.json().get("classificationDefs", []):
            print(f"  ✅ Created: {cd['name']}")
    else:
        print(f"  ⚠️ Error: {r.status_code} — {r.text[:300]}")
else:
    print("  All definitions already exist")

# ═══════════════════════════════════════════════════════════
# 2. Create classification RULES (scan API, matching new names)
# ═══════════════════════════════════════════════════════════
print(f"\n2. Create classification rules")
print("=" * 60)

rules = [
    {
        "name": "ICD10_Diagnosis_Code",
        "kind": "Custom",
        "properties": {
            "description": "ICD-10 diagnosis codes",
            "classificationName": "ICD10 Diagnosis Code",
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
        "name": "Swedish_Personnummer",
        "kind": "Custom",
        "properties": {
            "description": "Swedish personnummer",
            "classificationName": "Swedish Personnummer",
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
        "name": "OMOP_Concept_ID",
        "kind": "Custom",
        "properties": {
            "description": "OMOP CDM concept identifiers",
            "classificationName": "OMOP Concept ID",
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
        "name": "FHIR_Resource_ID",
        "kind": "Custom",
        "properties": {
            "description": "FHIR R4 resource identifiers — UUID",
            "classificationName": "FHIR Resource ID",
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
        "name": "SNOMED_CT_Code",
        "kind": "Custom",
        "properties": {
            "description": "SNOMED CT clinical codes",
            "classificationName": "SNOMED CT Code",
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
        "name": "Patient_Name_PHI",
        "kind": "Custom",
        "properties": {
            "description": "Patient name — PHI",
            "classificationName": "Patient Name PHI",
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
    print(f"  {status} {rule['name']}: {r.status_code}")
    if r.status_code not in (200, 201):
        print(f"     {r.text[:200]}")

# ═══════════════════════════════════════════════════════════
# 3. Update scan rulesets with new rule names
# ═══════════════════════════════════════════════════════════
print(f"\n3. Update scan rulesets with new rule names")
print("=" * 60)

rule_names = [r["name"] for r in rules]

# SQL ruleset
url1 = f"{SCAN_EP}/scan/scanrulesets/healthcare-sql-ruleset?api-version={API_VER}"
body1 = {
    "kind": "AzureSqlDatabase",
    "properties": {
        "description": "Healthcare SQL — PHI, ICD-10, OMOP, FHIR, SNOMED, personnummer",
        "includedCustomClassificationRuleNames": rule_names,
    },
}
r1 = requests.put(url1, headers=h, json=body1)
print(f"  {'✅' if r1.status_code in (200,201) else '⚠️'} healthcare-sql-ruleset: {r1.status_code}")

# Fabric/PowerBI ruleset
url2 = f"{SCAN_EP}/scan/scanrulesets/healthcare-fabric-ruleset?api-version={API_VER}"
body2 = {
    "kind": "PowerBI",
    "properties": {
        "description": "Healthcare Fabric — PHI, ICD-10, OMOP, FHIR, SNOMED, personnummer",
        "includedCustomClassificationRuleNames": rule_names,
    },
}
r2 = requests.put(url2, headers=h, json=body2)
print(f"  {'✅' if r2.status_code in (200,201) else '⚠️'} healthcare-fabric-ruleset: {r2.status_code}")

# ═══════════════════════════════════════════════════════════
# 4. Create glossary terms (singular endpoint)
# ═══════════════════════════════════════════════════════════
print(f"\n4. Create glossary terms")
print("=" * 60)

# Get existing glossary
glos_url = f"{atlas_base}/glossary"
r = requests.get(glos_url, headers=h)
glossary_guid = None
if r.status_code == 200:
    glossaries = r.json()
    if isinstance(glossaries, list) and glossaries:
        # Use existing or find healthcare one
        for g in glossaries:
            glossary_guid = g.get("guid")
            print(f"  Found glossary: {g.get('name')} ({glossary_guid})")
            break

if not glossary_guid:
    body = {
        "name": "Healthcare Data Catalog",
        "qualifiedName": "healthcare-data-catalog",
        "shortDescription": "Healthcare terms — OMOP CDM, FHIR R4, clinical codes",
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
        ("Person OMOP", "Core demographic table — one row per patient"),
        ("Condition Occurrence", "OMOP table for diagnoses and clinical conditions"),
        ("Drug Exposure", "OMOP table for medication prescriptions"),
        ("Measurement", "OMOP table for labs and vitals (LOINC mapped)"),
        ("Visit Occurrence", "OMOP table for patient encounters"),
        ("FHIR R4", "HL7 standard for healthcare data exchange"),
        ("ICD 10", "WHO standard for diagnosis codes"),
        ("DICOM", "Standard for medical imaging — MRI, pathology"),
        ("Genomic Medicine Sweden", "Swedish infrastructure for genomic diagnostics"),
        ("Protected Health Information", "Individually identifiable health data (GDPR/PDL)"),
        ("Bronze Layer", "Raw data ingestion — exact copies in Delta Lake"),
        ("Silver Layer", "Cleaned and enriched data — ML features"),
        ("Gold Layer", "Business-ready analytics — ML predictions + OMOP CDM"),
    ]

    # Singular endpoint: POST /glossary/term
    term_url = f"{atlas_base}/glossary/term"
    for name, desc in terms:
        body = {
            "name": name,
            "qualifiedName": f"{name.replace(' ', '_')}@Glossary",
            "shortDescription": desc,
            "anchor": {"glossaryGuid": glossary_guid},
        }
        r = requests.post(term_url, headers=h, json=body)
        if r.status_code in (200, 201):
            print(f"  ✅ {name}")
        elif r.status_code == 409:
            print(f"  ℹ️  {name} (already exists)")
        else:
            print(f"  ⚠️ {name}: {r.status_code} — {r.text[:150]}")

# ═══════════════════════════════════════════════════════════
# 5. Trigger scans
# ═══════════════════════════════════════════════════════════
print(f"\n5. Trigger scans")
print("=" * 60)

import time

for ds, scan in [("sql-hca-demo", "healthcare-scan"), ("Fabric", "Scan-HCA"), ("Fabric", "Scan-BrainChild")]:
    run_id = f"run-{int(time.time())}"
    url = f"{SCAN_EP}/scan/datasources/{ds}/scans/{scan}/runs/{run_id}?api-version={API_VER}"
    r = requests.put(url, headers=h, json={})
    status = "✅" if r.status_code in (200, 201, 202) else "⚠️"
    print(f"  {status} {ds}/{scan}: {r.status_code}")
    if r.status_code not in (200, 201, 202):
        print(f"     {r.text[:200]}")
    time.sleep(2)

print(f"\nDone! Scans triggered. Check status in 1-2 minutes.")
