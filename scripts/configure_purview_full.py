"""
Purview Data Catalog Configuration — Healthcare-Analytics & Brainchild-FHIR
============================================================================
Sets up collections, classification rules, scan rulesets, data sources,
scans, and glossary terms for both healthcare projects.

Usage:
    python scripts/configure_purview_full.py [--dry-run]
"""
import argparse
import json
import sys
import time

import requests
from azure.identity import AzureCliCredential

# ── Endpoints ──────────────────────────────────────────────────────
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ACCOUNT_EP = "https://prviewacc.purview.azure.com"
CATALOG_EP = f"{SCAN_EP}/catalog"

# ── Existing IDs ───────────────────────────────────────────────────
TENANT_ID = "71c4b6d5-0065-4c6c-a125-841a582754eb"
ROOT_COLLECTION = "prviewacc"
PARENT_COLLECTION = "nnx2vt"  # Hälsosjukvård

# Workspace IDs
HCA_WORKSPACE = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"   # Healthcare-Analytics
BC_WORKSPACE = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"    # BrainChild-Demo

SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DB = "HealthcareAnalyticsDB"

API_VER = "2023-09-01"


def get_headers():
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token("https://purview.azure.net/.default").token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def api_put(url, body, headers, label=""):
    resp = requests.put(url, headers=headers, json=body)
    status = "✅" if resp.status_code in (200, 201) else "⚠️"
    print(f"  {status} {label}: {resp.status_code}")
    if resp.status_code not in (200, 201):
        print(f"     {resp.text[:300]}")
    return resp


def api_post(url, body, headers, label=""):
    resp = requests.post(url, headers=headers, json=body)
    status = "✅" if resp.status_code in (200, 201, 202) else "⚠️"
    print(f"  {status} {label}: {resp.status_code}")
    if resp.status_code not in (200, 201, 202):
        print(f"     {resp.text[:300]}")
    return resp


# ══════════════════════════════════════════════════════════════════
# STEP 1: Create sub-collections under Hälsosjukvård
# ══════════════════════════════════════════════════════════════════
def create_collections(h):
    print("\n" + "=" * 60)
    print("STEP 1: Create collections")
    print("=" * 60)

    collections = {
        "hca-analytics": {
            "friendlyName": "Healthcare-Analytics",
            "description": "LOS & Readmission predictor — Bronze/Silver/Gold Lakehouse + OMOP CDM",
            "parentCollection": PARENT_COLLECTION,
        },
        "brainchild-fhir": {
            "friendlyName": "BrainChild-FHIR",
            "description": "Pediatric brain tumor registry — FHIR R4, DICOM, GMS, OMOP, BTB, SBCR",
            "parentCollection": PARENT_COLLECTION,
        },
    }

    created = {}
    for name, cfg in collections.items():
        url = f"{ACCOUNT_EP}/collections/{name}?api-version=2019-11-01-preview"
        body = {
            "friendlyName": cfg["friendlyName"],
            "description": cfg["description"],
            "parentCollection": {"referenceName": cfg["parentCollection"]},
        }
        resp = api_put(url, body, h, f"Collection: {cfg['friendlyName']}")
        if resp.status_code in (200, 201):
            data = resp.json()
            created[name] = data.get("name", name)

    return created


# ══════════════════════════════════════════════════════════════════
# STEP 2: Create custom classification rules for healthcare data
# ══════════════════════════════════════════════════════════════════
def create_classification_rules(h):
    print("\n" + "=" * 60)
    print("STEP 2: Create custom classification rules")
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
                "description": "Swedish personal identity number (personnummer) — YYYYMMDD-XXXX",
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
                "description": "OMOP CDM concept identifiers — numeric IDs mapping to standard vocabularies",
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
        api_put(url, rule, h, f"Rule: {rule['name']}")


# ══════════════════════════════════════════════════════════════════
# STEP 3: Move sql-hca-demo to Healthcare-Analytics collection
# ══════════════════════════════════════════════════════════════════
def update_datasource_collections(h, collections):
    print("\n" + "=" * 60)
    print("STEP 3: Update data source collections")
    print("=" * 60)

    hca_coll = collections.get("hca-analytics", "hca-analytics")

    # Move sql-hca-demo to Healthcare-Analytics collection
    url = f"{SCAN_EP}/scan/datasources/sql-hca-demo?api-version={API_VER}"
    body = {
        "kind": "AzureSqlDatabase",
        "properties": {
            "serverEndpoint": SQL_SERVER,
            "collection": {"referenceName": hca_coll, "type": "CollectionReference"},
        },
    }
    api_put(url, body, h, "sql-hca-demo → Healthcare-Analytics")

    # Update Fabric data source to Analysplattform (keep as-is since it's tenant-level)
    print("  ℹ️  Fabric data source stays in Analysplattform (tenant-scope)")


# ══════════════════════════════════════════════════════════════════
# STEP 4: Create scan rulesets
# ══════════════════════════════════════════════════════════════════
def create_scan_rulesets(h):
    print("\n" + "=" * 60)
    print("STEP 4: Create scan rulesets")
    print("=" * 60)

    custom_rules = [
        "ICD10-DiagnosisCode",
        "Swedish-Personnummer",
        "OMOP-ConceptId",
        "FHIR-ResourceId",
        "SNOMED-CT-Code",
        "PatientName-PHI",
    ]

    # SQL ruleset for healthcare
    url1 = f"{SCAN_EP}/scan/scanrulesets/healthcare-sql-ruleset?api-version={API_VER}"
    body1 = {
        "kind": "AzureSqlDatabase",
        "properties": {
            "description": "Healthcare SQL scan ruleset — PHI, ICD-10, OMOP, SNOMED, personnummer",
            "excludedSystemClassifications": [],
            "includedCustomClassificationRuleNames": custom_rules,
        },
    }
    api_put(url1, body1, h, "Ruleset: healthcare-sql-ruleset")

    # Fabric ruleset for healthcare
    url2 = f"{SCAN_EP}/scan/scanrulesets/healthcare-fabric-ruleset?api-version={API_VER}"
    body2 = {
        "kind": "Fabric",
        "properties": {
            "description": "Healthcare Fabric scan ruleset — PHI, ICD-10, OMOP, FHIR, SNOMED",
            "excludedSystemClassifications": [],
            "includedCustomClassificationRuleNames": custom_rules,
        },
    }
    api_put(url2, body2, h, "Ruleset: healthcare-fabric-ruleset")


# ══════════════════════════════════════════════════════════════════
# STEP 5: Update SQL scan with new ruleset
# ══════════════════════════════════════════════════════════════════
def update_sql_scan(h, collections):
    print("\n" + "=" * 60)
    print("STEP 5: Update SQL scan")
    print("=" * 60)

    hca_coll = collections.get("hca-analytics", "hca-analytics")

    url = f"{SCAN_EP}/scan/datasources/sql-hca-demo/scans/healthcare-scan?api-version={API_VER}"
    body = {
        "kind": "AzureSqlDatabaseMsi",
        "properties": {
            "databaseName": SQL_DB,
            "serverEndpoint": SQL_SERVER,
            "scanRulesetName": "healthcare-sql-ruleset",
            "collection": {"referenceName": hca_coll, "type": "CollectionReference"},
        },
    }
    api_put(url, body, h, "Scan: sql-hca-demo/healthcare-scan")


# ══════════════════════════════════════════════════════════════════
# STEP 6: Create workspace-specific Fabric scans
# ══════════════════════════════════════════════════════════════════
def create_fabric_scans(h, collections):
    print("\n" + "=" * 60)
    print("STEP 6: Create Fabric scans for both workspaces")
    print("=" * 60)

    scans = [
        {
            "name": "Scan-HCA",
            "collection": collections.get("hca-analytics", "hca-analytics"),
            "workspace_id": HCA_WORKSPACE,
            "description": "Healthcare-Analytics workspace — Bronze/Silver/Gold/OMOP lakehouses",
        },
        {
            "name": "Scan-BrainChild",
            "collection": collections.get("brainchild-fhir", "brainchild-fhir"),
            "workspace_id": BC_WORKSPACE,
            "description": "BrainChild-Demo workspace — FHIR, DICOM, genomics lakehouse",
        },
    ]

    for scan in scans:
        url = f"{SCAN_EP}/scan/datasources/Fabric/scans/{scan['name']}?api-version={API_VER}"
        body = {
            "kind": "FabricMsi",
            "properties": {
                "scanRulesetName": "healthcare-fabric-ruleset",
                "collection": {
                    "referenceName": scan["collection"],
                    "type": "CollectionReference",
                },
                "scanScope": {
                    "fabricItems": [
                        {
                            "workspaceId": scan["workspace_id"],
                            "resourceTypes": ["Lakehouse", "SemanticModel"],
                        }
                    ],
                },
            },
        }
        api_put(url, body, h, f"Fabric scan: {scan['name']}")


# ══════════════════════════════════════════════════════════════════
# STEP 7: Create glossary + terms for OMOP CDM and FHIR
# ══════════════════════════════════════════════════════════════════
def create_glossary_terms(h):
    print("\n" + "=" * 60)
    print("STEP 7: Create glossary terms")
    print("=" * 60)

    # Get or create glossary
    glossary_url = f"{ACCOUNT_EP}/catalog/api/glossary?api-version=2023-09-01"
    r = requests.get(glossary_url, headers=h)

    glossary_guid = None
    if r.status_code == 200:
        glossaries = r.json()
        if isinstance(glossaries, list) and glossaries:
            glossary_guid = glossaries[0].get("guid")
            print(f"  Using existing glossary: {glossary_guid}")

    if not glossary_guid:
        # Create a new glossary
        body = {
            "name": "Healthcare Data Catalog",
            "qualifiedName": "healthcare-data-catalog",
            "shortDescription": "Glossary for healthcare data terms — OMOP CDM, FHIR R4, clinical codes",
        }
        r2 = requests.post(glossary_url, headers=h, json=body)
        if r2.status_code in (200, 201):
            glossary_guid = r2.json().get("guid")
            print(f"  ✅ Created glossary: {glossary_guid}")
        else:
            print(f"  ⚠️ Create glossary: {r2.status_code} {r2.text[:200]}")
            return

    # Define terms
    terms = [
        {
            "name": "OMOP CDM",
            "shortDescription": "Observational Medical Outcomes Partnership Common Data Model — standardized schema for observational healthcare data",
            "longDescription": "OMOP CDM v5.4 includes Person, Visit Occurrence, Condition Occurrence, Drug Exposure, Measurement, Observation, and Observation Period tables with standardized concept IDs from the OMOP Vocabulary.",
        },
        {
            "name": "Person (OMOP)",
            "shortDescription": "Core demographic table in OMOP CDM — one row per patient",
            "longDescription": "Contains person_id, gender_concept_id, year_of_birth, race_concept_id, ethnicity_concept_id, and location_id. Maps to Patient in FHIR R4.",
        },
        {
            "name": "Condition Occurrence",
            "shortDescription": "OMOP table for diagnoses and clinical conditions",
            "longDescription": "Records condition_concept_id (ICD-10/SNOMED mapped), condition_start_date, condition_type_concept_id, and visit_occurrence_id. Maps to FHIR Condition resource.",
        },
        {
            "name": "Drug Exposure",
            "shortDescription": "OMOP table for medication prescriptions and administrations",
            "longDescription": "Records drug_concept_id (RxNorm/ATC mapped), drug_exposure_start_date, quantity, days_supply. Maps to FHIR MedicationRequest/MedicationAdministration.",
        },
        {
            "name": "Measurement",
            "shortDescription": "OMOP table for clinical measurements (labs, vitals)",
            "longDescription": "Records measurement_concept_id (LOINC mapped), value_as_number, unit_concept_id. Covers lab results, vital signs, and clinical assessments.",
        },
        {
            "name": "Visit Occurrence",
            "shortDescription": "OMOP table for patient encounters/visits",
            "longDescription": "Records visit_concept_id, visit_start_date, visit_end_date, visit_type_concept_id. Maps to FHIR Encounter resource.",
        },
        {
            "name": "FHIR R4",
            "shortDescription": "Fast Healthcare Interoperability Resources Release 4 — HL7 standard for exchanging healthcare data",
            "longDescription": "RESTful API-based standard with resources: Patient, Encounter, Condition, Observation, MedicationRequest, DiagnosticReport, ImagingStudy, Specimen.",
        },
        {
            "name": "ICD-10",
            "shortDescription": "International Classification of Diseases, 10th Revision — WHO standard for diagnosis codes",
            "longDescription": "Alphanumeric codes (e.g., I25.1 Atherosclerotic heart disease, E11.9 Type 2 diabetes, C71.0 Malignant neoplasm of cerebrum). Used in condition_source_value in OMOP.",
        },
        {
            "name": "DICOM",
            "shortDescription": "Digital Imaging and Communications in Medicine — standard for medical imaging",
            "longDescription": "Includes MRI (Modality=MR) and digital pathology (Modality=SM) studies with StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID, and patient demographics.",
        },
        {
            "name": "GMS (Genomic Medicine Sweden)",
            "shortDescription": "Swedish national infrastructure for genomic diagnostics",
            "longDescription": "FHIR-based genomic data: DiagnosticReport, Observation (variant calls), Patient demographics. Uses Swedish personnummer for identity.",
        },
        {
            "name": "Protected Health Information (PHI)",
            "shortDescription": "Individually identifiable health information subject to privacy regulations",
            "longDescription": "Includes patient names, personnummer, birth dates, addresses, and any data that could identify an individual patient. Subject to GDPR and Patientdatalagen (PDL).",
        },
        {
            "name": "Bronze Layer",
            "shortDescription": "Raw data ingestion layer in medallion lakehouse architecture",
            "longDescription": "Contains exact copies of source data (Azure SQL tables) in Delta Lake format. No transformations applied. Source of truth for data lineage.",
        },
        {
            "name": "Silver Layer",
            "shortDescription": "Cleaned and enriched data layer in medallion lakehouse architecture",
            "longDescription": "Contains ml_features table with engineered features: encounter aggregations, vital sign statistics, medication counts, diagnosis flags, LOS calculations.",
        },
        {
            "name": "Gold Layer",
            "shortDescription": "Business-ready analytics and ML output layer",
            "longDescription": "Contains ml_predictions (readmission risk, LOS predictions) and OMOP CDM tables (person, visit_occurrence, condition_occurrence, drug_exposure, measurement, observation).",
        },
    ]

    term_url = f"{ACCOUNT_EP}/catalog/api/glossary/{glossary_guid}/terms?api-version=2023-09-01"
    for term in terms:
        body = {
            "name": term["name"],
            "qualifiedName": f"healthcare-data-catalog@{term['name'].replace(' ', '_')}",
            "shortDescription": term["shortDescription"],
            "longDescription": term["longDescription"],
            "anchor": {"glossaryGuid": glossary_guid},
        }
        r = requests.post(term_url, headers=h, json=body)
        status = "✅" if r.status_code in (200, 201) else "⚠️"
        msg = ""
        if r.status_code == 409:
            msg = " (already exists)"
            status = "ℹ️"
        print(f"  {status} Term: {term['name']}{msg}")


# ══════════════════════════════════════════════════════════════════
# STEP 8: Trigger scans
# ══════════════════════════════════════════════════════════════════
def trigger_scans(h):
    print("\n" + "=" * 60)
    print("STEP 8: Trigger scans")
    print("=" * 60)

    scans_to_run = [
        ("sql-hca-demo", "healthcare-scan"),
        ("Fabric", "Scan-HCA"),
        ("Fabric", "Scan-BrainChild"),
    ]

    for ds, scan in scans_to_run:
        url = f"{SCAN_EP}/scan/datasources/{ds}/scans/{scan}/runs/run-{int(time.time())}?api-version={API_VER}"
        resp = api_post(url, {}, h, f"Trigger: {ds}/{scan}")


# ══════════════════════════════════════════════════════════════════
# STEP 9: Check scan status
# ══════════════════════════════════════════════════════════════════
def check_scan_status(h):
    print("\n" + "=" * 60)
    print("STEP 9: Check scan status (waiting 30s for initial progress...)")
    print("=" * 60)

    time.sleep(30)

    scans_to_check = [
        ("sql-hca-demo", "healthcare-scan"),
        ("Fabric", "Scan-HCA"),
        ("Fabric", "Scan-BrainChild"),
    ]

    for ds, scan in scans_to_check:
        url = f"{SCAN_EP}/scan/datasources/{ds}/scans/{scan}/runs?api-version={API_VER}"
        r = requests.get(url, headers=h)
        if r.status_code == 200:
            runs = r.json().get("value", [])
            if runs:
                latest = runs[0]
                print(f"  {ds}/{scan}: {latest.get('status')} (started: {latest.get('startTime','')[:19]})")
            else:
                print(f"  {ds}/{scan}: no runs")
        else:
            print(f"  {ds}/{scan}: error {r.status_code}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Configure Purview for Healthcare projects")
    parser.add_argument("--dry-run", action="store_true", help="Print plan without executing")
    parser.add_argument("--skip-scans", action="store_true", help="Skip triggering scans")
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — would execute:")
        print("  1. Create collections: Healthcare-Analytics, BrainChild-FHIR under Hälsosjukvård")
        print("  2. Create 6 custom classification rules (ICD-10, personnummer, OMOP, FHIR, SNOMED, PHI)")
        print("  3. Move sql-hca-demo to Healthcare-Analytics collection")
        print("  4. Create scan rulesets (SQL + Fabric) with healthcare classifications")
        print("  5. Update SQL scan with new ruleset")
        print("  6. Create targeted Fabric scans for both workspaces")
        print("  7. Create glossary with 14 healthcare terms")
        print("  8. Trigger all scans")
        return

    print("Purview Data Catalog — Healthcare Configuration")
    print("=" * 60)

    h = get_headers()

    # Step 1: Collections
    collections = create_collections(h)

    # Step 2: Classification rules
    create_classification_rules(h)

    # Step 3: Update data source collections
    update_datasource_collections(h, collections)

    # Step 4: Scan rulesets
    create_scan_rulesets(h)

    # Step 5: Update SQL scan
    update_sql_scan(h, collections)

    # Step 6: Fabric scans for both workspaces
    create_fabric_scans(h, collections)

    # Step 7: Glossary terms
    create_glossary_terms(h)

    if not args.skip_scans:
        # Step 8: Trigger scans
        trigger_scans(h)

        # Step 9: Check status
        check_scan_status(h)

    print("\n" + "=" * 60)
    print("DONE — Purview configuration complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
