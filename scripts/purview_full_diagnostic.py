"""
Purview Full Diagnostic & Fix
===============================
Goes through EVERY component of Purview and reports what exists / is missing.
Then fixes what can be fixed.

Components checked:
  1. Collections (hierarchy)
  2. Data Sources (SQL, Fabric)
  3. Scans + Scan Runs (status, last run time)
  4. Entity counts by type
  5. SQL entities (tables, views, columns)
  6. Fabric entities (lakehouses, tables, notebooks, pipelines)
  7. Glossary + terms + categories
  8. Governance Domains
  9. Data Products
  10. Labels / Tags on entities
  11. PII Classifications
  12. Term → Entity mappings

Then fixes:
  A. Missing collections
  B. Missing data sources
  C. Broken/missing scans
  D. Missing glossary terms
  E. Fabric table descriptions (correct API)
  F. Missing term→entity mappings
  G. Missing governance domains
  H. Missing data products
  I. Trigger scan runs

Usage:
  python scripts/purview_full_diagnostic.py              # Diagnose + fix
  python scripts/purview_full_diagnostic.py --diag-only  # Just diagnose
"""
import argparse
import json
import os
import sys
import time

# Fix Windows console encoding
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import requests
from azure.identity import AzureCliCredential
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── CONFIG ──────────────────────────────────────────────────────
cred = AzureCliCredential(process_timeout=30)

ACCT = "https://prviewacc.purview.azure.com"
TENANT_ID = "71c4b6d5-0065-4c6c-a125-841a582754eb"
TENANT_EP = f"https://{TENANT_ID}-api.purview-service.microsoft.com"
SCAN_EP = f"{ACCT}/scan"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
SCAN_API = "2022-07-01-preview"
COLL_API = "2019-11-01-preview"
DG_API = "2025-09-15-preview"
DG_BASE = f"{TENANT_EP}/datagovernance/catalog"

SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DB = "HealthcareAnalyticsDB"
SQL_RG = "rg-healthcare-analytics"
SQL_SUB = "5b44c9f3-bbe7-464c-aa3e-562726a12004"

HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"

sess = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
sess.mount("https://", HTTPAdapter(max_retries=retry))


# ── Formatting ──────────────────────────────────────────────────
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

stats = {"ok": 0, "missing": 0, "fixed": 0, "errors": 0}


def header(num, title):
    print(f"\n{BOLD}{BLUE}{'=' * 70}{RESET}")
    print(f"{BOLD}{BLUE}  {num}. {title}{RESET}")
    print(f"{BOLD}{BLUE}{'=' * 70}{RESET}")


def ok(msg):
    print(f"  {GREEN}OK{RESET}  {msg}")
    stats["ok"] += 1


def miss(msg):
    print(f"  {RED}MISS{RESET} {msg}")
    stats["missing"] += 1


def fixed(msg):
    print(f"  {CYAN}FIX{RESET}  {msg}")
    stats["fixed"] += 1


def warn(msg):
    print(f"  {YELLOW}WARN{RESET} {msg}")


def info(msg):
    print(f"  {DIM}INFO{RESET} {msg}")


def err(msg):
    print(f"  {RED}ERR{RESET}  {msg}")
    stats["errors"] += 1


def get_headers():
    token = cred.get_token("https://purview.azure.net/.default").token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ══════════════════════════════════════════════════════════════════════
# Expected configuration
# ══════════════════════════════════════════════════════════════════════

EXPECTED_COLLECTIONS = [
    ("halsosjukvard", "Hälsosjukvård", "prviewacc"),
    ("sql-databases", "SQL Databases", "halsosjukvard"),
    ("fabric-analytics", "Fabric Analytics", "halsosjukvard"),
    ("barncancer", "Barncancerforskning", "prviewacc"),
    ("fabric-brainchild", "Fabric BrainChild", "barncancer"),
]

EXPECTED_DATASOURCES = {
    "sql-hca-demo": {
        "kind": "AzureSqlDatabase",
        "collection": "sql-databases",
        "properties": {
            "serverEndpoint": SQL_SERVER,
            "resourceGroup": SQL_RG,
            "subscriptionId": SQL_SUB,
            "location": "swedencentral",
            "resourceName": "sql-hca-demo",
        },
    },
    "Fabric": {
        "kind": "PowerBI",
        "collection": "fabric-analytics",
        "properties": {
            "tenant": TENANT_ID,
        },
    },
}

EXPECTED_SCANS = {
    "sql-hca-demo": {
        "healthcare-scan": {
            "kinds": ["AzureSqlDatabaseMsiScan", "AzureSqlDatabaseCredentialScan"],
            "properties": {
                "databaseName": SQL_DB,
                "serverEndpoint": SQL_SERVER,
                "scanRulesetName": "AzureSqlDatabase",
                "scanRulesetType": "System",
                "collection": {"referenceName": "sql-databases", "type": "CollectionReference"},
            },
        }
    },
    "Fabric": {
        "Scan-HCA": {
            "kinds": ["PowerBIMsiScan", "PowerBIDelegatedScan"],
            "properties": {
                "includePersonalWorkspaces": False,
                "collection": {"referenceName": "fabric-analytics", "type": "CollectionReference"},
            },
        },
        "Scan-BrainChild": {
            "kinds": ["PowerBIMsiScan", "PowerBIDelegatedScan"],
            "properties": {
                "includePersonalWorkspaces": False,
                "collection": {"referenceName": "fabric-brainchild", "type": "CollectionReference"},
            },
        },
    },
}

GLOSSARY_CATEGORIES = [
    "Kliniska Standarder", "Interoperabilitet", "Dataarkitektur",
    "Klinisk Data", "Barncancerforskning",
]

REQUIRED_TERMS = {
    "ICD-10": ("Kliniska Standarder", "International Classification of Diseases, 10th Revision. WHO:s diagnoskodssystem."),
    "ATC-klassificering": ("Kliniska Standarder", "Anatomisk Terapeutisk Kemisk klassificering (WHO)."),
    "Skyddad hälsoinformation (PHI)": ("Kliniska Standarder", "Protected Health Information — personuppgifter som kräver GDPR/patientdatalagen-skydd."),
    "Svenskt personnummer": ("Kliniska Standarder", "Unikt 12-siffrigt identifikationsnummer (YYYYMMDD-XXXX)."),
    "SNOMED-CT": ("Kliniska Standarder", "Systematized Nomenclature of Medicine — Clinical Terms."),
    "ICD-O-3": ("Kliniska Standarder", "International Classification of Diseases for Oncology, 3rd Ed."),
    "LOINC": ("Kliniska Standarder", "Logical Observation Identifiers Names and Codes."),
    "ACMG-klassificering": ("Kliniska Standarder", "American College of Medical Genetics variant-klassificering."),
    "FHIR R4": ("Interoperabilitet", "Fast Healthcare Interoperability Resources Release 4. HL7-standarden."),
    "FHIR Patient": ("Interoperabilitet", "FHIR-resurs för patientdemografi."),
    "FHIR Encounter": ("Interoperabilitet", "FHIR-resurs för vårdkontakter."),
    "FHIR Condition": ("Interoperabilitet", "FHIR-resurs för diagnoser."),
    "FHIR MedicationRequest": ("Interoperabilitet", "FHIR-resurs för läkemedelsförskrivningar."),
    "FHIR Observation": ("Interoperabilitet", "FHIR-resurs för mätvärden och labresultat."),
    "FHIR ImagingStudy": ("Interoperabilitet", "FHIR-resurs som refererar till DICOM-bildstudier."),
    "FHIR Specimen": ("Interoperabilitet", "FHIR-resurs för biologiska prover."),
    "FHIR DiagnosticReport": ("Interoperabilitet", "FHIR-resurs för diagnostiska rapporter."),
    "OMOP CDM": ("Interoperabilitet", "Observational Medical Outcomes Partnership Common Data Model. OHDSI."),
    "OMOP Person": ("Interoperabilitet", "OMOP-tabell för patientdemografi."),
    "OMOP Visit Occurrence": ("Interoperabilitet", "OMOP-tabell för vårdbesök."),
    "OMOP Condition Occurrence": ("Interoperabilitet", "OMOP-tabell för diagnoser."),
    "OMOP Drug Exposure": ("Interoperabilitet", "OMOP-tabell för läkemedelsexponering."),
    "OMOP Measurement": ("Interoperabilitet", "OMOP-tabell för labvärden och mätningar."),
    "OMOP Specimen": ("Interoperabilitet", "OMOP-tabell för biologiska prover."),
    "OMOP Genomics": ("Interoperabilitet", "OMOP Genomics-tillägg: gene_sequence och variant_occurrence."),
    "Medallion-arkitektur": ("Dataarkitektur", "Bronze → Silver → Gold datalagermönster."),
    "Bronze-lager": ("Dataarkitektur", "Rådatalager med minimal transformation."),
    "Silver-lager": ("Dataarkitektur", "Renat och normaliserat datalager."),
    "Gold-lager": ("Dataarkitektur", "Aggregerat analyslager optimerat för konsumtion."),
    "Feature Engineering": ("Dataarkitektur", "Processen att skapa ML-features från rådata."),
    "ML-prediktion": ("Dataarkitektur", "Machine Learning-prediktioner i Gold-lagret."),
    "Vårdtid (LOS)": ("Klinisk Data", "Length of Stay — antal vårddagar."),
    "Återinläggningsrisk": ("Klinisk Data", "Sannolikhet för återinläggning inom 30 dagar."),
    "Vitalparametrar": ("Klinisk Data", "Blodtryck, puls, temperatur, saturation."),
    "Labresultat": ("Klinisk Data", "Hemoglobin, glukos, kreatinin, natrium, kalium."),
    "Charlson Comorbidity Index": ("Klinisk Data", "Komorbiditetsindex baserat på ICD-10-diagnoser."),
    "DRG-klassificering": ("Klinisk Data", "Diagnosrelaterade Grupper — patientklassificering."),
    "DICOM": ("Barncancerforskning", "Digital Imaging and Communications in Medicine."),
    "Genomic Medicine Sweden (GMS)": ("Barncancerforskning", "Nationellt program för genomisk medicin."),
    "VCF (Variant Call Format)": ("Barncancerforskning", "Filformat för genomiska varianter."),
    "BTB (Barntumörbanken)": ("Barncancerforskning", "Biobank för barncancerprover."),
    "SBCR (Svenska Barncancerregistret)": ("Barncancerforskning", "Nationellt kvalitetsregister för barncancer."),
    "Genomisk variant": ("Barncancerforskning", "Förändring i DNA-sekvensen."),
    "HGVS-nomenklatur": ("Barncancerforskning", "Human Genome Variation Society-nomenklatur."),
    "Tumörsite": ("Barncancerforskning", "Anatomisk lokalisation av tumör."),
    "Behandlingsprotokoll": ("Barncancerforskning", "Standardiserat behandlingsschema."),
    "Seneffekter": ("Barncancerforskning", "Långtidsbiverkningar efter cancerbehandling."),
    "FFPE (Formalinfixerat paraffin)": ("Barncancerforskning", "Fixeringsmetod för vävnadsprover."),
}

EXPECTED_DOMAINS = [
    {
        "name": "Klinisk Vård",
        "description": "Domän för klinisk patientdata — vårdbesök, diagnoser, medicinering, labresultat och ML-prediktioner.",
    },
    {
        "name": "Barncancerforskning",
        "description": "Domän för barncancerforskningsdata — FHIR-resurser, DICOM-bilder, genomik, biobanksdata.",
    },
]

EXPECTED_DATA_PRODUCTS = {
    "Klinisk Vård": [
        "Patientdemografi", "Vårdbesök & utfall", "Diagnoser (ICD-10)",
        "Medicinering (ATC)", "Vitalparametrar & labb", "ML-prediktion (LOS & readmission)",
    ],
    "Barncancerforskning": [
        "FHIR Patientresurser", "Medicinsk bilddiagnostik (DICOM)",
        "Genomik (GMS/VCF)", "Biobanksdata (BTB)", "Kvalitetsregister (SBCR)",
    ],
}

# Fabric table descriptions — correct API uses PUT guid?name=userDescription with data=string
FABRIC_TABLE_DESCS = {
    "hca_patients": "Bronze | Patientdemografi — ra ingestion fran hca.patients. FHIR: Patient, OMOP: person",
    "hca_encounters": "Bronze | Vardkontakter — ra ingestion fran hca.encounters. FHIR: Encounter, OMOP: visit_occurrence",
    "hca_diagnoses": "Bronze | Diagnoser — ra ingestion fran hca.diagnoses. FHIR: Condition, OMOP: condition_occurrence",
    "hca_vitals_labs": "Bronze | Vitalparametrar & labb — ra ingestion fran hca.vitals_labs. FHIR: Observation, OMOP: measurement",
    "hca_medications": "Bronze | Medicinering — ra ingestion fran hca.medications. FHIR: MedicationRequest, OMOP: drug_exposure",
    "ml_features": "Silver | ML-features — CCI-score, prior_admissions, medication_count, vitals. Feature engineering fran Bronze",
    "ml_predictions": "Gold | ML-prediktioner — LOS och readmission risk scores. Tranad pa Silver-features",
    "person": "Gold OMOP | OMOP CDM Person-tabell. Mappas fran hca_patients",
    "visit_occurrence": "Gold OMOP | OMOP CDM Visit Occurrence. Mappas fran hca_encounters",
    "condition_occurrence": "Gold OMOP | OMOP CDM Condition Occurrence. Mappas fran hca_diagnoses",
    "drug_exposure": "Gold OMOP | OMOP CDM Drug Exposure. Mappas fran hca_medications",
    "measurement": "Gold OMOP | OMOP CDM Measurement. Mappas fran hca_vitals_labs",
    "observation": "Gold OMOP | OMOP CDM Observation. Kompletterande observationsdata",
    "observation_period": "Gold OMOP | OMOP CDM Observation Period. Tidsintervall per patient",
    "location": "Gold OMOP | OMOP CDM Location. Geografisk plats",
    "concept": "Gold OMOP | OMOP CDM Concept-referenstabell. Standardvokabular",
    "specimen": "BrainChild | OMOP Specimen — Biobanksprover fran BTB. SNOMED-kodade provtyper",
    "brainchild_bronze_dicom_study": "BrainChild Bronze | DICOM Study-metadata (MRI + patologi)",
    "brainchild_bronze_dicom_series": "BrainChild Bronze | DICOM Series-metadata (MRI-sekvenser)",
    "brainchild_bronze_dicom_instance": "BrainChild Bronze | DICOM Instance-metadata (individuella bilder)",
    "brainchild_silver_dicom_studies": "BrainChild Silver | DICOM Study — renad studiedata med patientkoppling",
    "brainchild_silver_dicom_series": "BrainChild Silver | DICOM Series — renade seriedata med protokoll",
    "brainchild_silver_dicom_pathology": "BrainChild Silver | DICOM Patologi — histopatologiska prover med fargning",
}

# Lakehouse descriptions
LAKEHOUSE_DESCS = {
    "bronze_lakehouse": "Bronze Layer | Radatalager — direkt ingestion fran Azure SQL.",
    "silver_lakehouse": "Silver Layer | Renat & normaliserat datalager med feature engineering.",
    "gold_lakehouse": "Gold Layer | ML-ready features for LOS-prediktion och aterinlaggningsrisk.",
    "gold_omop": "Gold OMOP | OMOP CDM-standardiserad data.",
    "lh_brainchild": "BrainChild Lakehouse | Barncancerforskningsdata — FHIR, DICOM, Genomik, BTB, SBCR.",
}

# Term → Fabric table mappings
TERM_FABRIC_MAP = {
    "FHIR Patient": ["hca_patients", "person"],
    "FHIR Encounter": ["hca_encounters", "visit_occurrence"],
    "FHIR Condition": ["hca_diagnoses", "condition_occurrence"],
    "FHIR MedicationRequest": ["hca_medications", "drug_exposure"],
    "FHIR Observation": ["hca_vitals_labs", "measurement", "observation"],
    "FHIR Specimen": ["specimen"],
    "FHIR ImagingStudy": ["brainchild_bronze_dicom_study", "brainchild_silver_dicom_studies"],
    "OMOP CDM": ["person", "visit_occurrence", "condition_occurrence", "drug_exposure",
                  "measurement", "observation", "observation_period", "location", "concept"],
    "OMOP Person": ["person"],
    "OMOP Visit Occurrence": ["visit_occurrence"],
    "OMOP Condition Occurrence": ["condition_occurrence"],
    "OMOP Drug Exposure": ["drug_exposure"],
    "OMOP Measurement": ["measurement"],
    "OMOP Specimen": ["specimen"],
    "Bronze-lager": ["hca_patients", "hca_encounters", "hca_diagnoses", "hca_vitals_labs", "hca_medications",
                     "brainchild_bronze_dicom_study", "brainchild_bronze_dicom_series", "brainchild_bronze_dicom_instance"],
    "Silver-lager": ["ml_features", "brainchild_silver_dicom_studies",
                     "brainchild_silver_dicom_series", "brainchild_silver_dicom_pathology"],
    "Gold-lager": ["ml_predictions"],
    "Medallion-arkitektur": ["ml_features", "ml_predictions"],
    "Feature Engineering": ["ml_features"],
    "ML-prediktion": ["ml_predictions"],
    "DICOM": ["brainchild_bronze_dicom_study", "brainchild_bronze_dicom_series",
              "brainchild_bronze_dicom_instance", "brainchild_silver_dicom_studies",
              "brainchild_silver_dicom_series", "brainchild_silver_dicom_pathology"],
    "BTB (Barntumörbanken)": ["specimen"],
    "ICD-10": ["hca_diagnoses", "condition_occurrence"],
    "ATC-klassificering": ["hca_medications", "drug_exposure"],
    "SNOMED-CT": ["specimen"],
    "LOINC": ["measurement", "observation"],
    "Vitalparametrar": ["hca_vitals_labs", "measurement"],
    "Labresultat": ["hca_vitals_labs", "measurement"],
    "Vårdtid (LOS)": ["hca_encounters", "visit_occurrence", "ml_features"],
    "Återinläggningsrisk": ["hca_encounters", "ml_features", "ml_predictions"],
    "Charlson Comorbidity Index": ["ml_features"],
}

# SQL table labels
SQL_TABLE_LABELS = {
    "patients": ["PHI", "FHIR-Patient", "OMOP-Person", "Medallion-Source"],
    "encounters": ["PHI", "FHIR-Encounter", "OMOP-Visit", "Medallion-Source", "ML-Target"],
    "diagnoses": ["ICD-10", "FHIR-Condition", "OMOP-Condition", "Medallion-Source"],
    "medications": ["ATC", "FHIR-MedicationRequest", "OMOP-DrugExposure", "Medallion-Source"],
    "vitals_labs": ["FHIR-Observation", "OMOP-Measurement", "Medallion-Source", "ML-Feature"],
    "vw_ml_encounters": ["Gold-Layer", "ML-Ready", "Medallion-Gold"],
}


# ══════════════════════════════════════════════════════════════════════
# DIAGNOSTIC FUNCTIONS
# ══════════════════════════════════════════════════════════════════════

def diag_collections(h):
    header("1", "COLLECTIONS")
    r = sess.get(f"{ACCT}/account/collections?api-version={COLL_API}", headers=h, timeout=30)
    if r.status_code != 200:
        err(f"Cannot read collections: {r.status_code}")
        return {}

    existing = {c["name"]: c for c in r.json().get("value", [])}
    info(f"Total collections: {len(existing)}")

    for name, friendly, parent in EXPECTED_COLLECTIONS:
        if name in existing:
            c = existing[name]
            actual_parent = c.get("parentCollection", {}).get("referenceName", "?")
            ok(f"{c.get('friendlyName', name)} ({name}) -> {actual_parent}")
        else:
            miss(f"{friendly} ({name}) -> {parent}")

    # Show any extra collections
    expected_names = {c[0] for c in EXPECTED_COLLECTIONS} | {"prviewacc"}
    extras = set(existing.keys()) - expected_names
    if extras:
        for e in extras:
            info(f"Extra collection: {existing[e].get('friendlyName', e)} ({e})")

    return existing


def diag_datasources(h):
    header("2", "DATA SOURCES")
    r = sess.get(f"{SCAN_EP}/datasources?api-version={SCAN_API}", headers=h, timeout=30)
    if r.status_code != 200:
        err(f"Cannot read datasources: {r.status_code}")
        return {}

    existing = {ds["name"]: ds for ds in r.json().get("value", [])}
    info(f"Total datasources: {len(existing)}")

    for name, expected in EXPECTED_DATASOURCES.items():
        if name in existing:
            ds = existing[name]
            kind = ds.get("kind", "?")
            coll = ds.get("properties", {}).get("collection", {}).get("referenceName", "?")
            ok(f"{name} (kind={kind}, collection={coll})")
        else:
            miss(f"{name} (kind={expected['kind']})")

    # Show any extra datasources
    extras = set(existing.keys()) - set(EXPECTED_DATASOURCES.keys())
    for e in extras:
        ds = existing[e]
        info(f"Extra datasource: {e} (kind={ds.get('kind', '?')})")

    return existing


def diag_scans(h):
    header("3", "SCANS & SCAN RUNS")
    found_scans = {}

    for ds_name, scans in EXPECTED_SCANS.items():
        # List existing scans for this datasource
        r = sess.get(f"{SCAN_EP}/datasources/{ds_name}/scans?api-version={SCAN_API}",
                      headers=h, timeout=30)
        existing_scans = {}
        if r.status_code == 200:
            for s in r.json().get("value", []):
                existing_scans[s["name"]] = s

        for scan_name, scan_config in scans.items():
            if scan_name in existing_scans:
                s = existing_scans[scan_name]
                ok(f"{ds_name}/{scan_name} (kind={s.get('kind', '?')})")
                found_scans[(ds_name, scan_name)] = s

                # Check last run
                r2 = sess.get(
                    f"{SCAN_EP}/datasources/{ds_name}/scans/{scan_name}/runs?api-version={SCAN_API}",
                    headers=h, timeout=30
                )
                if r2.status_code == 200:
                    runs = r2.json().get("value", [])
                    if runs:
                        last = runs[0]
                        status = last.get("status", "?")
                        start = last.get("startTime", "?")
                        end = last.get("endTime", "?")
                        info(f"  Last run: {status} | {start} -> {end}")
                    else:
                        warn(f"  No runs yet — scan has never been triggered")
                time.sleep(0.2)
            else:
                # Check if there's a scan with different name
                matching = [s for s in existing_scans.values()
                           if ds_name.lower() in s.get("name", "").lower() or
                           scan_name.lower().replace("scan-", "") in s.get("name", "").lower()]
                if existing_scans:
                    warn(f"{ds_name}/{scan_name} NOT found, but found: {list(existing_scans.keys())}")
                    # Use existing scan instead
                    for sn, sv in existing_scans.items():
                        found_scans[(ds_name, sn)] = sv
                        info(f"  Using existing: {sn} (kind={sv.get('kind', '?')})")
                        # Check runs
                        r2 = sess.get(
                            f"{SCAN_EP}/datasources/{ds_name}/scans/{sn}/runs?api-version={SCAN_API}",
                            headers=h, timeout=30
                        )
                        if r2.status_code == 200:
                            runs = r2.json().get("value", [])
                            if runs:
                                last = runs[0]
                                info(f"  Last run: {last.get('status','?')} | {last.get('startTime','?')}")
                            else:
                                warn(f"  No runs yet")
                        time.sleep(0.2)
                else:
                    miss(f"{ds_name}/{scan_name}")

    return found_scans


def diag_entities(h):
    header("4", "ENTITY COUNTS BY TYPE")

    entity_types = [
        "azure_sql_table", "azure_sql_view", "azure_sql_column",
        "azure_sql_database", "azure_sql_server",
        "fabric_lake_warehouse", "fabric_lakehouse_table",
        "fabric_lakehouse_table_column",
        "fabric_synapse_notebook", "fabric_pipeline",
        "powerbi_dataset", "powerbi_table", "powerbi_column",
    ]

    counts = {}
    for etype in entity_types:
        body = {"keywords": "*", "filter": {"entityType": etype}, "limit": 1}
        r = sess.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            count = r.json().get("@search.count", 0)
            counts[etype] = count
            if count > 0:
                ok(f"{etype}: {count}")
            else:
                warn(f"{etype}: 0")
        else:
            err(f"{etype}: search failed {r.status_code}")
        time.sleep(0.15)

    # Total assets
    body = {"keywords": "*", "limit": 1}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        total = r.json().get("@search.count", 0)
        info(f"TOTAL ASSETS: {total}")

    return counts


def diag_sql_entities(h):
    header("5", "SQL ENTITIES IN PURVIEW")

    sql_tables = ["patients", "encounters", "diagnoses", "medications", "vitals_labs"]
    sql_views = ["vw_ml_encounters"]
    sql_guids = {}

    for table in sql_tables + sql_views:
        body = {
            "keywords": table,
            "filter": {"or": [{"entityType": "azure_sql_table"}, {"entityType": "azure_sql_view"}]},
            "limit": 10,
        }
        r = sess.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            for asset in r.json().get("value", []):
                qn = asset.get("qualifiedName", "")
                if table.lower() in qn.lower() and "hca" in qn.lower():
                    sql_guids[table] = asset["id"]
                    etype = asset.get("entityType", "?")
                    ok(f"hca.{table} (type={etype}, guid={asset['id'][:12]}...)")

                    # Check if it has labels
                    r2 = sess.get(f"{ATLAS}/entity/guid/{asset['id']}?minExtInfo=true",
                                  headers=h, timeout=30)
                    if r2.status_code == 200:
                        ent = r2.json().get("entity", {})
                        labels = ent.get("labels", [])
                        desc = ent.get("attributes", {}).get("userDescription", "")
                        if labels:
                            info(f"  Labels: {labels}")
                        if desc:
                            info(f"  Description: {desc[:60]}...")
                    break
            else:
                miss(f"hca.{table}")
        time.sleep(0.2)

    # Check columns
    body = {"keywords": "*", "filter": {"entityType": "azure_sql_column"}, "limit": 1}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    col_count = r.json().get("@search.count", 0) if r.status_code == 200 else 0
    if col_count > 0:
        ok(f"SQL columns: {col_count} indexed")
    else:
        miss(f"SQL columns: 0 — column-level scan not working")

    return sql_guids


def diag_fabric_entities(h):
    header("6", "FABRIC ENTITIES IN PURVIEW")

    # First check lakehouses
    lh_guids = {}
    for lh_name in LAKEHOUSE_DESCS:
        body = {
            "keywords": lh_name,
            "filter": {"or": [
                {"entityType": "fabric_lake_warehouse"},
                {"entityType": "powerbi_dataset"},
            ]},
            "limit": 10,
        }
        r = sess.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            for asset in r.json().get("value", []):
                name = asset.get("name", "")
                if lh_name.lower() in name.lower():
                    lh_guids[lh_name] = asset["id"]
                    ok(f"Lakehouse: {lh_name} (guid={asset['id'][:12]}...)")

                    # Check description
                    r2 = sess.get(f"{ATLAS}/entity/guid/{asset['id']}?minExtInfo=true",
                                  headers=h, timeout=30)
                    if r2.status_code == 200:
                        desc = r2.json().get("entity", {}).get("attributes", {}).get("userDescription", "")
                        if desc:
                            info(f"  Has description")
                        else:
                            warn(f"  No description")
                    break
            else:
                miss(f"Lakehouse: {lh_name}")
        time.sleep(0.15)

    # Check tables
    fabric_guids = {}
    all_expected_tables = list(FABRIC_TABLE_DESCS.keys())

    for table_name in all_expected_tables:
        body = {
            "keywords": table_name,
            "filter": {"or": [
                {"entityType": "fabric_lakehouse_table"},
                {"entityType": "powerbi_table"},
            ]},
            "limit": 10,
        }
        r = sess.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            found_match = False
            for asset in r.json().get("value", []):
                name = asset.get("name", "")
                qn = asset.get("qualifiedName", "")
                if table_name.lower() in name.lower() or table_name.lower() in qn.lower():
                    fabric_guids[table_name] = asset["id"]
                    # Check description
                    r2 = sess.get(f"{ATLAS}/entity/guid/{asset['id']}?minExtInfo=true",
                                  headers=h, timeout=30)
                    has_desc = False
                    if r2.status_code == 200:
                        desc = r2.json().get("entity", {}).get("attributes", {}).get("userDescription", "")
                        has_desc = bool(desc)
                    desc_status = "has desc" if has_desc else "NO desc"
                    ok(f"Table: {table_name} ({desc_status})")
                    found_match = True
                    break
            if not found_match:
                miss(f"Table: {table_name}")
        time.sleep(0.12)

    return lh_guids, fabric_guids


def diag_glossary(h):
    header("7", "GLOSSARY, TERMS & CATEGORIES")

    # Find glossary
    r = sess.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    glossary_guid = None
    if r.status_code == 200:
        glossaries = r.json()
        info(f"Glossaries found: {len(glossaries)}")
        for g in glossaries:
            name = g.get("name", "")
            guid = g["guid"]
            term_count = len(g.get("terms", []))
            cat_count = len(g.get("categories", []))
            info(f"  {name}: {term_count} terms, {cat_count} categories (guid={guid[:12]}...)")
            if "sjukvard" in name.lower() or "kund" in name.lower() or term_count > 0:
                glossary_guid = guid
    else:
        err(f"Cannot read glossary: {r.status_code}")

    if not glossary_guid:
        miss("No glossary found!")
        return None, {}, {}

    # Get categories
    r = sess.get(f"{ATLAS}/glossary/{glossary_guid}/categories?limit=50", headers=h, timeout=30)
    cat_guids = {}
    if r.status_code == 200:
        for cat in r.json():
            cat_guids[cat["name"]] = cat["guid"]

    for expected_cat in GLOSSARY_CATEGORIES:
        if expected_cat in cat_guids:
            ok(f"Category: {expected_cat}")
        else:
            miss(f"Category: {expected_cat}")

    # Get terms
    r = sess.get(f"{ATLAS}/glossary/{glossary_guid}/terms?limit=200", headers=h, timeout=30)
    term_guids = {}
    if r.status_code == 200:
        for t in r.json():
            term_guids[t["name"]] = t["guid"]
    info(f"Total terms: {len(term_guids)}")

    missing_terms = []
    for term_name, (cat, desc) in REQUIRED_TERMS.items():
        if term_name in term_guids:
            ok(f"Term: {term_name} [{cat}]")
        else:
            miss(f"Term: {term_name} [{cat}]")
            missing_terms.append(term_name)

    if missing_terms:
        warn(f"{len(missing_terms)} terms missing")
    else:
        info(f"All {len(REQUIRED_TERMS)} required terms exist")

    # Check term->entity assignments for a few key terms
    print()
    info("Checking term->entity assignments (sample)...")
    sample_terms = ["FHIR Patient", "OMOP CDM", "Bronze-lager", "DICOM"]
    for tn in sample_terms:
        if tn not in term_guids:
            continue
        r = sess.get(f"{ATLAS}/glossary/terms/{term_guids[tn]}?api-version=2022-03-01-preview",
                      headers=h, timeout=30)
        if r.status_code == 200:
            assigned = r.json().get("assignedEntities", [])
            if assigned:
                ok(f"{tn}: mapped to {len(assigned)} entities")
            else:
                warn(f"{tn}: NOT mapped to any entities")
        time.sleep(0.15)

    return glossary_guid, cat_guids, term_guids


def diag_governance_domains(h):
    header("8", "GOVERNANCE DOMAINS")

    domain_guids = {}
    # Try multiple API endpoints
    found = False
    for base in [DG_BASE, f"{ACCT}/datagovernance/catalog"]:
        r = sess.get(f"{base}/domains?api-version={DG_API}", headers=h, timeout=30)
        if r.status_code == 200:
            domains = r.json().get("value", [])
            info(f"Found {len(domains)} governance domains (via {base[:40]}...)")
            for d in domains:
                name = d.get("name", "?")
                did = d.get("id") or d.get("guid", "?")
                domain_guids[name] = did
                ok(f"Domain: {name} (id={str(did)[:12]}...)")
            found = True
            break
        elif r.status_code == 404:
            continue

    if not found:
        # Try datamap API
        for api_ver in ["2023-10-01-preview", "2023-02-01-preview"]:
            r = sess.get(f"{ACCT}/datamap/api/governance-domains?api-version={api_ver}",
                         headers=h, timeout=30)
            if r.status_code == 200:
                domains = r.json().get("value", [])
                for d in domains:
                    name = d.get("name", "?")
                    did = d.get("id") or d.get("guid", "?")
                    domain_guids[name] = did
                    ok(f"Domain: {name}")
                found = True
                break

    if not found:
        warn("Governance domains API not accessible")
        warn("Domains may need to be created manually in portal")

    for expected in EXPECTED_DOMAINS:
        if expected["name"] not in domain_guids:
            miss(f"Domain: {expected['name']}")

    return domain_guids


def diag_data_products(h, domain_guids):
    header("9", "DATA PRODUCTS")

    dp_guids = {}
    for base in [DG_BASE, f"{ACCT}/datagovernance/catalog"]:
        r = sess.get(f"{base}/dataProducts?api-version={DG_API}", headers=h, timeout=30)
        if r.status_code == 200:
            products = r.json().get("value", [])
            info(f"Found {len(products)} data products")
            for p in products:
                name = p.get("name", "?")
                status = p.get("status", "?")
                domain_id = p.get("domainId", "?")
                dp_id = p.get("id") or p.get("guid", "?")
                dp_guids[name] = dp_id
                # Find which domain
                domain_name = "?"
                for dn, did in domain_guids.items():
                    if str(did) == str(domain_id):
                        domain_name = dn
                        break
                ok(f"[{domain_name}] {name} (status={status})")
            break
        elif r.status_code == 404:
            continue

    # Check expected products
    all_expected = []
    for domain, products in EXPECTED_DATA_PRODUCTS.items():
        for p in products:
            all_expected.append((domain, p))
            if p not in dp_guids:
                miss(f"[{domain}] {p}")

    return dp_guids


# ══════════════════════════════════════════════════════════════════════
# FIX FUNCTIONS
# ══════════════════════════════════════════════════════════════════════

def fix_collections(h, existing_collections):
    header("FIX-A", "ENSURE COLLECTIONS EXIST")
    for name, friendly, parent in EXPECTED_COLLECTIONS:
        if name in existing_collections:
            continue
        body = {
            "name": name,
            "friendlyName": friendly,
            "parentCollection": {"referenceName": parent},
        }
        r = sess.put(f"{ACCT}/account/collections/{name}?api-version={COLL_API}",
                      headers=h, json=body, timeout=30)
        if r.status_code in (200, 201):
            fixed(f"Created collection: {friendly} ({name}) -> {parent}")
        else:
            err(f"Collection {name}: {r.status_code} — {r.text[:150]}")
        time.sleep(0.3)


def fix_datasources(h, existing_datasources):
    header("FIX-B", "ENSURE DATA SOURCES EXIST")
    for name, expected in EXPECTED_DATASOURCES.items():
        if name in existing_datasources:
            continue
        body = {
            "kind": expected["kind"],
            "name": name,
            "properties": {
                **expected["properties"],
                "collection": {"referenceName": expected["collection"], "type": "CollectionReference"},
            },
        }
        r = sess.put(f"{SCAN_EP}/datasources/{name}?api-version={SCAN_API}",
                      headers=h, json=body, timeout=30)
        if r.status_code in (200, 201):
            fixed(f"Created datasource: {name} (kind={expected['kind']})")
        else:
            err(f"Datasource {name}: {r.status_code} — {r.text[:150]}")
        time.sleep(0.3)


def fix_scans(h, found_scans):
    header("FIX-C", "ENSURE SCANS EXIST & TRIGGER RUNS")

    for ds_name, scans in EXPECTED_SCANS.items():
        for scan_name, scan_config in scans.items():
            # Check if any scan exists for this datasource
            existing_key = None
            for key in found_scans:
                if key[0] == ds_name:
                    existing_key = key
                    break

            if existing_key:
                info(f"Scan exists: {existing_key[0]}/{existing_key[1]} — checking if run needed")
                # Check last run status
                actual_scan = existing_key[1]
                r = sess.get(
                    f"{SCAN_EP}/datasources/{ds_name}/scans/{actual_scan}/runs?api-version={SCAN_API}",
                    headers=h, timeout=30
                )
                needs_run = True
                if r.status_code == 200:
                    runs = r.json().get("value", [])
                    if runs:
                        last_status = runs[0].get("status", "")
                        if last_status in ("InProgress", "Queued"):
                            info(f"  Scan already running: {last_status}")
                            needs_run = False
                        elif last_status == "Succeeded":
                            # Check age
                            end_time = runs[0].get("endTime", "")
                            info(f"  Last succeeded: {end_time}")
                            needs_run = False  # Don't auto-trigger if it succeeded

                if needs_run:
                    # Trigger a scan run
                    run_id = f"run-{int(time.time())}"
                    r = sess.put(
                        f"{SCAN_EP}/datasources/{ds_name}/scans/{actual_scan}/runs/{run_id}?api-version={SCAN_API}",
                        headers=h, json={}, timeout=30
                    )
                    if r.status_code in (200, 201, 202):
                        fixed(f"Triggered scan: {ds_name}/{actual_scan}")
                    else:
                        warn(f"Could not trigger scan: {r.status_code} — {r.text[:100]}")
            else:
                # Try to create scan
                created = False
                for scan_kind in scan_config["kinds"]:
                    body = {
                        "kind": scan_kind,
                        "name": scan_name,
                        "properties": scan_config["properties"],
                    }
                    r = sess.put(
                        f"{SCAN_EP}/datasources/{ds_name}/scans/{scan_name}?api-version={SCAN_API}",
                        headers=h, json=body, timeout=30
                    )
                    if r.status_code in (200, 201):
                        fixed(f"Created scan: {ds_name}/{scan_name} (kind={scan_kind})")
                        # Trigger run
                        run_id = f"run-{int(time.time())}"
                        r2 = sess.put(
                            f"{SCAN_EP}/datasources/{ds_name}/scans/{scan_name}/runs/{run_id}?api-version={SCAN_API}",
                            headers=h, json={}, timeout=30
                        )
                        if r2.status_code in (200, 201, 202):
                            fixed(f"Triggered scan: {ds_name}/{scan_name}")
                        created = True
                        break
                    else:
                        info(f"  Scan kind {scan_kind}: {r.status_code}")
                if not created:
                    err(f"Could not create scan: {ds_name}/{scan_name}")
            time.sleep(0.3)


def fix_glossary_terms(h, glossary_guid, cat_guids, term_guids):
    header("FIX-D", "ADD MISSING GLOSSARY TERMS")

    if not glossary_guid:
        err("No glossary — cannot create terms")
        return term_guids

    added = 0
    for term_name, (cat, desc) in REQUIRED_TERMS.items():
        if term_name in term_guids:
            continue

        body = {
            "name": term_name,
            "shortDescription": desc,
            "longDescription": desc,
            "anchor": {"glossaryGuid": glossary_guid},
        }
        if cat in cat_guids:
            body["categories"] = [{"categoryGuid": cat_guids[cat]}]

        r = sess.post(f"{ATLAS}/glossary/term", headers=h, json=body, timeout=30)
        if r.status_code in (200, 201):
            term_guids[term_name] = r.json()["guid"]
            fixed(f"Created term: {term_name} [{cat}]")
            added += 1
        else:
            err(f"Term {term_name}: {r.status_code} — {r.text[:100]}")
        time.sleep(0.2)

    info(f"Added {added} terms")
    return term_guids


def fix_fabric_descriptions(h, fabric_guids, lh_guids):
    header("FIX-E", "ADD FABRIC DESCRIPTIONS (correct API)")

    # The correct API is: PUT /entity/guid/{guid}?name=userDescription
    # with Content-Type: application/json and body = json string of description
    desc_fixed = 0

    # Lakehouse descriptions
    for lh_name, desc in LAKEHOUSE_DESCS.items():
        if lh_name not in lh_guids:
            continue
        guid = lh_guids[lh_name]
        r = sess.put(
            f"{ATLAS}/entity/guid/{guid}?name=userDescription",
            headers=h,
            data=json.dumps(desc),
            timeout=30,
        )
        if r.status_code in (200, 204):
            fixed(f"Lakehouse desc: {lh_name}")
            desc_fixed += 1
        else:
            warn(f"Lakehouse {lh_name}: {r.status_code} — {r.text[:100]}")
        time.sleep(0.15)

    # Table descriptions
    for table_name, desc in FABRIC_TABLE_DESCS.items():
        if table_name not in fabric_guids:
            continue
        guid = fabric_guids[table_name]

        # First check if already has description
        r = sess.get(f"{ATLAS}/entity/guid/{guid}?minExtInfo=true", headers=h, timeout=30)
        if r.status_code == 200:
            existing_desc = r.json().get("entity", {}).get("attributes", {}).get("userDescription", "")
            if existing_desc:
                continue  # Already has description

        r = sess.put(
            f"{ATLAS}/entity/guid/{guid}?name=userDescription",
            headers=h,
            data=json.dumps(desc),
            timeout=30,
        )
        if r.status_code in (200, 204):
            fixed(f"Table desc: {table_name}")
            desc_fixed += 1
        else:
            warn(f"Table {table_name}: {r.status_code} — {r.text[:100]}")
        time.sleep(0.12)

    info(f"Descriptions updated: {desc_fixed}")


def fix_term_mappings(h, term_guids, fabric_guids):
    header("FIX-F", "MAP TERMS TO FABRIC ENTITIES")
    mapped = 0
    for term_name, tables in TERM_FABRIC_MAP.items():
        if term_name not in term_guids:
            continue

        term_guid = term_guids[term_name]
        entity_guids = []
        for t in tables:
            if t in fabric_guids:
                entity_guids.append({"guid": fabric_guids[t]})

        if not entity_guids:
            continue

        r = sess.post(
            f"{ATLAS}/glossary/terms/{term_guid}/assignedEntities",
            headers=h, json=entity_guids, timeout=30
        )
        if r.status_code in (200, 201, 204):
            fixed(f"{term_name} -> {len(entity_guids)} entities")
            mapped += 1
        elif r.status_code == 409:
            pass  # Already mapped
        else:
            warn(f"{term_name}: {r.status_code}")
        time.sleep(0.15)

    info(f"Mapped {mapped} terms")


def fix_sql_labels(h, sql_guids):
    header("FIX-G", "ADD LABELS TO SQL ENTITIES")
    labeled = 0
    for table_name, labels in SQL_TABLE_LABELS.items():
        if table_name not in sql_guids:
            continue
        guid = sql_guids[table_name]
        r = sess.put(
            f"{DATAMAP}/entity/guid/{guid}/labels",
            headers=h, json=labels, timeout=30
        )
        if r.status_code == 204:
            fixed(f"{table_name}: {labels}")
            labeled += 1
        elif r.status_code == 200:
            fixed(f"{table_name}: {labels}")
            labeled += 1
        else:
            warn(f"{table_name}: {r.status_code}")
        time.sleep(0.15)
    info(f"Labeled {labeled} entities")


def fix_governance_domains(h, domain_guids):
    header("FIX-H", "CREATE GOVERNANCE DOMAINS")

    for domain in EXPECTED_DOMAINS:
        if domain["name"] in domain_guids:
            continue

        created = False
        body = {
            "name": domain["name"],
            "description": domain["description"],
            "type": "governance-domain",
        }
        for base in [DG_BASE, f"{ACCT}/datagovernance/catalog"]:
            r = sess.post(f"{base}/domains?api-version={DG_API}",
                          headers=h, json=body, timeout=30)
            if r.status_code in (200, 201):
                resp = r.json()
                domain_guids[domain["name"]] = resp.get("id") or resp.get("guid")
                fixed(f"Domain: {domain['name']}")
                created = True
                break
            elif r.status_code == 409:
                info(f"Domain {domain['name']}: already exists")
                # Try to fetch ID
                r2 = sess.get(f"{base}/domains?api-version={DG_API}", headers=h, timeout=30)
                if r2.status_code == 200:
                    for d in r2.json().get("value", []):
                        if d.get("name", "").lower() == domain["name"].lower():
                            domain_guids[domain["name"]] = d.get("id") or d.get("guid")
                created = True
                break

        if not created:
            # Fallback to datamap API
            for api_ver in ["2023-10-01-preview", "2023-02-01-preview"]:
                r = sess.post(f"{ACCT}/datamap/api/governance-domains?api-version={api_ver}",
                              headers=h, json=body, timeout=30)
                if r.status_code in (200, 201):
                    domain_guids[domain["name"]] = r.json().get("id") or r.json().get("guid")
                    fixed(f"Domain: {domain['name']} (datamap API)")
                    created = True
                    break

        if not created:
            warn(f"Could not create domain '{domain['name']}' via API")
            warn("Create manually: Purview portal -> Data Catalog -> Governance domains")
        time.sleep(0.3)

    return domain_guids


def fix_data_products(h, domain_guids, dp_guids):
    header("FIX-I", "CREATE DATA PRODUCTS")

    all_products_cfg = {
        "Klinisk Vård": [
            {
                "name": "Patientdemografi",
                "description": "Demografisk patientdata. Kalla: hca.patients (SQL). Standard: FHIR Patient, OMOP Person.",
                "businessUse": "Patientidentifiering, kohortanalys och demografisk stratifiering.",
                "updateFrequency": "Dagligen via SQL-sync",
                "terms": ["FHIR Patient", "OMOP Person", "Skyddad hälsoinformation (PHI)"],
            },
            {
                "name": "Vårdbesök & utfall",
                "description": "Vardbesoksdata med LOS och aterinlaggningsrisk.",
                "businessUse": "Prediktion av vardtid och aterinlaggningsrisk.",
                "updateFrequency": "Dagligen",
                "terms": ["FHIR Encounter", "OMOP Visit Occurrence", "Vårdtid (LOS)"],
            },
            {
                "name": "Diagnoser (ICD-10)",
                "description": "Diagnosinformation klassificerad med ICD-10.",
                "businessUse": "Epidemiologisk analys och DRG-klassificering.",
                "updateFrequency": "Dagligen",
                "terms": ["ICD-10", "FHIR Condition", "OMOP Condition Occurrence"],
            },
            {
                "name": "Medicinering (ATC)",
                "description": "Lakemedelsdata klassificerad med ATC.",
                "businessUse": "Lakemedelsinteraktionsanalys och farmakovigilans.",
                "updateFrequency": "Dagligen",
                "terms": ["ATC-klassificering", "FHIR MedicationRequest", "OMOP Drug Exposure"],
            },
            {
                "name": "Vitalparametrar & labb",
                "description": "Vitalparametrar och labresultat.",
                "businessUse": "Early Warning Score, ML-features.",
                "updateFrequency": "Realtid via SQL-sync",
                "terms": ["Vitalparametrar", "Labresultat", "FHIR Observation", "OMOP Measurement"],
            },
            {
                "name": "ML-prediktion (LOS & readmission)",
                "description": "ML-modell for vardtid och aterinlaggningsprediktion.",
                "businessUse": "Kliniskt beslutsstod och resursoptimering.",
                "updateFrequency": "Dagligen via Medallion-pipeline",
                "terms": ["Medallion-arkitektur", "Gold-lager", "ML-prediktion"],
            },
        ],
        "Barncancerforskning": [
            {
                "name": "FHIR Patientresurser",
                "description": "BrainChild FHIR R4-resurser: Patient, Encounter, Condition, Observation, Specimen.",
                "businessUse": "Interoperabel patientdata for multicenter-forskning.",
                "updateFrequency": "Veckovis via FHIR-sync",
                "terms": ["FHIR R4", "FHIR Patient", "FHIR Specimen", "FHIR ImagingStudy"],
            },
            {
                "name": "Medicinsk bilddiagnostik (DICOM)",
                "description": "MR-hjarna och patologidata i DICOM-format.",
                "businessUse": "AI-baserad bildanalys och tumorklassificering.",
                "updateFrequency": "Veckovis",
                "terms": ["DICOM", "FHIR ImagingStudy"],
            },
            {
                "name": "Genomik (GMS/VCF)",
                "description": "Genomiska varianter i VCF-format och GMS DiagnosticReports.",
                "businessUse": "Precisionsmedicin och variantklassificering.",
                "updateFrequency": "Per sekvenseringskörning",
                "terms": ["Genomic Medicine Sweden (GMS)", "VCF (Variant Call Format)", "Genomisk variant"],
            },
            {
                "name": "Biobanksdata (BTB)",
                "description": "Barntumorbankens provdata — FHIR Specimen med VCF-koppling.",
                "businessUse": "Provsparbarhet och forskningssamarbeten.",
                "updateFrequency": "Veckovis",
                "terms": ["BTB (Barntumörbanken)", "FHIR Specimen"],
            },
            {
                "name": "Kvalitetsregister (SBCR)",
                "description": "Svenska Barncancerregistret — registrering, behandling, uppfoljning.",
                "businessUse": "Nationell kvalitetsuppfoljning och overlevnadsstatistik.",
                "updateFrequency": "Månadsvis",
                "terms": ["SBCR (Svenska Barncancerregistret)"],
            },
        ],
    }

    created_count = 0
    for domain_name, products in all_products_cfg.items():
        domain_id = domain_guids.get(domain_name)
        if not domain_id:
            warn(f"No domain ID for '{domain_name}' — skipping products")
            continue

        for product in products:
            if product["name"] in dp_guids:
                continue

            body = {
                "name": product["name"],
                "description": product["description"],
                "domainId": domain_id,
            }
            if "businessUse" in product:
                body["businessUse"] = product["businessUse"]
            if "updateFrequency" in product:
                body["updateFrequency"] = product["updateFrequency"]

            created = False
            for base in [DG_BASE, f"{ACCT}/datagovernance/catalog"]:
                r = sess.post(f"{base}/dataProducts?api-version={DG_API}",
                              headers=h, json=body, timeout=30)
                if r.status_code in (200, 201):
                    resp = r.json()
                    dp_id = resp.get("id") or resp.get("guid")
                    dp_guids[product["name"]] = dp_id
                    fixed(f"[{domain_name}] {product['name']}")
                    created_count += 1
                    created = True
                    break
                elif r.status_code == 409:
                    info(f"[{domain_name}] {product['name']} already exists")
                    created = True
                    break

            if not created:
                warn(f"[{domain_name}] {product['name']}: {r.status_code}")
            time.sleep(0.2)

    info(f"Created {created_count} data products")
    return dp_guids


def fix_sql_term_mappings(h, term_guids, sql_guids):
    """Map glossary terms to SQL entities (not just Fabric)."""
    header("FIX-J", "MAP TERMS TO SQL ENTITIES")

    sql_term_map = {
        "ICD-10": ["diagnoses"],
        "ATC-klassificering": ["medications"],
        "Skyddad hälsoinformation (PHI)": ["patients", "encounters"],
        "FHIR R4": ["patients", "encounters", "diagnoses", "vitals_labs", "medications"],
        "FHIR Patient": ["patients"],
        "FHIR Encounter": ["encounters"],
        "FHIR Condition": ["diagnoses"],
        "FHIR MedicationRequest": ["medications"],
        "FHIR Observation": ["vitals_labs"],
        "OMOP CDM": ["patients", "encounters", "diagnoses", "vitals_labs", "medications"],
        "OMOP Person": ["patients"],
        "OMOP Visit Occurrence": ["encounters"],
        "OMOP Condition Occurrence": ["diagnoses"],
        "OMOP Drug Exposure": ["medications"],
        "OMOP Measurement": ["vitals_labs"],
        "Vårdtid (LOS)": ["encounters"],
        "Återinläggningsrisk": ["encounters"],
        "Vitalparametrar": ["vitals_labs"],
        "Labresultat": ["vitals_labs"],
        "Medallion-arkitektur": ["vw_ml_encounters"],
        "Gold-lager": ["vw_ml_encounters"],
    }

    mapped = 0
    for term_name, tables in sql_term_map.items():
        if term_name not in term_guids:
            continue
        term_guid = term_guids[term_name]
        entity_guids = [{"guid": sql_guids[t]} for t in tables if t in sql_guids]
        if not entity_guids:
            continue

        r = sess.post(
            f"{ATLAS}/glossary/terms/{term_guid}/assignedEntities",
            headers=h, json=entity_guids, timeout=30
        )
        if r.status_code in (200, 201, 204):
            fixed(f"{term_name} -> {len(entity_guids)} SQL entities")
            mapped += 1
        time.sleep(0.15)

    info(f"Mapped {mapped} terms to SQL entities")


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Purview Full Diagnostic & Fix")
    parser.add_argument("--diag-only", action="store_true", help="Only diagnose, don't fix")
    args = parser.parse_args()

    print(f"\n{BOLD}{'=' * 70}{RESET}")
    print(f"{BOLD}  PURVIEW FULL DIAGNOSTIC {'(diag only)' if args.diag_only else '& FIX'}{RESET}")
    print(f"{BOLD}  Account: prviewacc.purview.azure.com{RESET}")
    print(f"{BOLD}  Time: {time.strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}{'=' * 70}{RESET}")

    h = get_headers()

    # ── DIAGNOSE EVERYTHING ──
    existing_collections = diag_collections(h)
    existing_datasources = diag_datasources(h)
    found_scans = diag_scans(h)
    entity_counts = diag_entities(h)
    sql_guids = diag_sql_entities(h)
    lh_guids, fabric_guids = diag_fabric_entities(h)
    glossary_guid, cat_guids, term_guids = diag_glossary(h)
    domain_guids = diag_governance_domains(h)
    dp_guids = diag_data_products(h, domain_guids)

    # ── SUMMARY ──
    header("DIAG", "DIAGNOSTIC SUMMARY")
    print(f"""
  {BOLD}Component               OK    Missing{RESET}
  ──────────────────────  ────  ───────
  Collections             {sum(1 for c in [n for n,_,_ in EXPECTED_COLLECTIONS] if c in existing_collections):>4}  {sum(1 for c in [n for n,_,_ in EXPECTED_COLLECTIONS] if c not in existing_collections):>7}
  Data Sources            {sum(1 for d in EXPECTED_DATASOURCES if d in existing_datasources):>4}  {sum(1 for d in EXPECTED_DATASOURCES if d not in existing_datasources):>7}
  SQL Entities            {len(sql_guids):>4}  {6 - len(sql_guids):>7}
  Fabric Tables           {len(fabric_guids):>4}  {len(FABRIC_TABLE_DESCS) - len(fabric_guids):>7}
  Fabric Lakehouses       {len(lh_guids):>4}  {len(LAKEHOUSE_DESCS) - len(lh_guids):>7}
  Glossary Terms          {sum(1 for t in REQUIRED_TERMS if t in term_guids):>4}  {sum(1 for t in REQUIRED_TERMS if t not in term_guids):>7}
  Governance Domains      {len(domain_guids):>4}  {len(EXPECTED_DOMAINS) - len(domain_guids):>7}
  Data Products           {len(dp_guids):>4}  {sum(len(v) for v in EXPECTED_DATA_PRODUCTS.values()) - len(dp_guids):>7}

  {BOLD}Totals: OK={stats['ok']}  Missing={stats['missing']}  Errors={stats['errors']}{RESET}
""")

    if args.diag_only:
        print(f"  Run without --diag-only to fix missing items")
        return

    # ── FIX EVERYTHING ──
    print(f"\n{BOLD}{CYAN}{'=' * 70}{RESET}")
    print(f"{BOLD}{CYAN}  STARTING FIXES...{RESET}")
    print(f"{BOLD}{CYAN}{'=' * 70}{RESET}")

    # Refresh token before fixes
    h = get_headers()

    fix_collections(h, existing_collections)
    fix_datasources(h, {d["name"]: d for d in existing_datasources.values()} if isinstance(existing_datasources, dict) else existing_datasources)
    fix_scans(h, found_scans)
    term_guids = fix_glossary_terms(h, glossary_guid, cat_guids, term_guids)
    fix_fabric_descriptions(h, fabric_guids, lh_guids)
    fix_term_mappings(h, term_guids, fabric_guids)
    fix_sql_term_mappings(h, term_guids, sql_guids)
    fix_sql_labels(h, sql_guids)
    domain_guids = fix_governance_domains(h, domain_guids)
    dp_guids = fix_data_products(h, domain_guids, dp_guids)

    # ── FINAL SUMMARY ──
    header("FINAL", "FIX SUMMARY")
    print(f"""
  {BOLD}{GREEN}Fixed:    {stats['fixed']}{RESET}
  {BOLD}{RED}Errors:   {stats['errors']}{RESET}
  {BOLD}OK:       {stats['ok']}{RESET}
  {BOLD}Missing:  {stats['missing']}{RESET}

  {DIM}Re-run with --diag-only to verify results.{RESET}
  {DIM}Some changes (scan results) may take 2-5 minutes to appear.{RESET}
""")


if __name__ == "__main__":
    main()
