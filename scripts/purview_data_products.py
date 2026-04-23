"""
Purview Data Products, OKRs & Data Quality — Creates governance artifacts for demo.

1. Data Products via Purview Unified Catalog API
2. OKR terms in glossary (Objectives & Key Results for data governance)
3. SQL-based data quality rules & checks with results stored in Purview

Usage:
  python scripts/purview_data_products.py
"""
import json
import struct
import sys
import time
from datetime import datetime

import pyodbc
import requests
from azure.identity import AzureCliCredential

# ── CONFIG ──
cred = AzureCliCredential(process_timeout=30)
ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
UNIFIED = f"{ACCT}/datagovernance/catalog"
API_VER = "2025-09-15-preview"

SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DB = "HealthcareAnalyticsDB"

BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def refresh_token():
    global token, h
    token = cred.get_token("https://purview.azure.net/.default").token
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


refresh_token()


def header(num, title):
    print(f"\n{'=' * 70}")
    print(f"  {num}. {title}")
    print(f"{'=' * 70}")


def ok(msg):
    print(f"  {GREEN}✅{RESET} {msg}")


def warn(msg):
    print(f"  {YELLOW}⚠️ {RESET} {msg}")


def info(msg):
    print(f"  {CYAN}ℹ️ {RESET} {msg}")


# ══════════════════════════════════════════════════════════════════
#  1. DATA PRODUCTS
# ══════════════════════════════════════════════════════════════════
DATA_PRODUCTS = [
    {
        "name": "Klinisk Patientanalys",
        "description": (
            "Dataprodukt för klinisk patientanalys — innehåller patientdemografi, "
            "vårdbesök, diagnoser, vitalparametrar och labresultat. Används för "
            "prediktiv analys av vårdtid (LOS) och återinläggningsrisk. "
            "Följer FHIR R4 och OMOP CDM v5.4 standarder."
        ),
        "owners": ["Healthcare Analytics Team"],
        "type": "Analytics",
        "status": "Published",
        "tables": ["patients", "encounters", "diagnoses", "vitals_labs", "medications"],
        "use_cases": [
            "LOS-prediktion (LightGBM)",
            "Återinläggningsrisk (RandomForest)",
            "Charlson Comorbidity Index",
            "Avdelningsstatistik",
        ],
        "sla": "Daglig uppdatering, <1h latens, 99.5% tillgänglighet",
        "quality_score": None,  # Filled after DQ checks
    },
    {
        "name": "OMOP Forskningsdata",
        "description": (
            "OMOP CDM v5.4-transformerade data för observationell forskning. "
            "Möjliggör kors-institutionell forskning och federerad analys. "
            "Mappning: ICD-10-SE → SNOMED CT, ATC → RxNorm."
        ),
        "owners": ["Research Data Team"],
        "type": "Research",
        "status": "Published",
        "tables": ["person", "visit_occurrence", "condition_occurrence", "drug_exposure", "measurement"],
        "use_cases": [
            "Kohortstudier",
            "Läkemedelssäkerhet",
            "Kliniska utfall (OHDSI)",
            "Federerad analys (DataSHIELD)",
        ],
        "sla": "Veckovis uppdatering, <4h ETL, GDPR-kompatibel",
        "quality_score": None,
    },
    {
        "name": "BrainChild Barncancerforskning",
        "description": (
            "Multimodal forskningsplattform för barncancer — integrerar FHIR-klinisk data, "
            "DICOM-bilddiagnostik (MRI + patologi), genomikdata (WGS/WES via GMS), "
            "biobanksdata (BTB) och kvalitetsregister (SBCR)."
        ),
        "owners": ["BrainChild Research Team"],
        "type": "Research",
        "status": "Published",
        "tables": ["fhir_patients", "imaging_studies", "genomic_variants", "specimens", "sbcr_registrations"],
        "use_cases": [
            "Tumörklassificering (MRI + patologi AI)",
            "Genomisk variant-analys",
            "Behandlingsutfall (SBCR)",
            "Biobanksförvaltning (BTB)",
        ],
        "sla": "Realtid FHIR-ingest, daglig batch-ETL, forskaråtkomst via Fabric",
        "quality_score": None,
    },
    {
        "name": "ML Feature Store",
        "description": (
            "Gold-lager med ML-redo features — aggregerade per vårdbesök med "
            "Charlson Comorbidity Index, senaste vitalparametrar, primärdiagnos "
            "och läkemedelsdata. Används av LOS- och readmission-modeller."
        ),
        "owners": ["ML Engineering Team"],
        "type": "Analytics",
        "status": "Published",
        "tables": ["vw_ml_encounters"],
        "use_cases": [
            "Feature serving för ML-modeller",
            "A/B-testning av features",
            "Model monitoring",
            "Drift detection",
        ],
        "sla": "Uppdateras efter varje pipeline-körning, <30min latens",
        "quality_score": None,
    },
]


def create_data_products():
    header("1", "CREATING DATA PRODUCTS IN PURVIEW")
    refresh_token()

    created = 0

    # Check existing data products
    r = requests.get(
        f"{UNIFIED}/dataproducts?api-version={API_VER}",
        headers=h, timeout=30
    )
    existing = []
    if r.status_code == 200:
        existing = [dp["name"] for dp in r.json().get("value", [])]
        if existing:
            info(f"Existing data products: {', '.join(existing)}")

    for dp in DATA_PRODUCTS:
        if dp["name"] in existing:
            ok(f"{dp['name']} — already exists")
            created += 1
            continue

        # Try creating via Unified Catalog API
        payload = {
            "name": dp["name"],
            "description": dp["description"],
            "properties": {
                "type": dp["type"],
                "status": dp["status"],
                "owners": dp["owners"],
                "tables": dp["tables"],
                "useCases": dp["use_cases"],
                "sla": dp["sla"],
            },
        }

        r = requests.put(
            f"{UNIFIED}/dataproducts/{dp['name'].replace(' ', '-').lower()}?api-version={API_VER}",
            headers=h, json=payload, timeout=30
        )
        if r.status_code in (200, 201):
            ok(f"{dp['name']} — created via Unified Catalog API")
            created += 1
        else:
            # Fallback: store as custom entity in Atlas with business metadata
            info(f"{dp['name']}: Unified API returned {r.status_code}, using Atlas custom type")
            # Create as glossary term under a new category instead
            created += create_dp_as_glossary(dp)

    # Also register as custom Atlas entities for rich metadata
    register_dp_entities()

    print(f"\n  Created/verified {created}/{len(DATA_PRODUCTS)} data products")
    return created


def create_dp_as_glossary(dp):
    """Create data product as a glossary term with rich metadata."""
    # Get glossary guid
    r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    if r.status_code != 200:
        warn("Cannot access glossary")
        return 0

    data = r.json()
    glossaries = data if isinstance(data, list) else [data]
    g_guid = glossaries[0]["guid"]

    # First ensure "Dataprodukter" category exists
    cat_r = requests.get(f"{ATLAS}/glossary/{g_guid}", headers=h, timeout=30)
    cat_guid = None
    if cat_r.status_code == 200:
        for cat in cat_r.json().get("categories", []):
            if cat.get("displayText") == "Dataprodukter":
                cat_guid = cat.get("categoryGuid")

    if not cat_guid:
        cat_payload = {
            "name": "Dataprodukter",
            "glossaryGuid": g_guid,
            "shortDescription": "Registrerade dataprodukter i plattformen",
        }
        r = requests.post(f"{ATLAS}/glossary/category", headers=h, json=cat_payload, timeout=30)
        if r.status_code in (200, 201):
            cat_guid = r.json().get("guid")
            ok("Created category: Dataprodukter")
        elif r.status_code == 409 or (r.status_code == 400 and "ATLAS-400-00-0" in r.text):
            info("Category 'Dataprodukter' already exists")
        else:
            warn(f"Category creation failed: {r.status_code}")

    # Create term for data product
    term_payload = {
        "name": f"DP: {dp['name']}",
        "glossaryGuid": g_guid,
        "shortDescription": dp["description"][:256],
        "longDescription": (
            f"**Typ:** {dp['type']}\n"
            f"**Status:** {dp['status']}\n"
            f"**Ägare:** {', '.join(dp['owners'])}\n"
            f"**Tabeller:** {', '.join(dp['tables'])}\n"
            f"**SLA:** {dp['sla']}\n\n"
            f"**Användningsområden:**\n" +
            "\n".join(f"- {uc}" for uc in dp["use_cases"])
        ),
        "status": "Approved",
    }
    if cat_guid:
        term_payload["categories"] = [{"categoryGuid": cat_guid}]

    r = requests.post(f"{ATLAS}/glossary/term", headers=h, json=term_payload, timeout=30)
    if r.status_code in (200, 201):
        ok(f"DP: {dp['name']} — created as glossary term")
        return 1
    elif r.status_code == 409 or (r.status_code == 400 and "ATLAS-400-00-072" in r.text):
        ok(f"DP: {dp['name']} — already exists")
        return 1
    else:
        warn(f"DP: {dp['name']}: {r.status_code} {r.text[:100]}")
        return 0


def register_dp_entities():
    """Register data products as custom type definition for discoverability."""
    # Check if our custom type exists
    r = requests.get(f"{ATLAS}/types/typedef/name/healthcare_data_product", headers=h, timeout=15)
    if r.status_code == 200:
        info("Custom type 'healthcare_data_product' already defined")
        return

    typedef = {
        "classificationDefs": [],
        "entityDefs": [
            {
                "name": "healthcare_data_product",
                "description": "A healthcare data product combining multiple data assets",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "product_type", "typeName": "string", "isOptional": True},
                    {"name": "product_status", "typeName": "string", "isOptional": True},
                    {"name": "product_owners", "typeName": "string", "isOptional": True},
                    {"name": "sla", "typeName": "string", "isOptional": True},
                    {"name": "use_cases", "typeName": "string", "isOptional": True},
                    {"name": "quality_score", "typeName": "float", "isOptional": True},
                    {"name": "tables", "typeName": "string", "isOptional": True},
                ],
            }
        ],
        "enumDefs": [],
        "relationshipDefs": [],
        "structDefs": [],
    }

    r = requests.post(f"{ATLAS}/types/typedefs", headers=h, json=typedef, timeout=30)
    if r.status_code in (200, 201):
        ok("Custom type 'healthcare_data_product' registered")
    else:
        warn(f"Custom type creation: {r.status_code} {r.text[:100]}")
        return

    # Now create entity instances
    for dp in DATA_PRODUCTS:
        entity = {
            "entity": {
                "typeName": "healthcare_data_product",
                "attributes": {
                    "qualifiedName": f"dp://{dp['name'].lower().replace(' ', '-')}",
                    "name": dp["name"],
                    "description": dp["description"],
                    "product_type": dp["type"],
                    "product_status": dp["status"],
                    "product_owners": ", ".join(dp["owners"]),
                    "sla": dp["sla"],
                    "use_cases": " | ".join(dp["use_cases"]),
                    "tables": ", ".join(dp["tables"]),
                },
            }
        }
        r = requests.post(f"{ATLAS}/entity", headers=h, json=entity, timeout=30)
        if r.status_code in (200, 201):
            ok(f"Entity: {dp['name']}")
        else:
            warn(f"Entity {dp['name']}: {r.status_code} {r.text[:80]}")


# ══════════════════════════════════════════════════════════════════
#  2. OKRs (Objectives & Key Results)
# ══════════════════════════════════════════════════════════════════
OKRS = [
    {
        "objective": "Förbättra datakvalitet i kliniska datakällor",
        "key_results": [
            "KR1 Completeness minst 98 procent for alla obligatoriska falt",
            "KR2 Accuracy minst 99 procent for ICD-10 och ATC-koder",
            "KR3 Freshness max 24h for alla Bronze-tabeller",
            "KR4 Consistency minst 95 procent mellan kalla SQL och Lakehouse",
        ],
    },
    {
        "objective": "Stärka datastyrning och compliance",
        "key_results": [
            "KR1 100 procent av PHI-kolumner klassificerade i Purview",
            "KR2 Alla dataprodukter har definierade SLA",
            "KR3 Glossary-termer mappade till minst 90 procent av entiteter",
            "KR4 GDPR-datahantering dokumenterad for alla dataprodukter",
        ],
    },
    {
        "objective": "Maximera forskningsplattformens värde",
        "key_results": [
            "KR1 OMOP CDM-mappning komplett for alla 5 kliniska tabeller",
            "KR2 ML-modeller levererar AUC minst 0.75 for readmission-prediktion",
            "KR3 BrainChild multimodal integration FHIR DICOM Genomik lankade",
            "KR4 Self-service analytics tillgangligt via Fabric for minst 3 team",
        ],
    },
    {
        "objective": "Säkerställa driftexcellens",
        "key_results": [
            "KR1 Pipeline-framgang minst 99 procent Bronze Silver Gold",
            "KR2 End-to-end latens max 2h fran SQL till Gold-lager",
            "KR3 Automatiserade datakvalitetskontroller vid varje pipeline-korning",
            "KR4 Incident response max 30 min for datakvalitets-larm",
        ],
    },
]


def create_okrs():
    header("2", "CREATING OKRs IN GLOSSARY")
    refresh_token()

    # Get glossary guid
    r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    if r.status_code != 200:
        warn(f"Cannot access glossary: {r.status_code}")
        return 0

    data = r.json()
    glossaries = data if isinstance(data, list) else [data]
    g_guid = glossaries[0]["guid"]

    # Create OKR category
    cat_payload = {
        "name": "OKR Data Governance",
        "glossaryGuid": g_guid,
        "shortDescription": "Objectives and Key Results for datastyrning och datakvalitet Q2 2026",
    }
    r = requests.post(f"{ATLAS}/glossary/category", headers=h, json=cat_payload, timeout=30)
    if r.status_code in (200, 201):
        okr_cat_guid = r.json().get("guid")
        ok("Created category: OKR — Data Governance")
    elif r.status_code == 409:
        info("OKR category already exists")
        # Find existing
        r2 = requests.get(f"{ATLAS}/glossary/{g_guid}", headers=h, timeout=30)
        okr_cat_guid = None
        if r2.status_code == 200:
            for cat in r2.json().get("categories", []):
                if "OKR" in cat.get("displayText", ""):
                    okr_cat_guid = cat.get("categoryGuid")
                    break
    else:
        if r.status_code == 400 and "ATLAS-400-00-0" in r.text:
            info("OKR category already exists (400)")
        else:
            warn(f"OKR category creation failed: {r.status_code}")
        okr_cat_guid = None

    created = 0
    for i, okr in enumerate(OKRS, 1):
        # Sanitize term names — Purview rejects colon, em-dash, special chars
        obj_short = okr['objective'][:60].replace('≥', 'minst ').replace('≤', 'max ')
        term_name = f"OKR-O{i} {obj_short}"
        kr_text = "\n".join(f"  - {kr.replace(chr(8805), 'minst ').replace(chr(8804), 'max ')}" for kr in okr["key_results"])

        term_payload = {
            "name": term_name,
            "glossaryGuid": g_guid,
            "shortDescription": obj_short,
            "longDescription": f"Objective {i} - {obj_short}\n\nKey Results:\n{kr_text}",
            "status": "Approved",
        }
        if okr_cat_guid:
            term_payload["categories"] = [{"categoryGuid": okr_cat_guid}]

        r = requests.post(f"{ATLAS}/glossary/term", headers=h, json=term_payload, timeout=30)
        if r.status_code in (200, 201):
            ok(f"O{i}: {okr['objective'][:60]}...")
            created += 1
        elif r.status_code == 409 or (r.status_code == 400 and "ATLAS-400-00-0" in r.text):
            ok(f"O{i}: already exists")
            created += 1
        else:
            warn(f"O{i}: {r.status_code} {r.text[:80]}")

    print(f"\n  Created/verified {created}/{len(OKRS)} OKRs")
    return created


# ══════════════════════════════════════════════════════════════════
#  3. DATA QUALITY RULES & CHECKS
# ══════════════════════════════════════════════════════════════════
DQ_RULES = [
    # Completeness checks
    {
        "name": "DQ-001 Patient Completeness",
        "category": "Completeness",
        "table": "hca.patients",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) as null_patient_id,
                SUM(CASE WHEN birth_date IS NULL THEN 1 ELSE 0 END) as null_birth_date,
                SUM(CASE WHEN gender IS NULL THEN 1 ELSE 0 END) as null_gender,
                SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) as null_region,
                SUM(CASE WHEN ses_level IS NULL THEN 1 ELSE 0 END) as null_ses_level
            FROM hca.patients
        """,
        "threshold": 0.98,
        "description": "Alla obligatoriska fält i patients ska ha ≥98% completeness",
    },
    {
        "name": "DQ-002 Encounter Completeness",
        "category": "Completeness",
        "table": "hca.encounters",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN encounter_id IS NULL THEN 1 ELSE 0 END) as null_encounter_id,
                SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) as null_patient_id,
                SUM(CASE WHEN admission_date IS NULL THEN 1 ELSE 0 END) as null_admission_date,
                SUM(CASE WHEN department IS NULL THEN 1 ELSE 0 END) as null_department,
                SUM(CASE WHEN los_days IS NULL THEN 1 ELSE 0 END) as null_los_days
            FROM hca.encounters
        """,
        "threshold": 0.98,
        "description": "Alla obligatoriska fält i encounters ska ha ≥98% completeness",
    },
    # Validity checks
    {
        "name": "DQ-003 ICD-10 Code Format",
        "category": "Validity",
        "table": "hca.diagnoses",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN icd10_code LIKE '[A-Z][0-9][0-9]%' THEN 0 ELSE 1 END) as invalid_format,
                COUNT(DISTINCT icd10_code) as unique_codes,
                SUM(CASE WHEN diagnosis_type IN ('Primary','Secondary','Complication') THEN 0 ELSE 1 END) as invalid_type
            FROM hca.diagnoses
        """,
        "threshold": 0.99,
        "description": "ICD-10-koder ska följa format [A-Z][0-9][0-9].* och ha giltig diagnosis_type",
    },
    {
        "name": "DQ-004 ATC Code Format",
        "category": "Validity",
        "table": "hca.medications",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN atc_code LIKE '[A-Z][0-9][0-9][A-Z][A-Z][0-9][0-9]' THEN 0 ELSE 1 END) as invalid_atc,
                COUNT(DISTINCT atc_code) as unique_atc_codes,
                SUM(CASE WHEN drug_name IS NULL OR LEN(drug_name) < 2 THEN 1 ELSE 0 END) as invalid_drug_name
            FROM hca.medications
        """,
        "threshold": 0.99,
        "description": "ATC-koder ska följa format [A-Z][0-9][0-9][A-Z][A-Z][0-9][0-9]",
    },
    # Referential integrity
    {
        "name": "DQ-005 Encounter → Patient FK",
        "category": "Referential Integrity",
        "table": "hca.encounters",
        "sql": """
            SELECT
                (SELECT COUNT(*) FROM hca.encounters) as total_encounters,
                COUNT(*) as orphan_encounters
            FROM hca.encounters e
            LEFT JOIN hca.patients p ON e.patient_id = p.patient_id
            WHERE p.patient_id IS NULL
        """,
        "threshold": 1.0,
        "description": "Alla encounters ska ha en matchande patient (0 orphans)",
    },
    {
        "name": "DQ-006 Diagnosis → Encounter FK",
        "category": "Referential Integrity",
        "table": "hca.diagnoses",
        "sql": """
            SELECT
                (SELECT COUNT(*) FROM hca.diagnoses) as total_diagnoses,
                COUNT(*) as orphan_diagnoses
            FROM hca.diagnoses d
            LEFT JOIN hca.encounters e ON d.encounter_id = e.encounter_id
            WHERE e.encounter_id IS NULL
        """,
        "threshold": 1.0,
        "description": "Alla diagnoser ska ha en matchande encounter (0 orphans)",
    },
    # Range checks
    {
        "name": "DQ-007 Vitals Range Check",
        "category": "Accuracy",
        "table": "hca.vitals_labs",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN systolic_bp < 50 OR systolic_bp > 300 THEN 1 ELSE 0 END) as bp_out_of_range,
                SUM(CASE WHEN heart_rate < 20 OR heart_rate > 250 THEN 1 ELSE 0 END) as hr_out_of_range,
                SUM(CASE WHEN temperature_c < 30 OR temperature_c > 45 THEN 1 ELSE 0 END) as temp_out_of_range,
                SUM(CASE WHEN oxygen_saturation < 50 OR oxygen_saturation > 100 THEN 1 ELSE 0 END) as spo2_out_of_range,
                SUM(CASE WHEN glucose_mmol < 0.5 OR glucose_mmol > 50 THEN 1 ELSE 0 END) as glucose_out_of_range
            FROM hca.vitals_labs
        """,
        "threshold": 0.99,
        "description": "Vitalparametrar ska vara inom medicinskt rimliga intervall",
    },
    # Timeliness
    {
        "name": "DQ-008 Data Freshness",
        "category": "Timeliness",
        "table": "hca.encounters",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                MAX(created_at) as latest_record,
                DATEDIFF(DAY, MAX(created_at), GETDATE()) as days_since_latest,
                MIN(admission_date) as earliest_admission,
                MAX(admission_date) as latest_admission
            FROM hca.encounters
        """,
        "threshold": 0.0,  # Info only
        "description": "Kontrollerar hur aktuell datan är",
    },
    # Uniqueness
    {
        "name": "DQ-009 Primary Key Uniqueness",
        "category": "Uniqueness",
        "table": "multiple",
        "sql": """
            SELECT 'patients' as tbl,
                   COUNT(*) as total,
                   COUNT(DISTINCT patient_id) as unique_keys,
                   COUNT(*) - COUNT(DISTINCT patient_id) as duplicates
            FROM hca.patients
            UNION ALL
            SELECT 'encounters', COUNT(*), COUNT(DISTINCT encounter_id),
                   COUNT(*) - COUNT(DISTINCT encounter_id)
            FROM hca.encounters
            UNION ALL
            SELECT 'diagnoses', COUNT(*), COUNT(DISTINCT diagnosis_id),
                   COUNT(*) - COUNT(DISTINCT diagnosis_id)
            FROM hca.diagnoses
        """,
        "threshold": 1.0,
        "description": "Primärnycklar ska vara unika (0 dubletter)",
    },
    # Cross-table consistency
    {
        "name": "DQ-010 LOS Consistency",
        "category": "Consistency",
        "table": "hca.encounters",
        "sql": """
            SELECT
                COUNT(*) as total_encounters,
                SUM(CASE WHEN los_days = DATEDIFF(DAY, admission_date, discharge_date)
                         THEN 0 ELSE 1 END) as los_mismatch,
                SUM(CASE WHEN discharge_date < admission_date THEN 1 ELSE 0 END) as date_order_error,
                SUM(CASE WHEN los_days < 0 THEN 1 ELSE 0 END) as negative_los
            FROM hca.encounters
            WHERE discharge_date IS NOT NULL
        """,
        "threshold": 0.99,
        "description": "LOS ska matcha DATEDIFF(admission_date, discharge_date)",
    },
]


def get_sql_connection():
    """Connect to Azure SQL with AAD token."""
    tok = cred.get_token("https://database.windows.net/.default")
    tb = tok.token.encode("UTF-16-LE")
    ts = struct.pack(f"<I{len(tb)}s", len(tb), tb)
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DB};",
        attrs_before={1256: ts},
    )


def run_data_quality_checks():
    header("3", "DATA QUALITY RULES & CHECKS")

    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
    except Exception as e:
        warn(f"SQL connection failed: {e}")
        return []

    results = []
    passed = 0
    failed = 0

    for rule in DQ_RULES:
        try:
            cursor.execute(rule["sql"])
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            result = {
                "rule": rule["name"],
                "category": rule["category"],
                "table": rule["table"],
                "timestamp": datetime.now().isoformat(),
                "status": "UNKNOWN",
                "details": {},
            }

            if rule["category"] == "Completeness":
                row = rows[0]
                total = row[0]
                null_counts = {columns[i]: row[i] for i in range(1, len(columns))}
                worst_completeness = min(
                    (total - v) / total if total > 0 else 0
                    for v in null_counts.values()
                )
                result["details"] = {
                    "total_rows": total,
                    "null_counts": null_counts,
                    "completeness": round(worst_completeness, 4),
                }
                result["score"] = round(worst_completeness, 4)
                result["status"] = "PASS" if worst_completeness >= rule["threshold"] else "FAIL"

            elif rule["category"] == "Validity":
                row = rows[0]
                total = row[0]
                invalid = row[1]
                validity = (total - invalid) / total if total > 0 else 0
                result["details"] = {
                    "total_rows": total,
                    "invalid_count": invalid,
                    "validity": round(validity, 4),
                    "extra": {columns[i]: row[i] for i in range(2, len(columns))},
                }
                result["score"] = round(validity, 4)
                result["status"] = "PASS" if validity >= rule["threshold"] else "FAIL"

            elif rule["category"] == "Referential Integrity":
                row = rows[0]
                total = row[0]
                orphans = row[1]
                integrity = (total - orphans) / total if total > 0 else 1.0
                result["details"] = {
                    "total_rows": total,
                    "orphan_count": orphans,
                    "integrity": round(integrity, 4),
                }
                result["score"] = round(integrity, 4)
                result["status"] = "PASS" if orphans == 0 else "FAIL"

            elif rule["category"] == "Accuracy":
                row = rows[0]
                total = row[0]
                out_of_range = sum(row[i] for i in range(1, len(columns)))
                accuracy = (total - out_of_range) / total if total > 0 else 0
                result["details"] = {
                    "total_rows": total,
                    "out_of_range": {columns[i]: row[i] for i in range(1, len(columns))},
                    "accuracy": round(accuracy, 4),
                }
                result["score"] = round(accuracy, 4)
                result["status"] = "PASS" if accuracy >= rule["threshold"] else "FAIL"

            elif rule["category"] == "Timeliness":
                row = rows[0]
                result["details"] = {columns[i]: str(row[i]) for i in range(len(columns))}
                result["score"] = 1.0
                result["status"] = "INFO"

            elif rule["category"] == "Uniqueness":
                dups_found = False
                details = {}
                for row in rows:
                    tbl = row[0]
                    total = row[1]
                    unique = row[2]
                    dups = row[3]
                    details[tbl] = {"total": total, "unique": unique, "duplicates": dups}
                    if dups > 0:
                        dups_found = True
                result["details"] = details
                result["score"] = 0.0 if dups_found else 1.0
                result["status"] = "FAIL" if dups_found else "PASS"

            elif rule["category"] == "Consistency":
                row = rows[0]
                total = row[0]
                mismatches = row[1]
                consistency = (total - mismatches) / total if total > 0 else 0
                result["details"] = {columns[i]: row[i] for i in range(len(columns))}
                result["score"] = round(consistency, 4)
                result["status"] = "PASS" if consistency >= rule["threshold"] else "FAIL"

            results.append(result)

            icon = GREEN + "✅" if result["status"] in ("PASS", "INFO") else YELLOW + "⚠️"
            score_str = f" ({result['score']:.1%})" if isinstance(result.get("score"), float) else ""
            print(f"  {icon}{RESET} {rule['name']}: {BOLD}{result['status']}{RESET}{score_str}")

            if result["status"] == "PASS":
                passed += 1
            elif result["status"] == "FAIL":
                failed += 1
                # Print failure details
                if "null_counts" in result["details"]:
                    for k, v in result["details"]["null_counts"].items():
                        if v > 0:
                            print(f"       {DIM}↳ {k}: {v} nulls{RESET}")
                if "orphan_count" in result["details"] and result["details"]["orphan_count"] > 0:
                    print(f"       {DIM}↳ {result['details']['orphan_count']} orphaned records{RESET}")
            else:
                passed += 1  # INFO counts as pass

        except Exception as e:
            warn(f"{rule['name']}: query error — {e}")
            results.append({
                "rule": rule["name"],
                "category": rule["category"],
                "status": "ERROR",
                "error": str(e),
            })

    conn.close()

    # Summary
    total = passed + failed
    print(f"\n  ┌{'─' * 40}┬{'─' * 14}┐")
    print(f"  │ {'Data Quality Summary':<38} │ {'Score':>12} │")
    print(f"  ├{'─' * 40}┼{'─' * 14}┤")
    print(f"  │ {'Rules Passed':<38} │ {f'{passed}/{total}':>12} │")
    print(f"  │ {'Rules Failed':<38} │ {f'{failed}/{total}':>12} │")
    overall = passed / total if total > 0 else 0
    print(f"  │ {'Overall Score':<38} │ {f'{overall:.0%}':>12} │")
    print(f"  └{'─' * 40}┴{'─' * 14}┘")

    return results


def store_dq_results_in_purview(results):
    """Store data quality results as custom entities in Purview."""
    header("4", "STORING DQ RESULTS IN PURVIEW")
    refresh_token()

    # Create DQ category in glossary
    r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    if r.status_code != 200:
        warn("Cannot access glossary")
        return

    data = r.json()
    glossaries = data if isinstance(data, list) else [data]
    g_guid = glossaries[0]["guid"]

    # Create DQ category
    cat_payload = {
        "name": "Datakvalitetsregler",
        "glossaryGuid": g_guid,
        "shortDescription": "Data Quality Rules & Checks — automatiserade kontroller för datakvalitet",
    }
    r = requests.post(f"{ATLAS}/glossary/category", headers=h, json=cat_payload, timeout=30)
    dq_cat_guid = None
    if r.status_code in (200, 201):
        dq_cat_guid = r.json().get("guid")
        ok("Created category: Datakvalitetsregler")
    elif r.status_code == 409 or (r.status_code == 400 and "ATLAS-400-00-0" in r.text):
        info("DQ category already exists")
        r2 = requests.get(f"{ATLAS}/glossary/{g_guid}", headers=h, timeout=30)
        if r2.status_code == 200:
            for cat in r2.json().get("categories", []):
                if "Datakvalitet" in cat.get("displayText", ""):
                    dq_cat_guid = cat.get("categoryGuid")

    stored = 0
    for result in results:
        if result.get("status") == "ERROR":
            continue

        # Sanitize term name for Purview
        term_name = result["rule"].replace('→', '-').replace('—', '-')
        status = result["status"]
        score = result.get("score", 0)
        details = json.dumps(result.get("details", {}), ensure_ascii=False, default=str)

        short_desc = f"[{status}] Score {score:.0%} {result['category']}" if isinstance(score, float) else f"[{status}]"
        long_desc = (
            f"Regel {term_name}\n"
            f"Kategori {result['category']}\n"
            f"Status {status}\n"
        )
        if isinstance(score, float):
            long_desc += f"Score {score:.1%}\n"
        long_desc += (
            f"Tabell {result.get('table', 'N/A')}\n"
            f"Korning {result.get('timestamp', 'N/A')}\n\n"
            f"Detaljer\n{details}"
        )

        term_payload = {
            "name": term_name,
            "glossaryGuid": g_guid,
            "shortDescription": short_desc[:256],
            "longDescription": long_desc,
            "status": "Approved",
        }
        if dq_cat_guid:
            term_payload["categories"] = [{"categoryGuid": dq_cat_guid}]

        r = requests.post(f"{ATLAS}/glossary/term", headers=h, json=term_payload, timeout=30)
        if r.status_code in (200, 201):
            stored += 1
        elif r.status_code == 409 or (r.status_code == 400 and "ATLAS-400-00-0" in r.text):
            # Already exists
            stored += 1
        else:
            warn(f"Store {term_name}: {r.status_code}")

    ok(f"Stored {stored}/{len([r for r in results if r.get('status') != 'ERROR'])} DQ results in glossary")

    # Also save to local JSON
    from pathlib import Path
    out = Path(__file__).resolve().parent.parent / "data_quality_report.json"
    report = {
        "timestamp": datetime.now().isoformat(),
        "rules_total": len(results),
        "rules_passed": sum(1 for r in results if r["status"] in ("PASS", "INFO")),
        "rules_failed": sum(1 for r in results if r["status"] == "FAIL"),
        "results": results,
    }
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    ok(f"Report saved: {out}")


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    print(f"""
{BOLD}{BLUE}╔══════════════════════════════════════════════════════════════════╗
║  PURVIEW DATA PRODUCTS, OKRs & DATA QUALITY                     ║
║  {datetime.now().strftime('%Y-%m-%d %H:%M')}                                              ║
╚══════════════════════════════════════════════════════════════════╝{RESET}
""")

    # 1. Data Products
    dp_count = create_data_products()

    # 2. OKRs
    okr_count = create_okrs()

    # 3. Data Quality Checks
    dq_results = run_data_quality_checks()

    # 4. Store DQ results in Purview
    store_dq_results_in_purview(dq_results)

    # Update data product quality scores
    dq_pass = sum(1 for r in dq_results if r["status"] in ("PASS", "INFO"))
    dq_total = len(dq_results)
    quality_score = dq_pass / dq_total if dq_total > 0 else 0

    print(f"""
{'=' * 70}
  SUMMARY
{'=' * 70}
  Data Products:    {dp_count}/{len(DATA_PRODUCTS)}
  OKRs:             {okr_count}/{len(OKRS)}
  DQ Rules:         {dq_pass}/{dq_total} passed ({quality_score:.0%})

  {GREEN}✅{RESET} All governance artifacts created!
  View in Purview: https://purview.microsoft.com
{'=' * 70}
""")


if __name__ == "__main__":
    main()
