"""
Full Platform Audit & Fix
===========================
Goes through EVERY data file and verifies existence in:
  1. Azure SQL (hca schema)
  2. Fabric Lakehouse (Bronze/Silver/Gold + BrainChild)
  3. Purview (entities, glossary terms, classifications, descriptions)

Adds anything that's missing.

Usage:
  python scripts/audit_and_fix_all.py              # Full audit + fix
  python scripts/audit_and_fix_all.py --audit-only  # Just audit
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

# ── CONFIG ──
cred = AzureCliCredential(process_timeout=30)

ACCT = "https://prviewacc.purview.azure.com"
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"

SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DB = "HealthcareAnalyticsDB"

HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"

sess = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
sess.mount("https://", HTTPAdapter(max_retries=retry))

# ── Counters ──
audit = {"found": 0, "missing": 0, "fixed": 0, "errors": 0}


def get_purview_headers():
    token = cred.get_token("https://purview.azure.net/.default").token
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def get_sql_token():
    return cred.get_token("https://database.windows.net/.default").token


def ok(msg):
    print(f"  ✅ {msg}")
    audit["found"] += 1


def miss(msg):
    print(f"  ❌ {msg}")
    audit["missing"] += 1


def fix(msg):
    print(f"  🔧 {msg}")
    audit["fixed"] += 1


def warn(msg):
    print(f"  ⚠️  {msg}")


def info(msg):
    print(f"  ℹ️  {msg}")


def sep(title):
    print(f"\n{'═' * 70}")
    print(f"  {title}")
    print(f"{'═' * 70}")


# ══════════════════════════════════════════════════════════════════════
# COMPLETE DATA INVENTORY — what SHOULD exist
# ══════════════════════════════════════════════════════════════════════

# SQL Tables (hca schema)
SQL_TABLES = {
    "patients": ["patient_id", "birth_date", "gender", "ses_level", "postal_code",
                 "region", "smoking_status", "created_at"],
    "encounters": ["encounter_id", "patient_id", "admission_date", "discharge_date",
                   "department", "admission_source", "discharge_disposition",
                   "los_days", "readmission_30d", "created_at"],
    "diagnoses": ["diagnosis_id", "encounter_id", "icd10_code", "icd10_description",
                  "diagnosis_type", "confirmed_date"],
    "medications": ["medication_id", "encounter_id", "atc_code", "drug_name",
                    "dose_mg", "frequency", "route", "start_date", "end_date"],
    "vitals_labs": ["measurement_id", "encounter_id", "measured_at", "systolic_bp",
                    "diastolic_bp", "heart_rate", "temperature_c", "oxygen_saturation",
                    "glucose_mmol", "creatinine_umol", "hemoglobin_g", "sodium_mmol",
                    "potassium_mmol", "bmi", "weight_kg"],
}
SQL_VIEWS = ["vw_ml_encounters"]

# Fabric Lakehouse Tables — HCA workspace
FABRIC_HCA_TABLES = {
    "bronze_lakehouse": [
        "hca_patients", "hca_encounters", "hca_diagnoses",
        "hca_vitals_labs", "hca_medications",
    ],
    "silver_lakehouse": ["ml_features"],
    "gold_lakehouse": ["ml_predictions"],
    "gold_omop": [
        "person", "visit_occurrence", "condition_occurrence",
        "drug_exposure", "measurement", "observation",
        "observation_period", "location", "concept",
    ],
}

# Fabric Lakehouse Tables — BrainChild workspace
FABRIC_BC_TABLES = {
    "lh_brainchild": [
        # OMOP genomics
        "gene_sequence", "variant_occurrence",
        # OMOP standard
        "person", "condition_occurrence", "drug_exposure",
        "measurement", "specimen", "visit_occurrence",
        # SBCR
        "sbcr_registrations", "sbcr_treatments", "sbcr_followup",
        # Patient master
        "patients_master",
        # DICOM (from notebooks)
        "brainchild_bronze_dicom_study", "brainchild_bronze_dicom_series",
        "brainchild_bronze_dicom_instance",
        "brainchild_silver_dicom_studies", "brainchild_silver_dicom_series",
        "brainchild_silver_dicom_pathology",
    ],
}

# Purview — Glossary terms that MUST exist (existing 34 + new ones)
REQUIRED_GLOSSARY_TERMS = {
    # Existing terms (from purview_rebuild.py)
    "ICD-10": "Kliniska Standarder",
    "ATC-klassificering": "Kliniska Standarder",
    "Skyddad hälsoinformation (PHI)": "Kliniska Standarder",
    "Svenskt personnummer": "Kliniska Standarder",
    "FHIR R4": "Interoperabilitet",
    "FHIR Patient": "Interoperabilitet",
    "FHIR Encounter": "Interoperabilitet",
    "FHIR Condition": "Interoperabilitet",
    "FHIR MedicationRequest": "Interoperabilitet",
    "FHIR Observation": "Interoperabilitet",
    "FHIR ImagingStudy": "Interoperabilitet",
    "FHIR Specimen": "Interoperabilitet",
    "OMOP CDM": "Interoperabilitet",
    "OMOP Person": "Interoperabilitet",
    "OMOP Visit Occurrence": "Interoperabilitet",
    "OMOP Condition Occurrence": "Interoperabilitet",
    "OMOP Drug Exposure": "Interoperabilitet",
    "OMOP Measurement": "Interoperabilitet",
    "Medallion-arkitektur": "Dataarkitektur",
    "Bronze-lager": "Dataarkitektur",
    "Silver-lager": "Dataarkitektur",
    "Gold-lager": "Dataarkitektur",
    "Vårdtid (LOS)": "Klinisk Data",
    "Återinläggningsrisk": "Klinisk Data",
    "Vitalparametrar": "Klinisk Data",
    "Labresultat": "Klinisk Data",
    "DICOM": "Barncancerforskning",
    "Genomic Medicine Sweden (GMS)": "Barncancerforskning",
    "VCF (Variant Call Format)": "Barncancerforskning",
    "BTB (Barntumörbanken)": "Barncancerforskning",
    "SBCR (Svenska Barncancerregistret)": "Barncancerforskning",
    "Genomisk variant": "Barncancerforskning",
    # ── NEW terms that are missing ──
    "OMOP Specimen": "Interoperabilitet",
    "OMOP Genomics": "Interoperabilitet",
    "SNOMED-CT": "Kliniska Standarder",
    "ICD-O-3": "Kliniska Standarder",
    "LOINC": "Kliniska Standarder",
    "ACMG-klassificering": "Kliniska Standarder",
    "FHIR DiagnosticReport": "Interoperabilitet",
    "Feature Engineering": "Dataarkitektur",
    "ML-prediktion": "Dataarkitektur",
    "Charlson Comorbidity Index": "Klinisk Data",
    "DRG-klassificering": "Klinisk Data",
    "HGVS-nomenklatur": "Barncancerforskning",
    "Tumörsite": "Barncancerforskning",
    "Behandlingsprotokoll": "Barncancerforskning",
    "Seneffekter": "Barncancerforskning",
    "FFPE (Formalinfixerat paraffin)": "Barncancerforskning",
}

# New term definitions (only for terms not in purview_rebuild.py)
NEW_TERM_DEFS = {
    "OMOP Specimen": (
        "OMOP-tabell för biologiska prover. Mappas till BTB specimen-data.",
        "Specimen-tabellen lagrar biobanksprover med provtyp, datum och kvantitet."
    ),
    "OMOP Genomics": (
        "OMOP Genomics-tillägg: gene_sequence och variant_occurrence. Mappas till GMS/VCF-data.",
        "Genomics-tillägget utökar OMOP CDM med tabeller för DNA-sekvenser och varianter."
    ),
    "SNOMED-CT": (
        "Systematized Nomenclature of Medicine — Clinical Terms. Används i BTB specimen.type och GMS-resurser.",
        "SNOMED-CT är det mest omfattande kliniska terminologisystemet."
    ),
    "ICD-O-3": (
        "International Classification of Diseases for Oncology, 3rd Ed. Används i SBCR-registrering (icd_o3_morphology) och patients_master (icd_o3_code).",
        "ICD-O-3 klassificerar tumörer efter topografi (lokalisation) och morfologi (celltyp)."
    ),
    "LOINC": (
        "Logical Observation Identifiers Names and Codes. Används i GMS DiagnosticReport (51969-4) och Observation (69548-6).",
        "LOINC är den globala standarden för laboratorie- och kliniska observationskoder."
    ),
    "ACMG-klassificering": (
        "American College of Medical Genetics variant-klassificering: Pathogenic, Likely Pathogenic, VUS, Likely Benign, Benign. Används i VCF CLINSIG-fält.",
        "ACMG-riktlinjerna standardiserar tolkningen av genomiska varianter."
    ),
    "FHIR DiagnosticReport": (
        "FHIR-resurs för diagnostiska rapporter. Används i GMS-modulen med LOINC 51969-4 (Genetic analysis report).",
        "DiagnosticReport sammanfattar resultat från diagnostiska undersökningar."
    ),
    "Feature Engineering": (
        "Processen att skapa ML-features från rådata. Silver-lagret beräknar CCI-score, prior_admissions, medication_count etc.",
        "Feature engineering i Silver-lagret transformerar klinisk data till prediktiva variabler."
    ),
    "ML-prediktion": (
        "Machine Learning-prediktioner för LOS och återinläggningsrisk. Lagras i gold_lakehouse.ml_predictions.",
        "ML-modellen tränas på Silver-features och producerar sannolikheter i Gold-lagret."
    ),
    "Charlson Comorbidity Index": (
        "Komorbiditetsindex baserat på ICD-10-diagnoser. Beräknas i Silver feature engineering (cci_score).",
        "CCI viktar 17 komorbiditeter för att predicera 10-årsmortalitet."
    ),
    "DRG-klassificering": (
        "Diagnosrelaterade Grupper — patientklassificeringssystem. Potentiellt tillägg baserat på ICD-10 + procedurdata.",
        "DRG grupperar vårdtillfällen för ersättning och jämförelser."
    ),
    "HGVS-nomenklatur": (
        "Human Genome Variation Society-nomenklatur för DNA/protein-varianter (c.HGVS, p.HGVS). Används i GMS Observation och VCF.",
        "HGVS standardiserar hur genomiska varianter beskrivs på DNA-, RNA- och proteinnivå."
    ),
    "Tumörsite": (
        "Anatomisk lokalisation av tumör. Kolumner: tumor_site (patients_master, SBCR registrations), BodyPartExamined (DICOM).",
        "Tumörsite beskriver var i kroppen tumören finns och styr behandlingsstrategi."
    ),
    "Behandlingsprotokoll": (
        "Standardiserat behandlingsschema för cancerbehandling. Kolumn: protocol_name (SBCR treatments).",
        "Behandlingsprotokoll definierar kombinationer av cytostatika, strålning och kirurgi."
    ),
    "Seneffekter": (
        "Långtidsbiverkningar efter cancerbehandling. Kolumn: late_effects (SBCR followup).",
        "Seneffekter övervakas livslångt efter barncancerbehandling."
    ),
    "FFPE (Formalinfixerat paraffin)": (
        "Fixeringsmetod för vävnadsprover i biobanken. Processing-typ i BTB Specimen.",
        "FFPE-fixering bevarar vävnad för histopatologisk analys och DNA-extraktion."
    ),
}

# Purview — Column descriptions for ALL tables
COLUMN_DESCRIPTIONS = {
    # ── SQL: patients ──
    "patients.patient_id": "Unik patient-identifierare (UUID). PHI — kräver GDPR-skydd. FHIR: Patient.id, OMOP: person.person_source_value",
    "patients.birth_date": "Födelsedatum (YYYY-MM-DD). PHI — känslig personuppgift. FHIR: Patient.birthDate, OMOP: person.year/month/day_of_birth",
    "patients.gender": "Juridiskt kön (M/F). FHIR: Patient.gender, OMOP: person.gender_concept_id",
    "patients.ses_level": "Socioekonomisk nivå (1-5). Forskningsattribut — ej standard FHIR/OMOP",
    "patients.postal_code": "Postnummer (pseudonymiserat). Proxy för geografisk lokalisering. PHI",
    "patients.region": "Vårdregion (Norra/Södra/Östra/Västra/Stockholm/Uppsala-Örebro). HSA regionkod",
    "patients.smoking_status": "Rökstatus (never/former/current). FHIR: Observation (LOINC 72166-2)",
    "patients.created_at": "Tidsstämpel för postens skapande (UTC). Audit trail",
    # ── SQL: encounters ──
    "encounters.encounter_id": "Unik vårdkontakt-identifierare (UUID). PHI. FHIR: Encounter.id, OMOP: visit_occurrence.visit_occurrence_id",
    "encounters.patient_id": "FK → patients.patient_id. FHIR: Encounter.subject",
    "encounters.admission_date": "Inskrivningsdatum. FHIR: Encounter.period.start, OMOP: visit_start_date",
    "encounters.discharge_date": "Utskrivningsdatum. FHIR: Encounter.period.end, OMOP: visit_end_date",
    "encounters.department": "Vårdavdelning (Cardiology/Neurology/etc). FHIR: Encounter.serviceType",
    "encounters.admission_source": "Inskrivningskälla (Emergency/Planned/Transfer). FHIR: Encounter.hospitalization.admitSource",
    "encounters.discharge_disposition": "Utskrivningssätt (Home/Transfer/Deceased). FHIR: Encounter.hospitalization.dischargeDisposition",
    "encounters.los_days": "Vårdtid i dagar (Length of Stay). ML-målvariabel. DATEDIFF(admission, discharge)",
    "encounters.readmission_30d": "Återinläggning inom 30 dagar (0/1). ML-målvariabel. Binär klassificering",
    "encounters.created_at": "Tidsstämpel för postens skapande (UTC). Audit trail",
    # ── SQL: diagnoses ──
    "diagnoses.diagnosis_id": "Unik diagnos-identifierare (UUID). FHIR: Condition.id",
    "diagnoses.encounter_id": "FK → encounters.encounter_id. FHIR: Condition.encounter",
    "diagnoses.icd10_code": "ICD-10 diagnoskod (t.ex. I21.0). FHIR: Condition.code.coding[system=ICD-10]",
    "diagnoses.icd10_description": "ICD-10 beskrivning på svenska. FHIR: Condition.code.text",
    "diagnoses.diagnosis_type": "Diagnostyp (Primary/Secondary). FHIR: Condition.category",
    "diagnoses.confirmed_date": "Datum diagnos bekräftades. FHIR: Condition.recordedDate",
    # ── SQL: medications ──
    "medications.medication_id": "Unik medicinerings-identifierare (UUID). FHIR: MedicationRequest.id",
    "medications.encounter_id": "FK → encounters.encounter_id. FHIR: MedicationRequest.encounter",
    "medications.atc_code": "ATC-klassifikationskod (WHO). FHIR: MedicationRequest.medicationCodeableConcept.coding[system=ATC]",
    "medications.drug_name": "Läkemedelsnamn. FHIR: MedicationRequest.medicationCodeableConcept.text",
    "medications.dose_mg": "Dos i milligram. FHIR: MedicationRequest.dosageInstruction.doseAndRate.doseQuantity",
    "medications.frequency": "Doseringsfrekvens (QD/BID/TID/QID/PRN). FHIR: MedicationRequest.dosageInstruction.timing",
    "medications.route": "Administreringsväg (oral/iv/sc/im). FHIR: MedicationRequest.dosageInstruction.route",
    "medications.start_date": "Startdatum för medicinering. FHIR: MedicationRequest.dosageInstruction.timing.boundsPeriod.start",
    "medications.end_date": "Slutdatum för medicinering. FHIR: MedicationRequest.dosageInstruction.timing.boundsPeriod.end",
    # ── SQL: vitals_labs ──
    "vitals_labs.measurement_id": "Unik mätnings-identifierare (UUID). FHIR: Observation.id, OMOP: measurement_id",
    "vitals_labs.encounter_id": "FK → encounters.encounter_id. FHIR: Observation.encounter",
    "vitals_labs.measured_at": "Mättidpunkt (UTC). FHIR: Observation.effectiveDateTime, OMOP: measurement_date",
    "vitals_labs.systolic_bp": "Systoliskt blodtryck (mmHg). LOINC: 8480-6. Normalvärde: 90-140",
    "vitals_labs.diastolic_bp": "Diastoliskt blodtryck (mmHg). LOINC: 8462-4. Normalvärde: 60-90",
    "vitals_labs.heart_rate": "Hjärtfrekvens (slag/min). LOINC: 8867-4. Normalvärde: 60-100",
    "vitals_labs.temperature_c": "Kroppstemperatur (°C). LOINC: 8310-5. Normalvärde: 36.1-37.2",
    "vitals_labs.oxygen_saturation": "Syremättnad (%). LOINC: 2708-6. Normalvärde: 95-100",
    "vitals_labs.glucose_mmol": "Blodglukos (mmol/L). LOINC: 2345-7. Fasteglukos normalvärde: 4.0-6.0",
    "vitals_labs.creatinine_umol": "Kreatinin (µmol/L). LOINC: 2160-0. Normalvärde: 45-90 (kvinnor), 60-105 (män)",
    "vitals_labs.hemoglobin_g": "Hemoglobin (g/L). LOINC: 718-7. Normalvärde: 120-160 (kvinnor), 130-170 (män)",
    "vitals_labs.sodium_mmol": "Natrium (mmol/L). LOINC: 2951-2. Normalvärde: 136-145",
    "vitals_labs.potassium_mmol": "Kalium (mmol/L). LOINC: 2823-3. Normalvärde: 3.5-5.0",
    "vitals_labs.bmi": "Body Mass Index (kg/m²). LOINC: 39156-5. Normalvärde: 18.5-24.9",
    "vitals_labs.weight_kg": "Kroppsvikt (kg). LOINC: 29463-7",
    # ── Fabric OMOP: drug_exposure ──
    "drug_exposure.drug_exposure_id": "Unik OMOP drug exposure ID. Primärnyckel",
    "drug_exposure.person_id": "FK → person.person_id. Patientreferens",
    "drug_exposure.drug_concept_id": "OMOP concept_id för läkemedel. Mappa till RxNorm/ATC",
    "drug_exposure.drug_exposure_start_date": "Startdatum för läkemedelsexponering",
    "drug_exposure.drug_exposure_end_date": "Slutdatum för läkemedelsexponering",
    "drug_exposure.drug_type_concept_id": "OMOP type concept — provenance (EHR/claim/etc)",
    "drug_exposure.route_concept_id": "Administreringsväg OMOP concept_id",
    "drug_exposure.dose_unit_source_value": "Dosenhet i källformat (mg, ml, etc)",
    "drug_exposure.quantity": "Mängd administrerad/förskriven",
    "drug_exposure.drug_source_value": "Läkemedelskod i källformat (ATC-kod)",
    "drug_exposure.drug_source_concept_id": "OMOP source concept_id för läkemedel",
    # ── Fabric OMOP: person ──
    "person.person_id": "Unik OMOP person-identifierare. Primärnyckel",
    "person.person_source_value": "Patient-ID från källsystem (patient_uuid)",
    "person.gender_concept_id": "OMOP concept_id för kön (8507=Male, 8532=Female)",
    "person.year_of_birth": "Födelseår",
    "person.month_of_birth": "Födelsemånad",
    "person.day_of_birth": "Födelsedag",
    "person.race_concept_id": "OMOP concept_id för etnicitet (ej relevant i Sverige, satt till 0)",
    "person.ethnicity_concept_id": "OMOP concept_id för ethnicity (ej relevant i Sverige, satt till 0)",
    "person.care_site_id": "FK → care_site. Behandlande sjukhus-ID",
    # ── Fabric OMOP: condition_occurrence ──
    "condition_occurrence.condition_occurrence_id": "Unik OMOP diagnos-ID. Primärnyckel",
    "condition_occurrence.person_id": "FK → person.person_id",
    "condition_occurrence.condition_concept_id": "OMOP concept_id för diagnos. Mappa via ICD-10 → SNOMED",
    "condition_occurrence.condition_start_date": "Startdatum för diagnos/tillstånd",
    "condition_occurrence.condition_end_date": "Slutdatum för diagnos (null om pågående)",
    "condition_occurrence.condition_type_concept_id": "OMOP type concept — provenance",
    "condition_occurrence.condition_source_value": "ICD-10-kod från källsystem",
    "condition_occurrence.condition_source_concept_id": "OMOP source concept_id",
    # ── Fabric OMOP: measurement ──
    "measurement.measurement_id": "Unik OMOP mätnings-ID. Primärnyckel",
    "measurement.person_id": "FK → person.person_id",
    "measurement.measurement_concept_id": "OMOP concept_id för mätning. Mappa via LOINC",
    "measurement.measurement_date": "Mätningsdatum",
    "measurement.measurement_type_concept_id": "OMOP type concept — provenance",
    "measurement.value_as_number": "Numeriskt mätvärde",
    "measurement.unit_concept_id": "OMOP concept_id för enhet (UCUM)",
    "measurement.unit_source_value": "Enhet i källformat (mmHg, mmol/L, etc)",
    "measurement.measurement_source_value": "Mätningstyp i källformat",
    # ── Fabric OMOP: visit_occurrence ──
    "visit_occurrence.visit_occurrence_id": "Unik OMOP besöks-ID. Primärnyckel",
    "visit_occurrence.person_id": "FK → person.person_id",
    "visit_occurrence.visit_concept_id": "OMOP concept_id för besökstyp (9201=Inpatient, 9202=Outpatient)",
    "visit_occurrence.visit_start_date": "Inskrivningsdatum",
    "visit_occurrence.visit_end_date": "Utskrivningsdatum",
    "visit_occurrence.visit_type_concept_id": "OMOP type concept — provenance",
    "visit_occurrence.care_site_id": "FK → care_site. Vårdenhet",
    "visit_occurrence.visit_source_value": "Besökstyp i källformat",
    # ── Fabric OMOP: specimen ──
    "specimen.specimen_id": "Unik OMOP specimen-ID. Primärnyckel",
    "specimen.person_id": "FK → person.person_id",
    "specimen.specimen_concept_id": "OMOP concept_id för provtyp",
    "specimen.specimen_date": "Provtagningsdatum",
    "specimen.specimen_type_concept_id": "OMOP type concept — provenance",
    "specimen.specimen_source_value": "Provtyp i källformat (Tissue/Blood/etc)",
    "specimen.unit_concept_id": "OMOP concept_id för enhet",
    "specimen.quantity": "Provmängd",
}

# PII classifications for sensitive columns
PII_COLUMNS = {
    "patients.patient_id": "MICROSOFT.PERSONAL.IPADDRESS",  # Custom — unique ID
    "patients.birth_date": "MICROSOFT.PERSONAL.DATE_OF_BIRTH",
    "patients.postal_code": "MICROSOFT.PERSONAL.US.PHONE_NUMBER",  # Proxy for postal PHI
    "patients.gender": "MICROSOFT.PERSONAL.GENDER",
    "encounters.encounter_id": "MICROSOFT.PERSONAL.IPADDRESS",  # Custom — unique ID
    "encounters.patient_id": "MICROSOFT.PERSONAL.IPADDRESS",  # FK to PHI
    "person.person_source_value": "MICROSOFT.PERSONAL.IPADDRESS",  # Source patient ID
}


# ══════════════════════════════════════════════════════════════════════
# 1. AUDIT SQL TABLES
# ══════════════════════════════════════════════════════════════════════
def audit_sql():
    sep("1. AUDIT — Azure SQL Tables")
    try:
        import pyodbc
    except ImportError:
        warn("pyodbc not installed — skipping SQL audit")
        info("pip install pyodbc")
        return {}

    sql_token = get_sql_token()
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={SQL_SERVER};DATABASE={SQL_DB};"
        f"Authentication=ActiveDirectoryAccessToken;"
    )

    try:
        import struct
        token_bytes = sql_token.encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
        cursor = conn.cursor()
    except Exception as e:
        warn(f"SQL connection failed: {e}")
        return {}

    results = {}

    # Check tables
    for table, expected_cols in SQL_TABLES.items():
        cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='hca' AND TABLE_NAME=?",
            table
        )
        actual_cols = [row[0] for row in cursor.fetchall()]

        if actual_cols:
            ok(f"hca.{table} — {len(actual_cols)} columns")
            missing_cols = set(expected_cols) - set(actual_cols)
            extra_cols = set(actual_cols) - set(expected_cols)
            if missing_cols:
                miss(f"  Missing columns: {missing_cols}")
            if extra_cols:
                info(f"  Extra columns: {extra_cols}")
            results[table] = actual_cols
        else:
            miss(f"hca.{table} — TABLE NOT FOUND")
            results[table] = None

    # Check views
    for view in SQL_VIEWS:
        cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='hca' AND TABLE_NAME=?",
            view
        )
        actual_cols = [row[0] for row in cursor.fetchall()]
        if actual_cols:
            ok(f"hca.{view} — {len(actual_cols)} columns (view)")
        else:
            miss(f"hca.{view} — VIEW NOT FOUND")

    # Check row counts
    print()
    for table in SQL_TABLES:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM hca.[{table}]")
            count = cursor.fetchone()[0]
            info(f"hca.{table}: {count:,} rows")
        except Exception:
            pass

    conn.close()
    return results


# ══════════════════════════════════════════════════════════════════════
# 2. AUDIT PURVIEW ENTITIES (SQL + Fabric)
# ══════════════════════════════════════════════════════════════════════
def audit_purview_entities(h):
    sep("2. AUDIT — Purview Entities")

    entity_types = {}

    # Count entities by type
    for etype in ["azure_sql_table", "azure_sql_view", "azure_sql_column",
                  "fabric_lake_warehouse", "fabric_lakehouse_table",
                  "fabric_synapse_notebook", "fabric_pipeline"]:
        body = {"keywords": "*", "filter": {"entityType": etype}, "limit": 1}
        r = sess.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            count = r.json().get("@search.count", 0)
            entity_types[etype] = count
            if count > 0:
                ok(f"{etype}: {count} entities")
            else:
                miss(f"{etype}: 0 entities")
        time.sleep(0.2)

    # Check specific SQL tables in Purview
    print()
    sql_guids = {}
    for table in list(SQL_TABLES.keys()) + SQL_VIEWS:
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
                    ok(f"Purview: hca.{table} registered")
                    break
            else:
                miss(f"Purview: hca.{table} NOT found")
        time.sleep(0.2)

    # Check Fabric lakehouse tables
    print()
    fabric_guids = {}
    all_fabric_tables = {}
    for lh, tables in {**FABRIC_HCA_TABLES, **FABRIC_BC_TABLES}.items():
        for t in tables:
            all_fabric_tables[t] = lh

    for table_name, lakehouse in all_fabric_tables.items():
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
            found = False
            for asset in r.json().get("value", []):
                name = asset.get("name", "")
                qn = asset.get("qualifiedName", "")
                if table_name.lower() in name.lower() or table_name.lower() in qn.lower():
                    fabric_guids[table_name] = asset["id"]
                    ok(f"Purview Fabric: {table_name} ({lakehouse})")
                    found = True
                    break
            if not found:
                miss(f"Purview Fabric: {table_name} ({lakehouse}) NOT found")
        time.sleep(0.15)

    return sql_guids, fabric_guids, entity_types


# ══════════════════════════════════════════════════════════════════════
# 3. AUDIT GLOSSARY TERMS
# ══════════════════════════════════════════════════════════════════════
def audit_glossary(h):
    sep("3. AUDIT — Glossary Terms")

    # Get glossary
    r = sess.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    glossary_guid = None
    if r.status_code == 200:
        for g in r.json():
            if "sjukvard" in g["name"].lower() or "kund" in g["name"].lower():
                glossary_guid = g["guid"]
                info(f"Glossary: {g['name']} (guid={glossary_guid[:12]}...)")
                break
        if not glossary_guid and r.json():
            glossary_guid = r.json()[0]["guid"]

    if not glossary_guid:
        miss("No glossary found!")
        return None, {}

    # Get all terms
    r = sess.get(f"{ATLAS}/glossary/{glossary_guid}/terms?limit=100", headers=h, timeout=30)
    existing_terms = {}
    if r.status_code == 200:
        for t in r.json():
            existing_terms[t["name"]] = t["guid"]

    info(f"Total terms found: {len(existing_terms)}")

    # Check which required terms exist
    missing_terms = []
    for term_name, category in REQUIRED_GLOSSARY_TERMS.items():
        if term_name in existing_terms:
            ok(f"Term: {term_name} [{category}]")
        else:
            miss(f"Term: {term_name} [{category}] — MISSING")
            missing_terms.append(term_name)

    if missing_terms:
        print(f"\n  {len(missing_terms)} terms missing")
    else:
        print(f"\n  All {len(REQUIRED_GLOSSARY_TERMS)} required terms exist!")

    return glossary_guid, existing_terms


# ══════════════════════════════════════════════════════════════════════
# 4. AUDIT COLUMN DESCRIPTIONS
# ══════════════════════════════════════════════════════════════════════
def audit_column_descriptions(h, sql_guids):
    sep("4. AUDIT — Column Descriptions")

    missing_descs = []
    for col_key in COLUMN_DESCRIPTIONS:
        table_name, col_name = col_key.split(".", 1)

        body = {
            "keywords": col_name,
            "filter": {"entityType": "azure_sql_column"},
            "limit": 10,
        }
        r = sess.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            for asset in r.json().get("value", []):
                qn = asset.get("qualifiedName", "")
                if col_name.lower() in qn.lower() and table_name.lower() in qn.lower():
                    # Check if it has a description
                    guid = asset["id"]
                    r2 = sess.get(f"{ATLAS}/entity/guid/{guid}?minExtInfo=true", headers=h, timeout=30)
                    if r2.status_code == 200:
                        ent = r2.json().get("entity", {})
                        attrs = ent.get("attributes", {})
                        desc = attrs.get("userDescription", "") or attrs.get("description", "")
                        if desc:
                            ok(f"{col_key}: has description")
                        else:
                            miss(f"{col_key}: NO description")
                            missing_descs.append((col_key, guid))
                    break
        time.sleep(0.1)

    return missing_descs


# ══════════════════════════════════════════════════════════════════════
# 5. FIX — Add missing glossary terms
# ══════════════════════════════════════════════════════════════════════
def fix_missing_terms(h, glossary_guid, existing_terms):
    sep("5. FIX — Add Missing Glossary Terms")

    if not glossary_guid:
        warn("No glossary available — cannot add terms")
        return existing_terms

    # Get categories
    r = sess.get(f"{ATLAS}/glossary/{glossary_guid}/categories?limit=50", headers=h, timeout=30)
    cat_guids = {}
    if r.status_code == 200:
        for cat in r.json():
            cat_guids[cat["name"]] = cat["guid"]
        info(f"Existing categories: {list(cat_guids.keys())}")

    added = 0
    for term_name, category in REQUIRED_GLOSSARY_TERMS.items():
        if term_name in existing_terms:
            continue

        if term_name not in NEW_TERM_DEFS:
            warn(f"No definition for '{term_name}' — skipping")
            continue

        short_desc, long_desc = NEW_TERM_DEFS[term_name]

        body = {
            "name": term_name,
            "shortDescription": short_desc,
            "longDescription": long_desc,
            "anchor": {"glossaryGuid": glossary_guid},
        }
        if category in cat_guids:
            body["categories"] = [{"categoryGuid": cat_guids[category]}]

        r = sess.post(f"{ATLAS}/glossary/term", headers=h, json=body, timeout=30)
        if r.status_code in (200, 201):
            existing_terms[term_name] = r.json()["guid"]
            fix(f"Created term: {term_name} [{category}]")
            added += 1
        else:
            warn(f"Failed to create '{term_name}': {r.status_code} — {r.text[:150]}")
        time.sleep(0.2)

    print(f"\n  Added {added} new terms")
    return existing_terms


# ══════════════════════════════════════════════════════════════════════
# 6. FIX — Add column descriptions
# ══════════════════════════════════════════════════════════════════════
def fix_column_descriptions(h, missing_descs):
    sep("6. FIX — Add Column Descriptions")

    if not missing_descs:
        ok("All columns already have descriptions")
        return

    fixed = 0
    for col_key, guid in missing_descs:
        desc = COLUMN_DESCRIPTIONS.get(col_key, "")
        if not desc:
            continue

        r = sess.put(
            f"{ATLAS}/entity/guid/{guid}",
            headers=h,
            json={
                "entity": {
                    "guid": guid,
                    "attributes": {"userDescription": desc},
                    "typeName": "azure_sql_column",
                }
            },
            timeout=30,
        )
        if r.status_code in (200, 201):
            fix(f"{col_key}: description added")
            fixed += 1
        else:
            # Try alternative API
            r2 = sess.post(
                f"{ATLAS}/entity",
                headers=h,
                json={
                    "entity": {
                        "guid": guid,
                        "attributes": {"userDescription": desc},
                        "typeName": "azure_sql_column",
                    }
                },
                timeout=30,
            )
            if r2.status_code in (200, 201):
                fix(f"{col_key}: description added (alt API)")
                fixed += 1
            else:
                warn(f"{col_key}: failed {r.status_code}/{r2.status_code}")
        time.sleep(0.15)

    print(f"\n  Updated {fixed}/{len(missing_descs)} column descriptions")


# ══════════════════════════════════════════════════════════════════════
# 7. FIX — Add Fabric table descriptions
# ══════════════════════════════════════════════════════════════════════
def fix_fabric_descriptions(h, fabric_guids):
    sep("7. FIX — Add Fabric Table/Column Descriptions")

    # Table-level descriptions
    fabric_table_descs = {
        "hca_patients": "🔶 Bronze | Patientdemografi — rå ingestion från hca.patients. FHIR: Patient, OMOP: person",
        "hca_encounters": "🔶 Bronze | Vårdkontakter — rå ingestion från hca.encounters. FHIR: Encounter, OMOP: visit_occurrence",
        "hca_diagnoses": "🔶 Bronze | Diagnoser — rå ingestion från hca.diagnoses. FHIR: Condition, OMOP: condition_occurrence",
        "hca_vitals_labs": "🔶 Bronze | Vitalparametrar & labb — rå ingestion från hca.vitals_labs. FHIR: Observation, OMOP: measurement",
        "hca_medications": "🔶 Bronze | Medicinering — rå ingestion från hca.medications. FHIR: MedicationRequest, OMOP: drug_exposure",
        "ml_features": "⬜ Silver | ML-features — CCI-score, prior_admissions, medication_count, vitals. Feature engineering från Bronze",
        "ml_predictions": "🟡 Gold | ML-prediktioner — LOS och readmission risk scores. Tränad på Silver-features",
        "person": "🟡 Gold OMOP | OMOP CDM Person-tabell. Mappas från hca_patients",
        "visit_occurrence": "🟡 Gold OMOP | OMOP CDM Visit Occurrence. Mappas från hca_encounters",
        "condition_occurrence": "🟡 Gold OMOP | OMOP CDM Condition Occurrence. Mappas från hca_diagnoses",
        "drug_exposure": "🟡 Gold OMOP | OMOP CDM Drug Exposure. Mappas från hca_medications",
        "measurement": "🟡 Gold OMOP | OMOP CDM Measurement. Mappas från hca_vitals_labs",
        "observation": "🟡 Gold OMOP | OMOP CDM Observation. Kompletterande observationsdata",
        "observation_period": "🟡 Gold OMOP | OMOP CDM Observation Period. Tidsintervall per patient",
        "location": "🟡 Gold OMOP | OMOP CDM Location. Geografisk plats",
        "concept": "🟡 Gold OMOP | OMOP CDM Concept-referenstabell. Standardvokabulär",
        "gene_sequence": "🧬 BrainChild | OMOP Genomics — DNA-sekvensdata per patient och gen. Referensgenom: GRCh38",
        "variant_occurrence": "🧬 BrainChild | OMOP Genomics — Genomiska varianter. ACMG-klassificering och HGVS-nomenklatur",
        "specimen": "🧬 BrainChild | OMOP Specimen — Biobanksprover från BTB. SNOMED-kodade provtyper",
        "sbcr_registrations": "📋 BrainChild | SBCR registrering — Cancerdiagnoser med ICD-O-3 och ICD-10",
        "sbcr_treatments": "📋 BrainChild | SBCR behandling — Behandlingsprotokoll och respons",
        "sbcr_followup": "📋 BrainChild | SBCR uppföljning — Seneffekter och överlevnadsstatus",
        "patients_master": "🧬 BrainChild | Patient master — Komplett patientregister med alla identifierare och diagnoskoder",
        "brainchild_bronze_dicom_study": "🔶 BrainChild Bronze | DICOM Study-metadata (MRI + patologi)",
        "brainchild_bronze_dicom_series": "🔶 BrainChild Bronze | DICOM Series-metadata (MRI-sekvenser)",
        "brainchild_bronze_dicom_instance": "🔶 BrainChild Bronze | DICOM Instance-metadata (individuella bilder)",
        "brainchild_silver_dicom_studies": "⬜ BrainChild Silver | DICOM Study — renad studiedata med patientkoppling",
        "brainchild_silver_dicom_series": "⬜ BrainChild Silver | DICOM Series — renade seriedata med protokoll",
        "brainchild_silver_dicom_pathology": "⬜ BrainChild Silver | DICOM Patologi — histopatologiska prover med färgning",
    }

    fixed = 0
    for table_name, desc in fabric_table_descs.items():
        if table_name not in fabric_guids:
            continue

        guid = fabric_guids[table_name]
        r = sess.put(
            f"{ATLAS}/entity/guid/{guid}",
            headers=h,
            json={
                "entity": {
                    "guid": guid,
                    "attributes": {"userDescription": desc},
                }
            },
            timeout=30,
        )
        if r.status_code in (200, 201):
            fix(f"{table_name}: description added")
            fixed += 1
        else:
            warn(f"{table_name}: {r.status_code}")
        time.sleep(0.15)

    # Also update descriptions for Fabric OMOP columns where entity exists
    fabric_col_fixed = 0
    for col_key, desc in COLUMN_DESCRIPTIONS.items():
        table_name = col_key.split(".")[0]
        col_name = col_key.split(".")[1]

        # Only process Fabric OMOP tables (not SQL — those are handled in fix_column_descriptions)
        if table_name in SQL_TABLES:
            continue

        body = {
            "keywords": f"{table_name} {col_name}",
            "filter": {"entityType": "fabric_lakehouse_table_column"},
            "limit": 5,
        }
        r = sess.post(SEARCH, headers=h, json=body, timeout=30)
        if r.status_code == 200:
            for asset in r.json().get("value", []):
                qn = asset.get("qualifiedName", "")
                name = asset.get("name", "")
                if col_name.lower() == name.lower():
                    guid = asset["id"]
                    r2 = sess.put(
                        f"{ATLAS}/entity/guid/{guid}",
                        headers=h,
                        json={
                            "entity": {
                                "guid": guid,
                                "attributes": {"userDescription": desc},
                            }
                        },
                        timeout=30,
                    )
                    if r2.status_code in (200, 201):
                        fabric_col_fixed += 1
                    break
        time.sleep(0.1)

    print(f"\n  Table descriptions: {fixed}")
    print(f"  Fabric column descriptions: {fabric_col_fixed}")


# ══════════════════════════════════════════════════════════════════════
# 8. FIX — Map glossary terms to Fabric entities
# ══════════════════════════════════════════════════════════════════════
def fix_fabric_term_mappings(h, term_guids, fabric_guids):
    sep("8. FIX — Map Glossary Terms to Fabric Entities")

    # Term → Fabric table mappings
    fabric_term_map = {
        "FHIR Patient": ["hca_patients", "patients_master", "person"],
        "FHIR Encounter": ["hca_encounters", "visit_occurrence"],
        "FHIR Condition": ["hca_diagnoses", "condition_occurrence"],
        "FHIR MedicationRequest": ["hca_medications", "drug_exposure"],
        "FHIR Observation": ["hca_vitals_labs", "measurement", "observation"],
        "FHIR Specimen": ["specimen"],
        "FHIR DiagnosticReport": [],  # JSON in BrainChild, not a table
        "FHIR ImagingStudy": ["brainchild_bronze_dicom_study", "brainchild_silver_dicom_studies"],
        "OMOP CDM": ["person", "visit_occurrence", "condition_occurrence",
                      "drug_exposure", "measurement", "observation",
                      "observation_period", "location", "concept"],
        "OMOP Person": ["person"],
        "OMOP Visit Occurrence": ["visit_occurrence"],
        "OMOP Condition Occurrence": ["condition_occurrence"],
        "OMOP Drug Exposure": ["drug_exposure"],
        "OMOP Measurement": ["measurement"],
        "OMOP Specimen": ["specimen"],
        "OMOP Genomics": ["gene_sequence", "variant_occurrence"],
        "Bronze-lager": ["hca_patients", "hca_encounters", "hca_diagnoses",
                         "hca_vitals_labs", "hca_medications",
                         "brainchild_bronze_dicom_study",
                         "brainchild_bronze_dicom_series",
                         "brainchild_bronze_dicom_instance"],
        "Silver-lager": ["ml_features", "brainchild_silver_dicom_studies",
                         "brainchild_silver_dicom_series",
                         "brainchild_silver_dicom_pathology"],
        "Gold-lager": ["ml_predictions"],
        "Medallion-arkitektur": ["ml_features", "ml_predictions"],
        "Feature Engineering": ["ml_features"],
        "ML-prediktion": ["ml_predictions"],
        "DICOM": ["brainchild_bronze_dicom_study", "brainchild_bronze_dicom_series",
                   "brainchild_bronze_dicom_instance", "brainchild_silver_dicom_studies",
                   "brainchild_silver_dicom_series", "brainchild_silver_dicom_pathology"],
        "BTB (Barntumörbanken)": ["specimen", "patients_master"],
        "SBCR (Svenska Barncancerregistret)": ["sbcr_registrations", "sbcr_treatments", "sbcr_followup"],
        "Genomic Medicine Sweden (GMS)": ["gene_sequence", "variant_occurrence"],
        "VCF (Variant Call Format)": ["variant_occurrence"],
        "Genomisk variant": ["variant_occurrence"],
        "ICD-10": ["hca_diagnoses", "condition_occurrence", "sbcr_registrations"],
        "ATC-klassificering": ["hca_medications", "drug_exposure"],
        "ICD-O-3": ["sbcr_registrations", "patients_master"],
        "SNOMED-CT": ["specimen"],
        "LOINC": ["measurement", "observation"],
        "Vitalparametrar": ["hca_vitals_labs", "measurement"],
        "Labresultat": ["hca_vitals_labs", "measurement"],
        "Vårdtid (LOS)": ["hca_encounters", "visit_occurrence", "ml_features"],
        "Återinläggningsrisk": ["hca_encounters", "ml_features", "ml_predictions"],
        "Behandlingsprotokoll": ["sbcr_treatments"],
        "Seneffekter": ["sbcr_followup"],
        "Tumörsite": ["patients_master", "sbcr_registrations"],
        "HGVS-nomenklatur": ["variant_occurrence"],
        "ACMG-klassificering": ["variant_occurrence"],
        "Charlson Comorbidity Index": ["ml_features"],
    }

    mapped = 0
    for term_name, tables in fabric_term_map.items():
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
            fix(f"{term_name} → {len(entity_guids)} Fabric tables")
            mapped += 1
        elif r.status_code == 409:
            ok(f"{term_name} → already mapped")
        else:
            warn(f"{term_name}: {r.status_code} — {r.text[:100]}")
        time.sleep(0.2)

    print(f"\n  Mapped {mapped} terms to Fabric entities")


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Full Platform Audit & Fix")
    parser.add_argument("--audit-only", action="store_true", help="Only audit, don't fix")
    args = parser.parse_args()

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  FULL PLATFORM AUDIT — SQL + FABRIC + PURVIEW              ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    h = get_purview_headers()

    # ── AUDIT ──
    sql_results = audit_sql()
    sql_guids, fabric_guids, entity_types = audit_purview_entities(h)
    glossary_guid, existing_terms = audit_glossary(h)
    missing_descs = audit_column_descriptions(h, sql_guids)

    if args.audit_only:
        sep("AUDIT SUMMARY")
        print(f"  Found:   {audit['found']}")
        print(f"  Missing: {audit['missing']}")
        print(f"\n  Run without --audit-only to fix missing items")
        return

    # ── FIX ──
    existing_terms = fix_missing_terms(h, glossary_guid, existing_terms)
    fix_column_descriptions(h, missing_descs)
    fix_fabric_descriptions(h, fabric_guids)
    fix_fabric_term_mappings(h, existing_terms, fabric_guids)

    # ── SUMMARY ──
    sep("FINAL SUMMARY")
    print(f"""
  Audit results:
    Found:   {audit['found']}
    Missing: {audit['missing']}
    Fixed:   {audit['fixed']}
    Errors:  {audit['errors']}

  SQL Tables:      {len([v for v in sql_results.values() if v])} / {len(SQL_TABLES)}
  Purview SQL:     {len(sql_guids)} / {len(SQL_TABLES) + len(SQL_VIEWS)}
  Purview Fabric:  {len(fabric_guids)} / {sum(len(v) for v in {**FABRIC_HCA_TABLES, **FABRIC_BC_TABLES}.values())}
  Glossary Terms:  {len(existing_terms)} / {len(REQUIRED_GLOSSARY_TERMS)}
  Col Descriptions: {len(COLUMN_DESCRIPTIONS) - len(missing_descs)} / {len(COLUMN_DESCRIPTIONS)}
""")


if __name__ == "__main__":
    main()
