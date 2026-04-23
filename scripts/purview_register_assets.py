"""
Purview Asset Registration — Direct Atlas API registration bypassing broken scan API.

Since the Purview scan creation API returns 400 "Invalid scanAuthorizationType" for ALL
scan kinds (MSI, Credential, SqlAuth, etc.), this script registers SQL entities directly
via the Atlas API. This gives us full assets to work with for:
  - Glossary term → entity mappings
  - PII classifications on sensitive columns
  - Labels/tags for discoverability
  - Descriptions and metadata enrichment

Entity hierarchy created:
  azure_sql_server → azure_sql_db → azure_sql_schema → azure_sql_table/view → azure_sql_column

Also:
  - Fetches existing glossary term GUIDs (fixes re-run issue where 409s leave term_guids empty)
  - Maps terms to SQL entities
  - Applies PII classifications
  - Adds labels/tags
  - Adds owner/expert contacts
"""
import requests, json, time, sys, uuid
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

# ── CONFIG ──
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

sess = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
sess.mount("https://", HTTPAdapter(max_retries=retry))

ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"

SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DB = "HealthcareAnalyticsDB"
SQL_SCHEMA = "hca"
COLLECTION = "sql-databases"  # Target collection for all SQL assets

# QualifiedName patterns matching Purview scanner conventions
SERVER_QN = f"mssql://{SQL_SERVER}"
DB_QN = f"{SERVER_QN}/{SQL_DB}"
SCHEMA_QN = f"{DB_QN}/dbo/{SQL_SCHEMA}"  # Purview convention for schemas

# SQL table/view definitions with columns
# Format: {table_name: {type, description, columns: {col_name: {type, description}}}}
TABLES = {
    "patients": {
        "type": "azure_sql_table",
        "description": "Patientdemografi — 10 000 syntetiska patienter med FHIR Patient / OMOP Person mappning",
        "columns": {
            "patient_id":     {"type": "uniqueidentifier", "desc": "Unik patient-ID (FHIR Patient.id)"},
            "birth_date":     {"type": "date",             "desc": "Födelsedatum (FHIR Patient.birthDate)"},
            "gender":         {"type": "char",             "desc": "Kön M/F/O (FHIR Patient.gender)"},
            "ses_level":      {"type": "tinyint",          "desc": "Socioekonomisk nivå 1-5"},
            "postal_code":    {"type": "varchar",          "desc": "Postnummer (pseudonymiserat)"},
            "region":         {"type": "varchar",          "desc": "Svensk region (län)"},
            "smoking_status": {"type": "varchar",          "desc": "Rökstatus: never/former/current"},
            "created_at":     {"type": "datetime2",        "desc": "Skapad tidsstämpel"},
        }
    },
    "encounters": {
        "type": "azure_sql_table",
        "description": "Vårdbesök — 17 292 encounters med LOS och återinläggningsrisk. FHIR Encounter / OMOP Visit Occurrence",
        "columns": {
            "encounter_id":         {"type": "uniqueidentifier", "desc": "Unik besöks-ID (FHIR Encounter.id)"},
            "patient_id":           {"type": "uniqueidentifier", "desc": "FK → patients.patient_id"},
            "admission_date":       {"type": "date",             "desc": "Inläggningsdatum"},
            "discharge_date":       {"type": "date",             "desc": "Utskrivningsdatum"},
            "department":           {"type": "varchar",          "desc": "Avdelning: Cardiology, Neurology, etc."},
            "admission_source":     {"type": "varchar",          "desc": "Källa: Emergency, Planned, Transfer"},
            "discharge_disposition":{"type": "varchar",          "desc": "Utskrivning: Home, Transfer, etc."},
            "los_days":             {"type": "smallint",         "desc": "Vårdtid i dagar (Length of Stay)"},
            "readmission_30d":      {"type": "bit",              "desc": "Återinlagd inom 30 dagar (0/1)"},
            "created_at":           {"type": "datetime2",        "desc": "Skapad tidsstämpel"},
        }
    },
    "diagnoses": {
        "type": "azure_sql_table",
        "description": "Diagnoser — 30 297 ICD-10-kodade diagnoser. FHIR Condition / OMOP Condition Occurrence",
        "columns": {
            "diagnosis_id":      {"type": "uniqueidentifier", "desc": "Unik diagnos-ID"},
            "encounter_id":      {"type": "uniqueidentifier", "desc": "FK → encounters.encounter_id"},
            "icd10_code":        {"type": "varchar",          "desc": "ICD-10 diagnoskod (t.ex. I21.0)"},
            "icd10_description": {"type": "varchar",          "desc": "Diagnosbeskrivning på svenska"},
            "diagnosis_type":    {"type": "varchar",          "desc": "Typ: Primary, Secondary, Complication"},
            "confirmed_date":    {"type": "date",             "desc": "Datum för bekräftad diagnos"},
        }
    },
    "vitals_labs": {
        "type": "azure_sql_table",
        "description": "Vitalparametrar & labresultat — 48 131 mätningar. FHIR Observation / OMOP Measurement",
        "columns": {
            "measurement_id":    {"type": "uniqueidentifier", "desc": "Unik mätnings-ID"},
            "encounter_id":      {"type": "uniqueidentifier", "desc": "FK → encounters.encounter_id"},
            "measured_at":       {"type": "datetime2",        "desc": "Tidpunkt för mätning"},
            "systolic_bp":       {"type": "smallint",         "desc": "Systoliskt blodtryck (mmHg)"},
            "diastolic_bp":      {"type": "smallint",         "desc": "Diastoliskt blodtryck (mmHg)"},
            "heart_rate":        {"type": "smallint",         "desc": "Hjärtfrekvens (slag/min)"},
            "temperature_c":     {"type": "decimal",          "desc": "Kroppstemperatur (°C)"},
            "oxygen_saturation": {"type": "decimal",          "desc": "Syremättnad (SpO2 %)"},
            "glucose_mmol":      {"type": "decimal",          "desc": "Blodglukos (mmol/L)"},
            "creatinine_umol":   {"type": "decimal",          "desc": "Kreatinin (µmol/L)"},
            "hemoglobin_g":      {"type": "decimal",          "desc": "Hemoglobin (g/L)"},
            "sodium_mmol":       {"type": "decimal",          "desc": "Natrium (mmol/L)"},
            "potassium_mmol":    {"type": "decimal",          "desc": "Kalium (mmol/L)"},
            "bmi":               {"type": "decimal",          "desc": "Body Mass Index"},
            "weight_kg":         {"type": "decimal",          "desc": "Vikt (kg)"},
        }
    },
    "medications": {
        "type": "azure_sql_table",
        "description": "Läkemedel — 60 563 förskrivningar med ATC-klassificering. FHIR MedicationRequest / OMOP Drug Exposure",
        "columns": {
            "medication_id": {"type": "uniqueidentifier", "desc": "Unik läkemedels-ID"},
            "encounter_id":  {"type": "uniqueidentifier", "desc": "FK → encounters.encounter_id"},
            "atc_code":      {"type": "varchar",          "desc": "ATC-klassificeringskod (t.ex. C01AA05)"},
            "drug_name":     {"type": "varchar",          "desc": "Läkemedelsnamn"},
            "dose_mg":       {"type": "decimal",          "desc": "Dos i milligram"},
            "frequency":     {"type": "varchar",          "desc": "Doseringsfrekvens"},
            "route":         {"type": "varchar",          "desc": "Administreringsväg: Oral, IV, etc."},
            "start_date":    {"type": "date",             "desc": "Startdatum för medicinering"},
            "end_date":      {"type": "date",             "desc": "Slutdatum för medicinering"},
        }
    },
    "vw_ml_encounters": {
        "type": "azure_sql_view",
        "description": "Gold-lager ML-vy — 17 292 rader med sammanslagen patient+besök+diagnos+lab-data. Medallion Gold layer.",
        "columns": {
            "encounter_id":      {"type": "uniqueidentifier", "desc": "Besöks-ID"},
            "patient_id":        {"type": "uniqueidentifier", "desc": "Patient-ID"},
            "admission_date":    {"type": "date",             "desc": "Inläggningsdatum"},
            "discharge_date":    {"type": "date",             "desc": "Utskrivningsdatum"},
            "department":        {"type": "varchar",          "desc": "Avdelning"},
            "admission_source":  {"type": "varchar",          "desc": "Inläggningskälla"},
            "los_days":          {"type": "smallint",         "desc": "Vårdtid (dagar) — ML target"},
            "readmission_30d":   {"type": "bit",              "desc": "Återinläggning 30d — ML target"},
            "age_at_admission":  {"type": "int",              "desc": "Ålder vid inläggning (beräknad)"},
            "gender":            {"type": "char",             "desc": "Kön"},
            "ses_level":         {"type": "tinyint",          "desc": "Socioekonomisk nivå"},
            "smoking_status":    {"type": "varchar",          "desc": "Rökstatus"},
            "systolic_bp":       {"type": "smallint",         "desc": "Systoliskt blodtryck (feature)"},
            "glucose_mmol":      {"type": "decimal",          "desc": "Blodglukos (feature)"},
            "creatinine_umol":   {"type": "decimal",          "desc": "Kreatinin (feature)"},
            "bmi":               {"type": "decimal",          "desc": "BMI (feature)"},
            "primary_icd10":     {"type": "varchar",          "desc": "Primär ICD-10 diagnoskod"},
            "primary_diagnosis": {"type": "varchar",          "desc": "Primär diagnosbeskrivning"},
        }
    },
}


def sep(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def refresh_token():
    global token, h
    token = cred.get_token("https://purview.azure.net/.default").token
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ══════════════════════════════════════════════════════════════════════
# STEP 1: REGISTER SQL SERVER → DATABASE → SCHEMA HIERARCHY
# ══════════════════════════════════════════════════════════════════════
sep("1. REGISTERING SQL SERVER HIERARCHY")
refresh_token()

entity_guids = {}  # qualifiedName → guid

def create_entity(type_name, qn, name, description, collection=COLLECTION, attrs_extra=None):
    """Create an Atlas entity, returning its GUID."""
    attrs = {
        "qualifiedName": qn,
        "name": name,
        "description": description,
    }
    if attrs_extra:
        attrs.update(attrs_extra)

    body = {
        "entity": {
            "typeName": type_name,
            "attributes": attrs,
            "collectionId": collection,
        }
    }
    r = sess.post(f"{ATLAS}/entity?api-version=2023-09-01", headers=h, json=body, timeout=30)
    if r.status_code in (200, 201):
        result = r.json()
        mutations = result.get("mutatedEntities", {})
        created = mutations.get("CREATE", []) + mutations.get("UPDATE", [])
        if created:
            guid = created[0]["guid"]
            entity_guids[qn] = guid
            return guid
        # Entity already exists unchanged — extract GUID from guidAssignments
        assignments = result.get("guidAssignments", {})
        if assignments:
            guid = list(assignments.values())[0]
            entity_guids[qn] = guid
            return guid
        print(f"    ⚠️ No GUID in response for {name}: {r.text[:200]}")
    else:
        print(f"    ❌ API error {r.status_code} for {name}: {r.text[:200]}")
    return None

# 1a. Server
guid = create_entity("azure_sql_server", SERVER_QN, "sql-hca-demo",
                      "Azure SQL Server — Healthcare Analytics & BrainChild Demo (Sweden Central)")
if guid:
    print(f"  ✅ Server: sql-hca-demo ({guid[:12]}...)")
else:
    print("  ❌ Server creation failed")

# 1b. Database
guid = create_entity("azure_sql_db", DB_QN, SQL_DB,
                      "Healthcare Analytics database — 166 283 rader, 5 tabeller, 1 ML-vy. Schema: hca.")
if guid:
    print(f"  ✅ Database: {SQL_DB} ({guid[:12]}...)")
else:
    print("  ❌ Database creation failed")

# 1c. Schema
guid = create_entity("azure_sql_schema", SCHEMA_QN, SQL_SCHEMA,
                      "Healthcare Analytics schema — klinisk data med FHIR R4 / OMOP CDM mappning")
if guid:
    print(f"  ✅ Schema: {SQL_SCHEMA} ({guid[:12]}...)")
else:
    print("  ❌ Schema creation failed")


# ══════════════════════════════════════════════════════════════════════
# STEP 2: REGISTER ALL TABLES, VIEWS AND COLUMNS
# ══════════════════════════════════════════════════════════════════════
sep("2. REGISTERING TABLES, VIEWS AND COLUMNS")

table_guids = {}  # table_name → guid
column_guids = {}  # (table_name, col_name) → guid

for table_name, table_def in TABLES.items():
    type_name = table_def["type"]
    table_qn = f"{SCHEMA_QN}/{table_name}"

    guid = create_entity(type_name, table_qn, table_name, table_def["description"])
    if guid:
        table_guids[table_name] = guid
        print(f"  ✅ {type_name.split('_')[-1].upper()}: {table_name} ({guid[:12]}...)")
    else:
        print(f"  ❌ {table_name}: creation failed")
        continue

    # Register columns
    col_type = "azure_sql_column" if "view" not in type_name else "azure_sql_view_column"
    col_count = 0
    for col_name, col_def in table_def["columns"].items():
        col_qn = f"{table_qn}/{col_name}"
        col_guid = create_entity(col_type, col_qn, col_name, col_def["desc"],
                                 attrs_extra={"data_type": col_def["type"]})
        if col_guid:
            column_guids[(table_name, col_name)] = col_guid
            col_count += 1
        time.sleep(0.05)  # Small delay to avoid throttling

    print(f"       └─ {col_count}/{len(table_def['columns'])} columns registered")

print(f"\n  Total: {len(table_guids)} tables/views, {len(column_guids)} columns")


# ══════════════════════════════════════════════════════════════════════
# STEP 3: SET RELATIONSHIPS (schema→tables, tables→columns)
# ══════════════════════════════════════════════════════════════════════
sep("3. SETTING ENTITY RELATIONSHIPS")

schema_guid = entity_guids.get(SCHEMA_QN)
rel_count = 0

if schema_guid:
    for table_name, tbl_guid in table_guids.items():
        table_def = TABLES[table_name]
        # Relationship: schema → table (azure_sql_schema_tables)
        rel_type = "azure_sql_schema_tables" if table_def["type"] == "azure_sql_table" else "azure_sql_schema_views"
        rel_body = {
            "typeName": rel_type,
            "end1": {"guid": schema_guid, "typeName": "azure_sql_schema"},
            "end2": {"guid": tbl_guid, "typeName": table_def["type"]},
        }
        r = sess.post(f"{ATLAS}/relationship?api-version=2023-09-01", headers=h, json=rel_body, timeout=30)
        if r.status_code == 200:
            rel_count += 1
        time.sleep(0.05)

    # Relationship: table → columns
    for (table_name, col_name), col_guid in column_guids.items():
        tbl_guid = table_guids.get(table_name)
        if not tbl_guid:
            continue
        table_def = TABLES[table_name]
        if table_def["type"] == "azure_sql_table":
            rel_type = "azure_sql_table_columns"
            rel_body = {
                "typeName": rel_type,
                "end1": {"guid": tbl_guid, "typeName": "azure_sql_table"},
                "end2": {"guid": col_guid, "typeName": "azure_sql_column"},
            }
        else:
            rel_type = "azure_sql_view_columns"
            rel_body = {
                "typeName": rel_type,
                "end1": {"guid": tbl_guid, "typeName": "azure_sql_view"},
                "end2": {"guid": col_guid, "typeName": "azure_sql_view_column"},
            }
        r = sess.post(f"{ATLAS}/relationship?api-version=2023-09-01", headers=h, json=rel_body, timeout=30)
        if r.status_code == 200:
            rel_count += 1
        time.sleep(0.05)

# Also: db → schema
db_guid = entity_guids.get(DB_QN)
if db_guid and schema_guid:
    rel_body = {
        "typeName": "azure_sql_db_schemas",
        "end1": {"guid": db_guid, "typeName": "azure_sql_db"},
        "end2": {"guid": schema_guid, "typeName": "azure_sql_schema"},
    }
    r = sess.post(f"{ATLAS}/relationship?api-version=2023-09-01", headers=h, json=rel_body, timeout=30)
    if r.status_code == 200:
        rel_count += 1

# server → db
server_guid = entity_guids.get(SERVER_QN)
if server_guid and db_guid:
    rel_body = {
        "typeName": "azure_sql_server_databases",
        "end1": {"guid": server_guid, "typeName": "azure_sql_server"},
        "end2": {"guid": db_guid, "typeName": "azure_sql_db"},
    }
    r = sess.post(f"{ATLAS}/relationship?api-version=2023-09-01", headers=h, json=rel_body, timeout=30)
    if r.status_code == 200:
        rel_count += 1

print(f"  ✅ Created {rel_count} relationships")


# ══════════════════════════════════════════════════════════════════════
# STEP 4: FETCH EXISTING GLOSSARY TERMS (fixes re-run issue)
# ══════════════════════════════════════════════════════════════════════
sep("4. FETCHING GLOSSARY TERM GUIDS")
refresh_token()

# Find the glossary
r = sess.get(f"{ATLAS}/glossary", headers=h, timeout=30)
glossaries = r.json() if r.status_code == 200 else []
glossary_guid = None

for g in glossaries:
    if "Sjukvårdstermer" in g.get("name", ""):
        glossary_guid = g["guid"]
        break

if not glossary_guid:
    print("  ⚠️ Glossary 'Sjukvårdstermer' not found — run purview_rebuild.py first")
    print("     Skipping term mapping steps")
else:
    print(f"  ✅ Found glossary: {glossary_guid[:12]}...")

# Fetch all terms
term_guids = {}
if glossary_guid:
    r = sess.get(f"{ATLAS}/glossary/{glossary_guid}/terms?limit=500", headers=h, timeout=30)
    if r.status_code == 200:
        for term in r.json():
            term_guids[term["name"]] = term["guid"]
        print(f"  ✅ Loaded {len(term_guids)} term GUIDs")
        for name in sorted(term_guids.keys()):
            print(f"     • {name}")
    else:
        print(f"  ❌ Failed to load terms: {r.status_code}")


# ══════════════════════════════════════════════════════════════════════
# STEP 5: MAP GLOSSARY TERMS TO SQL ENTITIES
# ══════════════════════════════════════════════════════════════════════
sep("5. MAPPING GLOSSARY TERMS TO SQL ENTITIES")

# Term → table/column mappings
term_entity_map = {
    "ICD-10": [("diagnoses", ["icd10_code", "icd10_description"])],
    "ATC-klassificering": [("medications", ["atc_code", "drug_name"])],
    "Skyddad hälsoinformation (PHI)": [("patients", ["patient_id", "birth_date"]), ("encounters", ["encounter_id"])],
    "Svenskt personnummer": [("patients", ["postal_code"])],
    "FHIR R4": [("patients", []), ("encounters", []), ("diagnoses", []), ("vitals_labs", []), ("medications", [])],
    "FHIR Patient": [("patients", [])],
    "FHIR Encounter": [("encounters", [])],
    "FHIR Condition": [("diagnoses", [])],
    "FHIR MedicationRequest": [("medications", [])],
    "FHIR Observation": [("vitals_labs", [])],
    "OMOP CDM": [("patients", []), ("encounters", []), ("diagnoses", []), ("vitals_labs", []), ("medications", [])],
    "OMOP Person": [("patients", [])],
    "OMOP Visit Occurrence": [("encounters", [])],
    "OMOP Condition Occurrence": [("diagnoses", [])],
    "OMOP Drug Exposure": [("medications", [])],
    "OMOP Measurement": [("vitals_labs", [])],
    "Medallion-arkitektur": [("vw_ml_encounters", [])],
    "Gold-lager": [("vw_ml_encounters", [])],
    "Vårdtid (LOS)": [("encounters", ["los_days"]), ("vw_ml_encounters", [])],
    "Återinläggningsrisk": [("encounters", ["readmission_30d"]), ("vw_ml_encounters", [])],
    "Vitalparametrar": [("vitals_labs", ["systolic_bp", "diastolic_bp", "heart_rate", "oxygen_saturation", "temperature_c"])],
    "Labresultat": [("vitals_labs", ["hemoglobin_g", "glucose_mmol", "creatinine_umol", "bmi", "weight_kg"])],
}

mapped_count = 0

for term_name, targets in term_entity_map.items():
    if term_name not in term_guids:
        print(f"  ⚠️ Term '{term_name}' not in glossary — skipping")
        continue

    term_guid = term_guids[term_name]
    entity_refs = []

    for tbl_name, columns in targets:
        tbl_guid = table_guids.get(tbl_name)
        if tbl_guid:
            entity_refs.append({"guid": tbl_guid})
        for col in columns:
            col_guid = column_guids.get((tbl_name, col))
            if col_guid:
                entity_refs.append({"guid": col_guid})

    if entity_refs:
        r = sess.post(
            f"{ATLAS}/glossary/terms/{term_guid}/assignedEntities",
            headers=h, json=entity_refs, timeout=30
        )
        if r.status_code in (200, 201, 204):
            mapped_count += 1
            names = [e["guid"][:8] for e in entity_refs[:3]]
            extra = f" +{len(entity_refs)-3}" if len(entity_refs) > 3 else ""
            print(f"  ✅ {term_name} → {len(entity_refs)} entities ({', '.join(names)}{extra})")
        elif "already" in r.text.lower() or r.status_code == 409:
            mapped_count += 1
            print(f"  ↳ {term_name}: already mapped")
        else:
            print(f"  ❌ {term_name}: {r.status_code} — {r.text[:150]}")
        time.sleep(0.15)

print(f"\n  Mapped {mapped_count}/{len(term_entity_map)} terms")


# ══════════════════════════════════════════════════════════════════════
# STEP 6: ADD PII CLASSIFICATIONS TO SENSITIVE COLUMNS
# ══════════════════════════════════════════════════════════════════════
sep("6. ADDING PII CLASSIFICATIONS")
refresh_token()

pii_columns = {
    ("patients", "patient_id"):       ["MICROSOFT.PERSONAL.NAME"],
    ("patients", "birth_date"):       ["MICROSOFT.PERSONAL.DATEOFBIRTH"],
    ("patients", "postal_code"):      ["MICROSOFT.PERSONAL.ZIPCODE"],
    ("patients", "gender"):           ["MICROSOFT.PERSONAL.GENDER"],
    ("patients", "smoking_status"):   ["MICROSOFT.PERSONAL.HEALTH"],
    ("encounters", "patient_id"):     ["MICROSOFT.PERSONAL.NAME"],
    ("encounters", "admission_date"): ["MICROSOFT.PERSONAL.DATE"],
    ("encounters", "discharge_date"): ["MICROSOFT.PERSONAL.DATE"],
    ("vitals_labs", "systolic_bp"):   ["MICROSOFT.PERSONAL.HEALTH"],
    ("vitals_labs", "heart_rate"):    ["MICROSOFT.PERSONAL.HEALTH"],
    ("vitals_labs", "glucose_mmol"):  ["MICROSOFT.PERSONAL.HEALTH"],
    ("vitals_labs", "hemoglobin_g"):  ["MICROSOFT.PERSONAL.HEALTH"],
    ("medications", "drug_name"):     ["MICROSOFT.PERSONAL.HEALTH"],
    ("diagnoses", "icd10_code"):      ["MICROSOFT.PERSONAL.HEALTH"],
}

classified_count = 0
for (table_name, col_name), class_types in pii_columns.items():
    col_guid = column_guids.get((table_name, col_name))
    if not col_guid:
        print(f"  ⏳ {table_name}.{col_name}: no GUID — skipping")
        continue

    classifications = [{"typeName": ct} for ct in class_types]
    r = sess.post(
        f"{DATAMAP}/entity/guid/{col_guid}/classifications?api-version=2023-09-01",
        headers=h, json=classifications, timeout=30
    )
    if r.status_code == 204:
        classified_count += 1
        print(f"  ✅ {table_name}.{col_name} ← {', '.join(class_types)}")
    elif r.status_code == 409 or "already" in r.text.lower():
        classified_count += 1
        print(f"  ↳ {table_name}.{col_name}: already classified")
    else:
        print(f"  ⚠️ {table_name}.{col_name}: {r.status_code} — {r.text[:120]}")
    time.sleep(0.1)

print(f"\n  Classified {classified_count}/{len(pii_columns)} columns")


# ══════════════════════════════════════════════════════════════════════
# STEP 7: ADD LABELS/TAGS TO TABLES
# ══════════════════════════════════════════════════════════════════════
sep("7. ADDING LABELS/TAGS")

table_labels = {
    "patients":         ["PHI", "FHIR-Patient", "OMOP-Person", "Medallion-Source", "Klinisk-Data"],
    "encounters":       ["PHI", "FHIR-Encounter", "OMOP-Visit", "Medallion-Source", "ML-Target", "Klinisk-Data"],
    "diagnoses":        ["ICD-10", "FHIR-Condition", "OMOP-Condition", "Medallion-Source", "Klinisk-Data"],
    "medications":      ["ATC", "FHIR-MedicationRequest", "OMOP-DrugExposure", "Medallion-Source", "Klinisk-Data"],
    "vitals_labs":      ["FHIR-Observation", "OMOP-Measurement", "Medallion-Source", "ML-Feature", "Klinisk-Data"],
    "vw_ml_encounters": ["Gold-Layer", "ML-Ready", "Medallion-Gold", "Feature-Store"],
}

labeled_count = 0
for table_name, labels in table_labels.items():
    guid = table_guids.get(table_name)
    if not guid:
        continue

    r = sess.put(
        f"{DATAMAP}/entity/guid/{guid}/labels",
        headers=h, json=labels, timeout=30
    )
    if r.status_code == 204:
        labeled_count += 1
        print(f"  ✅ {table_name} ← {', '.join(labels)}")
    else:
        print(f"  ⚠️ {table_name}: {r.status_code} — {r.text[:100]}")
    time.sleep(0.1)

print(f"\n  Labeled {labeled_count}/{len(table_labels)} tables/views")


# ══════════════════════════════════════════════════════════════════════
# STEP 8: ADD DESCRIPTIONS TO ENTITIES (userDescription + expert)
# ══════════════════════════════════════════════════════════════════════
sep("8. ENRICHING ENTITY METADATA")

# Add rich user descriptions with standards references
enrichments = {
    "patients": "📊 Patientdemografi (10 000 patienter)\n"
                "Standards: FHIR Patient, OMOP Person\n"
                "PHI-känsliga kolumner: patient_id, birth_date, postal_code\n"
                "Kopplad till: encounters (patient_id), Medallion Bronze→Silver→Gold",
    "encounters": "🏥 Vårdbesök (17 292 encounters)\n"
                  "Standards: FHIR Encounter, OMOP Visit Occurrence\n"
                  "ML-mål: los_days (vårdtid), readmission_30d (återinläggning)\n"
                  "Kopplad till: patients, diagnoses, vitals_labs, medications",
    "diagnoses": "🩺 Diagnoser (30 297 st, ICD-10-klassificerade)\n"
                 "Standards: FHIR Condition, OMOP Condition Occurrence\n"
                 "Huvudkolumner: icd10_code, icd10_description\n"
                 "Kopplad till: encounters (encounter_id)",
    "vitals_labs": "🔬 Vitalparametrar & labresultat (48 131 mätningar)\n"
                   "Standards: FHIR Observation, OMOP Measurement\n"
                   "Vital: systolic_bp, heart_rate, temperature_c, oxygen_saturation\n"
                   "Lab: glucose_mmol, creatinine_umol, hemoglobin_g, bmi",
    "medications": "💊 Läkemedel (60 563 förskrivningar, ATC-klassificerade)\n"
                   "Standards: FHIR MedicationRequest, OMOP Drug Exposure\n"
                   "Huvudkolumner: atc_code, drug_name, dose_mg\n"
                   "Kopplad till: encounters (encounter_id)",
    "vw_ml_encounters": "🤖 ML Gold View (17 292 rader)\n"
                        "Medallion Gold Layer — sammanslagen patient+besök+diagnos+lab\n"
                        "ML-target: los_days, readmission_30d\n"
                        "Features: age_at_admission, systolic_bp, glucose_mmol, bmi, primary_icd10",
}

enriched_count = 0
for table_name, desc in enrichments.items():
    guid = table_guids.get(table_name)
    if not guid:
        continue

    r = sess.put(
        f"{ATLAS}/entity/guid/{guid}?name=userDescription",
        headers=h, data=json.dumps(desc), timeout=30
    )
    if r.status_code == 200:
        enriched_count += 1
        print(f"  ✅ {table_name}: description enriched")
    else:
        print(f"  ⚠️ {table_name}: {r.status_code}")
    time.sleep(0.1)

print(f"\n  Enriched {enriched_count}/{len(enrichments)} entities")


# ══════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════════════
sep("ASSET REGISTRATION COMPLETE")

# Verify via search
time.sleep(2)  # Let indexing catch up
refresh_token()

r = sess.post(SEARCH, headers=h, json={"keywords": "*", "limit": 1}, timeout=30)
total_assets = r.json().get("@search.count", "?") if r.status_code == 200 else "?"

# Count by type
type_counts = {}
for type_name in ["azure_sql_table", "azure_sql_view", "azure_sql_column", "azure_sql_server", "azure_sql_db", "azure_sql_schema"]:
    r = sess.post(SEARCH, headers=h, json={"keywords": "*", "filter": {"entityType": type_name}, "limit": 1}, timeout=30)
    if r.status_code == 200:
        type_counts[type_name] = r.json().get("@search.count", 0)

print(f"""
  ╔══════════════════════════════════════════════════════════╗
  ║       PURVIEW ASSET REGISTRATION — RESULTAT             ║
  ╠══════════════════════════════════════════════════════════╣
  ║  Total assets in catalog:    {str(total_assets):>6}                     ║
  ║                                                          ║
  ║  Registered this run:                                    ║
  ║    SQL Servers:              {str(type_counts.get('azure_sql_server', '?')):>6}                     ║
  ║    SQL Databases:            {str(type_counts.get('azure_sql_db', '?')):>6}                     ║
  ║    SQL Schemas:              {str(type_counts.get('azure_sql_schema', '?')):>6}                     ║
  ║    SQL Tables:               {str(type_counts.get('azure_sql_table', '?')):>6}                     ║
  ║    SQL Views:                {str(type_counts.get('azure_sql_view', '?')):>6}                     ║
  ║    SQL Columns:              {str(type_counts.get('azure_sql_column', '?')):>6}                     ║
  ║                                                          ║
  ║  Governance applied:                                     ║
  ║    Terms mapped:             {str(mapped_count):>6}                     ║
  ║    PII classified:           {str(classified_count):>6}                     ║
  ║    Labels applied:           {str(labeled_count):>6}                     ║
  ║    Descriptions enriched:    {str(enriched_count):>6}                     ║
  ╚══════════════════════════════════════════════════════════╝
""")

print("  ✅ ASSET REGISTRATION COMPLETE!")
print("  Note: Governance domains and data products must be created in the Purview portal:")
print("    https://purview.microsoft.com → Data Catalog → Governance Domains")
