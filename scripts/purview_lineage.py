"""
Purview Lineage Setup
======================
Creates Process entities in Purview to show data lineage:
  - SQL tables → Fabric Bronze (ETL pipeline)
  - Fabric Bronze → Fabric Silver (notebook transformation)
  - FHIR Server → Bronze tables (FHIR ingestion)
  - DICOM Server → image processing pipeline

Adapted from Microsoft-Purview-Unified-Catalog/create_lineage.py

Usage:
  python scripts/purview_lineage.py
"""
import json
import os
import sys
import time

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

cred = AzureCliCredential(process_timeout=30)
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1,
                                                      status_forcelist=[429, 502, 503])))

# ── ENDPOINTS ──
ACCT = "https://prviewacc.purview.azure.com"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"

# ── Workspace IDs ──
HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"

# ── Formatting ──
G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; C = "\033[96m"
B = "\033[94m"; D = "\033[2m"; BOLD = "\033[1m"; RST = "\033[0m"

_token_cache = {}


def get_headers():
    if "purview" not in _token_cache or _token_cache["purview"][1] < time.time() - 2400:
        token = cred.get_token("https://purview.azure.net/.default")
        _token_cache["purview"] = (token.token, time.time())
    return {"Authorization": f"Bearer {_token_cache['purview'][0]}",
            "Content-Type": "application/json"}


def hdr(title):
    print(f"\n{BOLD}{B}{'═' * 70}")
    print(f"  {title}")
    print(f"{'═' * 70}{RST}")


def ok(msg):
    print(f"  {G}✓{RST} {msg}")


def warn(msg):
    print(f"  {Y}⚠{RST} {msg}")


def info(msg):
    print(f"  {D}·{RST} {msg}")


# ══════════════════════════════════════════════════════════════════════
# HELPERS: Search & resolve entities
# ══════════════════════════════════════════════════════════════════════

def search_entities(h, keywords, entity_type=None, limit=50):
    """Search Purview catalog and return list of (name, guid, qualifiedName, typeName)."""
    body = {"keywords": keywords, "limit": limit}
    if entity_type:
        body["filter"] = {"entityType": entity_type}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code != 200:
        return []
    results = []
    for ent in r.json().get("value", []):
        results.append({
            "name": ent.get("name", ""),
            "guid": ent.get("id", ""),
            "qualifiedName": ent.get("qualifiedName", ""),
            "typeName": ent.get("entityType", ""),
        })
    return results


def resolve_entity(h, name_or_qname, entity_type=None):
    """Find entity GUID and qualifiedName by name or qualifiedName."""
    results = search_entities(h, name_or_qname, entity_type)
    for r in results:
        if (r["name"].lower() == name_or_qname.lower() or
                name_or_qname.lower() in r["qualifiedName"].lower()):
            return r
    return None


def build_entity_map(h):
    """Build comprehensive entity map: name → {guid, qualifiedName, typeName}."""
    entity_map = {}

    # SQL tables & views
    for etype in ["azure_sql_table", "azure_sql_view"]:
        for ent in search_entities(h, "*", etype, limit=100):
            entity_map[ent["name"]] = ent
        time.sleep(0.2)

    # Fabric lakehouse tables & lakehouses
    for etype in ["fabric_lakehouse_table", "fabric_lake_warehouse"]:
        for ent in search_entities(h, "*", etype, limit=100):
            entity_map[ent["name"]] = ent
        time.sleep(0.2)

    # FHIR/DICOM custom entities
    for etype in ["healthcare_fhir_service", "healthcare_fhir_resource_type",
                  "healthcare_dicom_service", "healthcare_dicom_modality"]:
        for ent in search_entities(h, "*", etype, limit=50):
            entity_map[ent["name"]] = ent
        time.sleep(0.2)

    return entity_map


# ══════════════════════════════════════════════════════════════════════
# CORE: Create Process entity (lineage link)
# ══════════════════════════════════════════════════════════════════════

def create_process_entity(h, source_qname, target_qname, process_name,
                          column_mappings=None, source_guid=None, target_guid=None):
    """
    Create a Process entity in Purview to represent lineage.

    Uses GUID references when available to prevent accidental entity creation.
    Falls back to qualifiedName-based references otherwise.
    """
    # Build process qualifiedName
    src_short = source_qname.split("/")[-1] if "/" in source_qname else source_qname.split(".")[-1]
    tgt_short = target_qname.split("/")[-1] if "/" in target_qname else target_qname.split(".")[-1]
    process_qname = f"hca_lineage_process://{process_name.replace(' ', '_')}_{src_short}_to_{tgt_short}"

    # Source reference
    if source_guid:
        source_ref = {"typeName": "DataSet", "guid": source_guid}
    else:
        source_ref = {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": source_qname}}

    # Target reference
    if target_guid:
        target_ref = {"typeName": "DataSet", "guid": target_guid}
    else:
        target_ref = {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": target_qname}}

    # Build entity payload
    process_entity = {
        "entities": [{
            "typeName": "Process",
            "attributes": {
                "qualifiedName": process_qname,
                "name": process_name,
                "description": f"Lineage: {src_short} → {tgt_short}",
                "inputs": [source_ref],
                "outputs": [target_ref],
            },
            "guid": "-1",
        }]
    }

    # Add column mapping if provided
    if column_mappings:
        mapping_json = json.dumps([{
            "DatasetMapping": {"Source": source_qname, "Sink": target_qname},
            "ColumnMapping": column_mappings,
        }])
        process_entity["entities"][0]["attributes"]["columnMapping"] = mapping_json

    # POST to create
    url = f"{DATAMAP}/entity/bulk"
    r = sess.post(url, headers=h, json=process_entity, timeout=30)

    if r.status_code in (200, 201):
        result = r.json()
        process_guid = result.get("guidAssignments", {}).get("-1")
        if process_guid:
            ok(f"{process_name}: {src_short} → {tgt_short} (guid={process_guid[:12]}...)")
        else:
            # Entity already existed — mutatedEntities contains the update
            mutated = result.get("mutatedEntities", {})
            updated = mutated.get("UPDATE", mutated.get("CREATE", []))
            if updated:
                ok(f"{process_name}: {src_short} → {tgt_short} (uppdaterad)")
            else:
                ok(f"{process_name}: {src_short} → {tgt_short}")
        return True
    else:
        warn(f"{process_name}: {r.status_code} — {r.text[:200]}")
        return False


# ══════════════════════════════════════════════════════════════════════
# LINEAGE DEFINITIONS  (names match actual Purview catalog entities)
# ══════════════════════════════════════════════════════════════════════

# ── 1. SQL → Fabric HCA Bronze (ETL notebooks) ──
SQL_TO_BRONZE = [
    ("patients", "hca_patients", "SQL ETL: Patients → HCA Bronze",
     [{"Source": "patient_id", "Sink": "patient_id"},
      {"Source": "birth_date", "Sink": "birth_date"},
      {"Source": "gender", "Sink": "gender"},
      {"Source": "region", "Sink": "region"},
      {"Source": "ses_level", "Sink": "ses_level"},
      {"Source": "postal_code", "Sink": "postal_code"},
      {"Source": "smoking_status", "Sink": "smoking_status"},
      {"Source": "created_at", "Sink": "created_at"}]),
    ("encounters", "hca_encounters", "SQL ETL: Encounters → HCA Bronze",
     [{"Source": "encounter_id", "Sink": "encounter_id"},
      {"Source": "patient_id", "Sink": "patient_id"},
      {"Source": "admission_date", "Sink": "admission_date"},
      {"Source": "discharge_date", "Sink": "discharge_date"},
      {"Source": "department", "Sink": "department"},
      {"Source": "los_days", "Sink": "los_days"},
      {"Source": "readmission_30d", "Sink": "readmission_30d"}]),
    ("diagnoses", "hca_diagnoses", "SQL ETL: Diagnoses → HCA Bronze",
     [{"Source": "diagnosis_id", "Sink": "diagnosis_id"},
      {"Source": "encounter_id", "Sink": "encounter_id"},
      {"Source": "icd10_code", "Sink": "icd10_code"},
      {"Source": "icd10_description", "Sink": "icd10_description"},
      {"Source": "diagnosis_type", "Sink": "diagnosis_type"},
      {"Source": "confirmed_date", "Sink": "confirmed_date"}]),
    ("vitals_labs", "hca_vitals_labs", "SQL ETL: Vitals/Labs → HCA Bronze",
     [{"Source": "measurement_id", "Sink": "measurement_id"},
      {"Source": "encounter_id", "Sink": "encounter_id"},
      {"Source": "systolic_bp", "Sink": "systolic_bp"},
      {"Source": "glucose_mmol", "Sink": "glucose_mmol"},
      {"Source": "creatinine_umol", "Sink": "creatinine_umol"},
      {"Source": "hemoglobin_g", "Sink": "hemoglobin_g"},
      {"Source": "bmi", "Sink": "bmi"}]),
    ("medications", "hca_medications", "SQL ETL: Medications → HCA Bronze",
     [{"Source": "medication_id", "Sink": "medication_id"},
      {"Source": "encounter_id", "Sink": "encounter_id"},
      {"Source": "atc_code", "Sink": "atc_code"},
      {"Source": "drug_name", "Sink": "drug_name"},
      {"Source": "dose_mg", "Sink": "dose_mg"},
      {"Source": "route", "Sink": "route"}]),
]

# ── 2. FHIR Server → Fabric FHIR Bronze (FHIR ingestion) ──
FHIR_TO_BRONZE = [
    ("BrainChild FHIR Server (R4)", "fhir_bronze_patient", "FHIR Ingest: Patient",
     [{"Source": "resource", "Sink": "resource_json"}]),
    ("BrainChild FHIR Server (R4)", "fhir_bronze_observation", "FHIR Ingest: Observation",
     [{"Source": "resource", "Sink": "resource_json"}]),
    ("BrainChild FHIR Server (R4)", "fhir_bronze_specimen", "FHIR Ingest: Specimen",
     [{"Source": "resource", "Sink": "resource_json"}]),
    ("BrainChild FHIR Server (R4)", "fhir_bronze_diagnosticreport", "FHIR Ingest: DiagnosticReport",
     [{"Source": "resource", "Sink": "resource_json"}]),
    ("BrainChild FHIR Server (R4)", "fhir_bronze_imagingstudy", "FHIR Ingest: ImagingStudy",
     [{"Source": "resource", "Sink": "resource_json"}]),
]

# ── 3. FHIR Bronze → Silver (notebook transformation) ──
FHIR_BRONZE_TO_SILVER = [
    ("fhir_bronze_patient", "silver_patient", "Transform: FHIR Patient → Silver",
     [{"Source": "resource_json", "Sink": "patient_id"},
      {"Source": "resource_json", "Sink": "gender"},
      {"Source": "resource_json", "Sink": "birth_date"},
      {"Source": "resource_json", "Sink": "family_name"},
      {"Source": "resource_json", "Sink": "given_name"}]),
    ("fhir_bronze_observation", "silver_specimen", "Transform: FHIR Specimen → Silver",
     [{"Source": "resource_json", "Sink": "specimen_id"},
      {"Source": "resource_json", "Sink": "patient_ref"},
      {"Source": "resource_json", "Sink": "specimen_type"}]),
    ("fhir_bronze_imagingstudy", "silver_imaging_study", "Transform: FHIR ImagingStudy → Silver",
     [{"Source": "resource_json", "Sink": "study_id"},
      {"Source": "resource_json", "Sink": "patient_ref"},
      {"Source": "resource_json", "Sink": "modality"}]),
]

# ── 4. HCA Bronze → OMOP Gold (OMOP CDM transformation) ──
HCA_TO_OMOP = [
    ("hca_patients", "person", "OMOP Transform: Patients → Person",
     [{"Source": "patient_id", "Sink": "person_source_value"},
      {"Source": "birth_date", "Sink": "year_of_birth"},
      {"Source": "gender", "Sink": "gender_source_value"}]),
    ("hca_encounters", "visit_occurrence", "OMOP Transform: Encounters → Visit",
     [{"Source": "encounter_id", "Sink": "visit_source_value"},
      {"Source": "admission_date", "Sink": "visit_start_date"},
      {"Source": "discharge_date", "Sink": "visit_end_date"}]),
    ("hca_diagnoses", "condition_occurrence", "OMOP Transform: Diagnoses → Condition",
     [{"Source": "icd10_code", "Sink": "condition_source_value"},
      {"Source": "encounter_id", "Sink": "visit_occurrence_id"}]),
    ("hca_vitals_labs", "measurement", "OMOP Transform: Vitals → Measurement",
     [{"Source": "measurement_id", "Sink": "measurement_source_value"},
      {"Source": "encounter_id", "Sink": "visit_occurrence_id"}]),
    ("hca_medications", "drug_exposure", "OMOP Transform: Medications → Drug Exposure",
     [{"Source": "atc_code", "Sink": "drug_source_value"},
      {"Source": "encounter_id", "Sink": "visit_occurrence_id"}]),
]

# ── 5. Silver → Gold ML Features ──
SILVER_TO_GOLD = [
    ("silver_patient", "ml_features", "ML Feature: Patient → Features",
     [{"Source": "patient_id", "Sink": "patient_id"},
      {"Source": "gender", "Sink": "gender"},
      {"Source": "birth_date", "Sink": "age_at_admission"}]),
    ("silver_encounter", "ml_features", "ML Feature: Encounter → Features",
     [{"Source": "encounter_id", "Sink": "encounter_id"},
      {"Source": "period_start", "Sink": "admission_date"},
      {"Source": "period_end", "Sink": "discharge_date"}]),
    ("ml_features", "ml_predictions", "ML Pipeline: Features → Predictions",
     [{"Source": "encounter_id", "Sink": "encounter_id" },
      {"Source": "cci_score", "Sink": "cci_score"},
      {"Source": "age_at_admission", "Sink": "age_at_admission"},
      {"Source": "gender", "Sink": "gender"}]),
    ("ml_features", "vw_ml_encounters", "ML Feature: Fabric → SQL Gold View",
     [{"Source": "encounter_id", "Sink": "encounter_id"},
      {"Source": "patient_id", "Sink": "patient_id"},
      {"Source": "los_days", "Sink": "los_days"},
      {"Source": "age_at_admission", "Sink": "age_at_admission"}]),
]

# ── 6. DICOM Server → BrainChild Bronze → Silver ──
DICOM_LINEAGE = [
    ("BrainChild DICOM Server", "brainchild_bronze_dicom_study", "DICOM Ingest: Studies",
     [{"Source": "study", "Sink": "study_instance_uid"},
      {"Source": "patient", "Sink": "patient_id"},
      {"Source": "modality", "Sink": "modality"}]),
    ("BrainChild DICOM Server", "brainchild_bronze_dicom_instance", "DICOM Ingest: Instances",
     [{"Source": "instance", "Sink": "sop_instance_uid"},
      {"Source": "series", "Sink": "series_instance_uid"},
      {"Source": "modality", "Sink": "modality"}]),
    ("BrainChild DICOM Server", "brainchild_bronze_dicom_series", "DICOM Ingest: Series",
     [{"Source": "series", "Sink": "series_instance_uid"},
      {"Source": "modality", "Sink": "modality"},
      {"Source": "body_part", "Sink": "body_part_examined"}]),
    ("brainchild_bronze_dicom_study", "brainchild_silver_dicom_studies", "Transform: DICOM Study → Silver",
     [{"Source": "study_instance_uid", "Sink": "study_instance_uid"},
      {"Source": "patient_id", "Sink": "patient_id"},
      {"Source": "modality", "Sink": "modality"},
      {"Source": "study_date", "Sink": "study_date"}]),
    ("brainchild_bronze_dicom_instance", "brainchild_silver_dicom_pathology", "Transform: DICOM Instance → Pathology Silver",
     [{"Source": "sop_instance_uid", "Sink": "sop_instance_uid"},
      {"Source": "modality", "Sink": "modality"},
      {"Source": "rows", "Sink": "rows"},
      {"Source": "columns", "Sink": "columns"}]),
    ("brainchild_bronze_dicom_series", "brainchild_silver_dicom_series", "Transform: DICOM Series → Silver",
     [{"Source": "series_instance_uid", "Sink": "series_instance_uid"},
      {"Source": "modality", "Sink": "modality"},
      {"Source": "body_part_examined", "Sink": "body_part_examined"}]),
]

# ── 7. Knowledge Graph: Bronze → Silver → Gold ──
KNOWLEDGE_GRAPH = [
    ("bronze_diseases", "silver_diseases", "KG Transform: Diseases → Silver",
     [{"Source": "node_id", "Sink": "node_id"},
      {"Source": "name", "Sink": "name"},
      {"Source": "icd10", "Sink": "icd10"},
      {"Source": "snomed", "Sink": "snomed"}]),
    ("bronze_drugs", "silver_drugs", "KG Transform: Drugs → Silver",
     [{"Source": "node_id", "Sink": "node_id"},
      {"Source": "name", "Sink": "name"},
      {"Source": "atc", "Sink": "atc"},
      {"Source": "drug_class", "Sink": "drug_class"}]),
    ("bronze_guidelines", "silver_guidelines", "KG Transform: Guidelines → Silver",
     [{"Source": "node_id", "Sink": "node_id"},
      {"Source": "rec_class", "Sink": "rec_class"},
      {"Source": "evidence_level", "Sink": "evidence_level"}]),
    ("silver_diseases", "gold_medical_nodes", "KG Gold: Diseases → Medical Nodes",
     [{"Source": "node_id", "Sink": "node_id"},
      {"Source": "name", "Sink": "name"}]),
    ("silver_drugs", "gold_medical_nodes", "KG Gold: Drugs → Medical Nodes",
     [{"Source": "node_id", "Sink": "node_id"},
      {"Source": "name", "Sink": "name"}]),
    ("silver_guidelines", "gold_medical_nodes", "KG Gold: Guidelines → Medical Nodes",
     [{"Source": "node_id", "Sink": "node_id"},
      {"Source": "rec_class", "Sink": "rec_class"}]),
]


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{BOLD}{B}{'═' * 70}")
    print(f"  PURVIEW LINEAGE SETUP")
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 70}{RST}")

    h = get_headers()

    # Build entity map
    hdr("1. INVENTERA ENTITETER")
    entity_map = build_entity_map(h)
    info(f"Hittade {len(entity_map)} entiteter i katalogen")
    for name, ent in sorted(entity_map.items()):
        info(f"  {name} ({ent['typeName']})")

    created = 0
    failed = 0

    # Helper to process a lineage group (all use 4-tuples now)
    def run_group(step_num, title, flows):
        nonlocal created, failed
        hdr(f"{step_num}. LINEAGE: {title}")
        for src_name, tgt_name, proc_name, col_map in flows:
            src = entity_map.get(src_name)
            tgt = entity_map.get(tgt_name)
            if not src:
                warn(f"Källa saknas: {src_name}")
                failed += 1
                continue
            if not tgt:
                warn(f"Mål saknas: {tgt_name}")
                failed += 1
                continue
            if create_process_entity(h, src["qualifiedName"], tgt["qualifiedName"],
                                      proc_name, col_map, src["guid"], tgt["guid"]):
                created += 1
            else:
                failed += 1
            time.sleep(0.3)

    run_group(2, "SQL → Fabric HCA Bronze", SQL_TO_BRONZE)
    run_group(3, "FHIR Server → FHIR Bronze", FHIR_TO_BRONZE)
    run_group(4, "FHIR Bronze → Silver", FHIR_BRONZE_TO_SILVER)
    run_group(5, "HCA Bronze → OMOP Gold", HCA_TO_OMOP)
    run_group(6, "Silver → Gold / ML", SILVER_TO_GOLD)
    run_group(7, "DICOM Server → BrainChild", DICOM_LINEAGE)
    run_group(8, "Knowledge Graph Pipeline", KNOWLEDGE_GRAPH)

    # Summary
    hdr("SAMMANFATTNING")
    print(f"  {G}Skapade: {created}{RST}")
    if failed:
        print(f"  {Y}Misslyckade: {failed}{RST}")
    print(f"  {D}Totala lineage-flöden: {created + failed}{RST}")

    return created, failed


if __name__ == "__main__":
    main()
