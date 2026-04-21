"""Create OMOP gold lakehouse, upload transformation notebook, and deploy semantic model.

Skapar:
  1. gold_omop lakehouse i Fabric-workspacet
  2. Laddar upp 04_omop_transformation-notebooken
  3. Skapar en OMOP CDM v5.4 semantisk modell (Power BI dataset)

Referens: https://learn.microsoft.com/en-us/industry/healthcare/
           healthcare-data-solutions/omop-transformations-overview

Semantisk modell-tabeller (per Microsoft HDS docs):
  Person, Visit_Occurrence, Condition_Occurrence, Drug_Exposure,
  Measurement, Observation, Location + Concept (lookup)

Relationer:
  Person 1→* Visit_Occurrence (person_id)
  Person 1→* Condition_Occurrence (person_id)
  Person 1→* Drug_Exposure (person_id)
  Person 1→* Measurement (person_id)
  Person 1→* Observation (person_id)
  Person *→1 Location (location_id)
  Visit_Occurrence 1→* Condition_Occurrence (visit_occurrence_id)
  Visit_Occurrence 1→* Drug_Exposure (visit_occurrence_id)
  Visit_Occurrence 1→* Measurement (visit_occurrence_id)
"""
import base64
import json
import logging
import time

import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
BASE_URL = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}"


def get_token():
    cred = AzureCliCredential()
    return cred.get_token("https://api.fabric.microsoft.com/.default").token


def headers(token):
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def poll_operation(url, hdrs, timeout=120):
    for _ in range(timeout // 2):
        time.sleep(2)
        resp = requests.get(url, headers=hdrs)
        if resp.status_code == 200:
            body = resp.json() if resp.text else {}
            status = body.get("status", "Succeeded")
            if status in ("Succeeded", "Completed"):
                return body
            if status == "Failed":
                log.error("Operation failed: %s", body)
                return None
    log.warning("Operation timeout")
    return None


# ── 1. Skapa gold_omop lakehouse ─────────────────────────────────────────────
def create_lakehouse(token):
    """Skapa gold_omop lakehouse om den inte redan finns."""
    # Kolla om den redan finns
    resp = requests.get(f"{BASE_URL}/lakehouses", headers=headers(token))
    if resp.status_code == 200:
        for lh in resp.json().get("value", []):
            if lh["displayName"] == "gold_omop":
                log.info("✅ gold_omop lakehouse finns redan: %s", lh["id"])
                return lh["id"]

    # Skapa ny
    payload = {"displayName": "gold_omop", "type": "Lakehouse"}
    resp = requests.post(f"{BASE_URL}/items", headers=headers(token), json=payload)
    if resp.status_code in (200, 201):
        lh_id = resp.json()["id"]
        log.info("✅ Skapade gold_omop lakehouse: %s", lh_id)
        return lh_id
    elif resp.status_code == 202:
        location = resp.headers.get("Location")
        if location:
            result = poll_operation(location, headers(token))
            if result:
                lh_id = result.get("id", "unknown")
                log.info("✅ Skapade gold_omop lakehouse: %s", lh_id)
                return lh_id
    else:
        log.error("❌ Kunde inte skapa lakehouse: %d — %s", resp.status_code, resp.text[:300])
    return None


# ── 2. Ladda upp notebook ────────────────────────────────────────────────────
def py_to_ipynb(py_path):
    """Convert a .py notebook file to ipynb JSON format."""
    from pathlib import Path
    code = Path(py_path).read_text(encoding="utf-8")
    cells = []
    current_lines = []
    for line in code.splitlines():
        if line.startswith("# ── CELL") or line.startswith("# ── PARAMETERCELL"):
            if current_lines:
                cells.append("\n".join(current_lines))
                current_lines = []
        current_lines.append(line)
    if current_lines:
        cells.append("\n".join(current_lines))

    nb_cells = []
    for cell_code in cells:
        # source must be a list of strings (one per line, each ending with \n)
        source_lines = [line + "\n" for line in cell_code.splitlines()]
        if source_lines:
            source_lines[-1] = source_lines[-1].rstrip("\n")  # last line no trailing newline
        nb_cells.append({
            "cell_type": "code",
            "source": source_lines,
            "metadata": {},
            "outputs": [],
            "execution_count": None,
        })

    return {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
            "kernelspec": {
                "name": "synapse_pyspark",
                "display_name": "Synapse PySpark",
            },
        },
        "cells": nb_cells,
    }


def create_and_upload_notebook(token, lakehouse_id):
    """Skapa 04_omop_transformation notebook i Fabric."""
    from pathlib import Path

    nb_path = Path(__file__).resolve().parent.parent / "src" / "notebooks" / "04_omop_transformation.py"
    if not nb_path.exists():
        log.error("Notebook-fil saknas: %s", nb_path)
        return None

    ipynb = py_to_ipynb(nb_path)
    ipynb_json = json.dumps(ipynb, ensure_ascii=False)
    content_b64 = base64.b64encode(ipynb_json.encode("utf-8")).decode("ascii")

    # Kolla om notebooken redan finns
    resp = requests.get(f"{BASE_URL}/items?type=Notebook", headers=headers(token))
    nb_id = None
    if resp.status_code == 200:
        for item in resp.json().get("value", []):
            if item["displayName"] == "04_omop_transformation":
                nb_id = item["id"]
                log.info("Notebook finns redan: %s", nb_id)
                break

    if nb_id:
        # Uppdatera befintlig
        url = f"{BASE_URL}/items/{nb_id}/updateDefinition"
        payload = {
            "definition": {
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": content_b64,
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }
        resp = requests.post(url, headers=headers(token), json=payload)
        if resp.status_code in (200, 202):
            log.info("✅ Uppdaterade notebook: %s", nb_id)
        else:
            log.error("❌ Kunde inte uppdatera notebook: %d — %s", resp.status_code, resp.text[:300])
    else:
        # Skapa ny
        payload = {
            "displayName": "04_omop_transformation",
            "type": "Notebook",
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebook-content.py",
                        "payload": content_b64,
                        "payloadType": "InlineBase64",
                    }
                ]
            }
        }
        resp = requests.post(f"{BASE_URL}/items", headers=headers(token), json=payload)
        if resp.status_code in (200, 201):
            nb_id = resp.json()["id"]
            log.info("✅ Skapade notebook: %s", nb_id)
        elif resp.status_code == 202:
            location = resp.headers.get("Location")
            if location:
                result = poll_operation(location, headers(token))
                log.info("✅ Notebook skapad (async)")
                # Hämta notebook-ID via lista
                resp2 = requests.get(f"{BASE_URL}/items?type=Notebook", headers=headers(token))
                if resp2.status_code == 200:
                    for item in resp2.json().get("value", []):
                        if item["displayName"] == "04_omop_transformation":
                            nb_id = item["id"]
                            break
        else:
            log.error("❌ Kunde inte skapa notebook: %d — %s", resp.status_code, resp.text[:300])

    return nb_id


# ── 3. Skapa OMOP Semantisk Modell (TMSL/TMDL) ─────────────────────────────
def create_semantic_model(token, lakehouse_id):
    """Skapa OMOP CDM v5.4 semantisk modell baserad på gold_omop lakehouse."""

    # Kolla om modellen redan finns
    resp = requests.get(f"{BASE_URL}/items?type=SemanticModel", headers=headers(token))
    sm_id = None
    if resp.status_code == 200:
        for item in resp.json().get("value", []):
            if item["displayName"] == "OMOP_CDM_Semantic_Model":
                sm_id = item["id"]
                log.info("Semantisk modell finns redan: %s", sm_id)
                break

    # TMSL model definition
    model_bim = {
        "compatibilityLevel": 1604,
        "model": {
            "name": "OMOP_CDM_v54",
            "culture": "sv-SE",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "tables": [
                _table_person(),
                _table_visit_occurrence(),
                _table_condition_occurrence(),
                _table_drug_exposure(),
                _table_measurement(),
                _table_observation(),
                _table_location(),
                _table_observation_period(),
                _table_concept(),
            ],
            "relationships": _relationships(),
            "annotations": [
                {
                    "name": "PBI_QueryOrder",
                    "value": json.dumps([
                        "person", "visit_occurrence", "condition_occurrence",
                        "drug_exposure", "measurement", "observation",
                        "location", "observation_period", "concept"
                    ])
                },
                {
                    "name": "__PBI_TimeIntelligenceEnabled",
                    "value": "0"
                }
            ]
        }
    }

    model_json = json.dumps(model_bim, ensure_ascii=False)
    model_b64 = base64.b64encode(model_json.encode("utf-8")).decode("ascii")

    # definition.pbism krävs av Fabric — minimal version
    pbism = {"version": "1.0"}
    pbism_b64 = base64.b64encode(json.dumps(pbism).encode("utf-8")).decode("ascii")

    definition = {
        "parts": [
            {
                "path": "definition.pbism",
                "payload": pbism_b64,
                "payloadType": "InlineBase64",
            },
            {
                "path": "model.bim",
                "payload": model_b64,
                "payloadType": "InlineBase64",
            }
        ]
    }

    if sm_id:
        url = f"{BASE_URL}/items/{sm_id}/updateDefinition"
        payload = {"definition": definition}
        resp = requests.post(url, headers=headers(token), json=payload)
        if resp.status_code in (200, 202):
            log.info("✅ Uppdaterade semantisk modell: %s", sm_id)
            if resp.status_code == 202:
                location = resp.headers.get("Location")
                if location:
                    poll_operation(location, headers(token))
        else:
            log.error("❌ Uppdatering misslyckades: %d — %s", resp.status_code, resp.text[:500])
    else:
        payload = {
            "displayName": "OMOP_CDM_Semantic_Model",
            "type": "SemanticModel",
            "definition": definition,
        }
        resp = requests.post(f"{BASE_URL}/items", headers=headers(token), json=payload)
        if resp.status_code in (200, 201):
            sm_id = resp.json()["id"]
            log.info("✅ Skapade semantisk modell: %s", sm_id)
        elif resp.status_code == 202:
            location = resp.headers.get("Location")
            if location:
                result = poll_operation(location, headers(token))
                if result:
                    sm_id = result.get("id", "unknown")
            log.info("✅ Semantisk modell skapad (async): %s", sm_id)
        else:
            log.error("❌ Kunde inte skapa semantisk modell: %d — %s", resp.status_code, resp.text[:500])

    return sm_id


# ── TMSL Table Definitions ───────────────────────────────────────────────────

def _lakehouse_expression(table_name):
    """M-expression som läser en Delta-tabell från gold_omop lakehouse."""
    return (
        f'let\n'
        f'    Source = Lakehouse.Contents(null){{[workspaceName="Healthcare-Analytics"]}}[Data],\n'
        f'    gold_omop = Source{{[lakehouseName="gold_omop"]}}[Data],\n'
        f'    #"{table_name}" = gold_omop{{[Id="{table_name}", ItemKind="Table"]}}[Data]\n'
        f'in\n'
        f'    #"{table_name}"'
    )


def _table_person():
    return {
        "name": "person",
        "columns": [
            {"name": "person_id",                    "dataType": "int64",    "isHidden": False, "sourceColumn": "person_id"},
            {"name": "gender_concept_id",            "dataType": "int64",    "sourceColumn": "gender_concept_id"},
            {"name": "year_of_birth",                "dataType": "int64",    "sourceColumn": "year_of_birth"},
            {"name": "month_of_birth",               "dataType": "int64",    "sourceColumn": "month_of_birth"},
            {"name": "day_of_birth",                 "dataType": "int64",    "sourceColumn": "day_of_birth"},
            {"name": "birth_datetime",               "dataType": "dateTime", "sourceColumn": "birth_datetime"},
            {"name": "race_concept_id",              "dataType": "int64",    "sourceColumn": "race_concept_id"},
            {"name": "ethnicity_concept_id",         "dataType": "int64",    "sourceColumn": "ethnicity_concept_id"},
            {"name": "location_id",                  "dataType": "int64",    "sourceColumn": "location_id"},
            {"name": "provider_id",                  "dataType": "int64",    "sourceColumn": "provider_id"},
            {"name": "care_site_id",                 "dataType": "int64",    "sourceColumn": "care_site_id"},
            {"name": "person_source_value",          "dataType": "string",   "sourceColumn": "person_source_value"},
            {"name": "gender_source_value",          "dataType": "string",   "sourceColumn": "gender_source_value"},
        ],
        "partitions": [{
            "name": "person_partition",
            "mode": "import",
            "source": {"type": "m", "expression": _lakehouse_expression("person")}
        }],
        "measures": [
            {"name": "Antal patienter", "expression": "COUNTROWS(person)"},
            {"name": "Medelålder",      "expression": 'AVERAGEX(person, YEAR(TODAY()) - [year_of_birth])'},
        ]
    }


def _table_visit_occurrence():
    return {
        "name": "visit_occurrence",
        "columns": [
            {"name": "visit_occurrence_id",          "dataType": "int64",    "sourceColumn": "visit_occurrence_id"},
            {"name": "person_id",                    "dataType": "int64",    "sourceColumn": "person_id"},
            {"name": "visit_concept_id",             "dataType": "int64",    "sourceColumn": "visit_concept_id"},
            {"name": "visit_start_date",             "dataType": "dateTime", "sourceColumn": "visit_start_date"},
            {"name": "visit_start_datetime",         "dataType": "dateTime", "sourceColumn": "visit_start_datetime"},
            {"name": "visit_end_date",               "dataType": "dateTime", "sourceColumn": "visit_end_date"},
            {"name": "visit_end_datetime",           "dataType": "dateTime", "sourceColumn": "visit_end_datetime"},
            {"name": "visit_type_concept_id",        "dataType": "int64",    "sourceColumn": "visit_type_concept_id"},
            {"name": "visit_source_value",           "dataType": "string",   "sourceColumn": "visit_source_value"},
            {"name": "admitted_from_source_value",   "dataType": "string",   "sourceColumn": "admitted_from_source_value"},
            {"name": "discharged_to_source_value",   "dataType": "string",   "sourceColumn": "discharged_to_source_value"},
        ],
        "partitions": [{
            "name": "visit_occurrence_partition",
            "mode": "import",
            "source": {"type": "m", "expression": _lakehouse_expression("visit_occurrence")}
        }],
        "measures": [
            {"name": "Antal besök",          "expression": "COUNTROWS(visit_occurrence)"},
            {"name": "Medel vårdtid (dagar)", "expression": 'AVERAGEX(visit_occurrence, DATEDIFF([visit_start_date], [visit_end_date], DAY))'},
        ]
    }


def _table_condition_occurrence():
    return {
        "name": "condition_occurrence",
        "columns": [
            {"name": "condition_occurrence_id",      "dataType": "int64",    "sourceColumn": "condition_occurrence_id"},
            {"name": "person_id",                    "dataType": "int64",    "sourceColumn": "person_id"},
            {"name": "condition_concept_id",         "dataType": "int64",    "sourceColumn": "condition_concept_id"},
            {"name": "condition_start_date",         "dataType": "dateTime", "sourceColumn": "condition_start_date"},
            {"name": "condition_end_date",           "dataType": "dateTime", "sourceColumn": "condition_end_date"},
            {"name": "condition_type_concept_id",    "dataType": "int64",    "sourceColumn": "condition_type_concept_id"},
            {"name": "visit_occurrence_id",          "dataType": "int64",    "sourceColumn": "visit_occurrence_id"},
            {"name": "condition_source_value",       "dataType": "string",   "sourceColumn": "condition_source_value"},
            {"name": "condition_status_source_value","dataType": "string",   "sourceColumn": "condition_status_source_value"},
        ],
        "partitions": [{
            "name": "condition_occurrence_partition",
            "mode": "import",
            "source": {"type": "m", "expression": _lakehouse_expression("condition_occurrence")}
        }],
        "measures": [
            {"name": "Antal diagnoser", "expression": "COUNTROWS(condition_occurrence)"},
            {"name": "Unika diagnoser", "expression": "DISTINCTCOUNT(condition_occurrence[condition_source_value])"},
        ]
    }


def _table_drug_exposure():
    return {
        "name": "drug_exposure",
        "columns": [
            {"name": "drug_exposure_id",             "dataType": "int64",    "sourceColumn": "drug_exposure_id"},
            {"name": "person_id",                    "dataType": "int64",    "sourceColumn": "person_id"},
            {"name": "drug_concept_id",              "dataType": "int64",    "sourceColumn": "drug_concept_id"},
            {"name": "drug_exposure_start_date",     "dataType": "dateTime", "sourceColumn": "drug_exposure_start_date"},
            {"name": "drug_exposure_end_date",       "dataType": "dateTime", "sourceColumn": "drug_exposure_end_date"},
            {"name": "drug_type_concept_id",         "dataType": "int64",    "sourceColumn": "drug_type_concept_id"},
            {"name": "quantity",                     "dataType": "double",   "sourceColumn": "quantity"},
            {"name": "route_concept_id",             "dataType": "int64",    "sourceColumn": "route_concept_id"},
            {"name": "visit_occurrence_id",          "dataType": "int64",    "sourceColumn": "visit_occurrence_id"},
            {"name": "drug_source_value",            "dataType": "string",   "sourceColumn": "drug_source_value"},
            {"name": "route_source_value",           "dataType": "string",   "sourceColumn": "route_source_value"},
            {"name": "dose_unit_source_value",       "dataType": "string",   "sourceColumn": "dose_unit_source_value"},
        ],
        "partitions": [{
            "name": "drug_exposure_partition",
            "mode": "import",
            "source": {"type": "m", "expression": _lakehouse_expression("drug_exposure")}
        }],
        "measures": [
            {"name": "Antal läkemedelsexponeringar", "expression": "COUNTROWS(drug_exposure)"},
            {"name": "Unika läkemedel",              "expression": "DISTINCTCOUNT(drug_exposure[drug_source_value])"},
        ]
    }


def _table_measurement():
    return {
        "name": "measurement",
        "columns": [
            {"name": "measurement_id",               "dataType": "int64",    "sourceColumn": "measurement_id"},
            {"name": "person_id",                    "dataType": "int64",    "sourceColumn": "person_id"},
            {"name": "measurement_concept_id",       "dataType": "int64",    "sourceColumn": "measurement_concept_id"},
            {"name": "measurement_date",             "dataType": "dateTime", "sourceColumn": "measurement_date"},
            {"name": "measurement_datetime",         "dataType": "dateTime", "sourceColumn": "measurement_datetime"},
            {"name": "measurement_type_concept_id",  "dataType": "int64",    "sourceColumn": "measurement_type_concept_id"},
            {"name": "value_as_number",              "dataType": "double",   "sourceColumn": "value_as_number"},
            {"name": "unit_concept_id",              "dataType": "int64",    "sourceColumn": "unit_concept_id"},
            {"name": "visit_occurrence_id",          "dataType": "int64",    "sourceColumn": "visit_occurrence_id"},
            {"name": "measurement_source_value",     "dataType": "string",   "sourceColumn": "measurement_source_value"},
            {"name": "unit_source_value",            "dataType": "string",   "sourceColumn": "unit_source_value"},
        ],
        "partitions": [{
            "name": "measurement_partition",
            "mode": "import",
            "source": {"type": "m", "expression": _lakehouse_expression("measurement")}
        }],
        "measures": [
            {"name": "Antal mätningar",     "expression": "COUNTROWS(measurement)"},
            {"name": "Medelvärde",          "expression": "AVERAGE(measurement[value_as_number])"},
            {"name": "Unika mättyper",      "expression": "DISTINCTCOUNT(measurement[measurement_source_value])"},
        ]
    }


def _table_observation():
    return {
        "name": "observation",
        "columns": [
            {"name": "observation_id",               "dataType": "int64",    "sourceColumn": "observation_id"},
            {"name": "person_id",                    "dataType": "int64",    "sourceColumn": "person_id"},
            {"name": "observation_concept_id",       "dataType": "int64",    "sourceColumn": "observation_concept_id"},
            {"name": "observation_date",             "dataType": "dateTime", "sourceColumn": "observation_date"},
            {"name": "observation_datetime",         "dataType": "dateTime", "sourceColumn": "observation_datetime"},
            {"name": "observation_type_concept_id",  "dataType": "int64",    "sourceColumn": "observation_type_concept_id"},
            {"name": "value_as_number",              "dataType": "double",   "sourceColumn": "value_as_number"},
            {"name": "value_as_string",              "dataType": "string",   "sourceColumn": "value_as_string"},
            {"name": "value_as_concept_id",          "dataType": "int64",    "sourceColumn": "value_as_concept_id"},
            {"name": "observation_source_value",     "dataType": "string",   "sourceColumn": "observation_source_value"},
        ],
        "partitions": [{
            "name": "observation_partition",
            "mode": "import",
            "source": {"type": "m", "expression": _lakehouse_expression("observation")}
        }],
        "measures": [
            {"name": "Antal observationer", "expression": "COUNTROWS(observation)"},
        ]
    }


def _table_location():
    return {
        "name": "location",
        "columns": [
            {"name": "location_id",          "dataType": "int64",    "sourceColumn": "location_id"},
            {"name": "city",                  "dataType": "string",   "sourceColumn": "city"},
            {"name": "state",                 "dataType": "string",   "sourceColumn": "state"},
            {"name": "zip",                   "dataType": "string",   "sourceColumn": "zip"},
            {"name": "county",                "dataType": "string",   "sourceColumn": "county"},
            {"name": "country_source_value",  "dataType": "string",   "sourceColumn": "country_source_value"},
        ],
        "partitions": [{
            "name": "location_partition",
            "mode": "import",
            "source": {"type": "m", "expression": _lakehouse_expression("location")}
        }],
        "measures": [
            {"name": "Antal platser", "expression": "COUNTROWS(location)"},
        ]
    }


def _table_observation_period():
    return {
        "name": "observation_period",
        "columns": [
            {"name": "observation_period_id",        "dataType": "int64",    "sourceColumn": "observation_period_id"},
            {"name": "person_id",                    "dataType": "int64",    "sourceColumn": "person_id"},
            {"name": "observation_period_start_date","dataType": "dateTime", "sourceColumn": "observation_period_start_date"},
            {"name": "observation_period_end_date",  "dataType": "dateTime", "sourceColumn": "observation_period_end_date"},
            {"name": "period_type_concept_id",       "dataType": "int64",    "sourceColumn": "period_type_concept_id"},
        ],
        "partitions": [{
            "name": "observation_period_partition",
            "mode": "import",
            "source": {"type": "m", "expression": _lakehouse_expression("observation_period")}
        }]
    }


def _table_concept():
    return {
        "name": "concept",
        "columns": [
            {"name": "concept_id",        "dataType": "int64",    "sourceColumn": "concept_id"},
            {"name": "concept_name",      "dataType": "string",   "sourceColumn": "concept_name"},
            {"name": "domain_id",         "dataType": "string",   "sourceColumn": "domain_id"},
            {"name": "vocabulary_id",     "dataType": "string",   "sourceColumn": "vocabulary_id"},
            {"name": "standard_concept",  "dataType": "string",   "sourceColumn": "standard_concept"},
            {"name": "concept_code",      "dataType": "string",   "sourceColumn": "concept_code"},
        ],
        "partitions": [{
            "name": "concept_partition",
            "mode": "import",
            "source": {"type": "m", "expression": _lakehouse_expression("concept")}
        }]
    }


def _relationships():
    """OMOP CDM relationer — star schema med person i centrum."""
    return [
        # Person ← Location (many-to-one)
        {
            "name": "person_location",
            "fromTable": "person",
            "fromColumn": "location_id",
            "toTable": "location",
            "toColumn": "location_id",
        },
        # Visit_Occurrence → Person (many-to-one)
        {
            "name": "visit_person",
            "fromTable": "visit_occurrence",
            "fromColumn": "person_id",
            "toTable": "person",
            "toColumn": "person_id",
        },
        # Condition_Occurrence → Person (inactive — traverseras via visit)
        {
            "name": "condition_person",
            "fromTable": "condition_occurrence",
            "fromColumn": "person_id",
            "toTable": "person",
            "toColumn": "person_id",
            "isActive": False,
        },
        # Condition_Occurrence → Visit_Occurrence (many-to-one)
        {
            "name": "condition_visit",
            "fromTable": "condition_occurrence",
            "fromColumn": "visit_occurrence_id",
            "toTable": "visit_occurrence",
            "toColumn": "visit_occurrence_id",
        },
        # Drug_Exposure → Person (inactive — traverseras via visit)
        {
            "name": "drug_person",
            "fromTable": "drug_exposure",
            "fromColumn": "person_id",
            "toTable": "person",
            "toColumn": "person_id",
            "isActive": False,
        },
        # Drug_Exposure → Visit_Occurrence (many-to-one)
        {
            "name": "drug_visit",
            "fromTable": "drug_exposure",
            "fromColumn": "visit_occurrence_id",
            "toTable": "visit_occurrence",
            "toColumn": "visit_occurrence_id",
        },
        # Measurement → Person (inactive — traverseras via visit)
        {
            "name": "measurement_person",
            "fromTable": "measurement",
            "fromColumn": "person_id",
            "toTable": "person",
            "toColumn": "person_id",
            "isActive": False,
        },
        # Measurement → Visit_Occurrence (many-to-one)
        {
            "name": "measurement_visit",
            "fromTable": "measurement",
            "fromColumn": "visit_occurrence_id",
            "toTable": "visit_occurrence",
            "toColumn": "visit_occurrence_id",
        },
        # Observation → Person (many-to-one)
        {
            "name": "observation_person",
            "fromTable": "observation",
            "fromColumn": "person_id",
            "toTable": "person",
            "toColumn": "person_id",
        },
        # Observation_Period → Person (many-to-one)
        {
            "name": "obs_period_person",
            "fromTable": "observation_period",
            "fromColumn": "person_id",
            "toTable": "person",
            "toColumn": "person_id",
        },
    ]


# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    token = get_token()
    log.info("Token acquired")

    # Steg 1: Skapa gold_omop lakehouse
    lh_id = create_lakehouse(token)
    if not lh_id:
        log.error("Avbryter — kunde inte skapa/hitta gold_omop lakehouse")
        return

    # Steg 2: Ladda upp notebook
    nb_id = create_and_upload_notebook(token, lh_id)
    log.info("Notebook ID: %s", nb_id)

    # Steg 3: Skapa semantisk modell
    sm_id = create_semantic_model(token, lh_id)
    log.info("Semantisk modell ID: %s", sm_id)

    print("\n=== OMOP DEPLOYMENT SUMMARY ===")
    print(f"  gold_omop lakehouse:    {lh_id}")
    print(f"  04_omop_transformation: {nb_id}")
    print(f"  OMOP_CDM_Semantic_Model:{sm_id}")
    print("\nNästa steg:")
    print("  1. Kör notebooken 04_omop_transformation i Fabric för att populera OMOP-tabellerna")
    print("  2. Öppna den semantiska modellen i Fabric och verifiera relationer")
    print("  3. Skapa Power BI-rapporter baserat på den semantiska modellen")


if __name__ == "__main__":
    main()
