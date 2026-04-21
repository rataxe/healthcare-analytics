"""
tests/test_data_generation.py
Enhetstester för syntetisk datagenerering.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import pytest
from src.python.generate_synthetic_data import (
    generate_patients,
    generate_encounters,
    generate_diagnoses,
    generate_vitals_labs,
    generate_medications,
    _compute_los,
    _compute_readmission_risk,
    ICD10_CATALOG,
)


def test_generate_patients_count():
    df = generate_patients(100)
    assert len(df) == 100


def test_patients_schema():
    df = generate_patients(50)
    required = {"patient_id", "birth_date", "gender", "ses_level", "postal_code", "region"}
    assert required.issubset(df.columns)


def test_gender_values():
    df = generate_patients(500)
    assert set(df["gender"].unique()).issubset({"M", "F", "X"})


def test_ses_level_range():
    df = generate_patients(500)
    assert df["ses_level"].between(1, 3).all()


def test_los_positive():
    """LOS ska alltid vara minst 1 dag."""
    diags = [ICD10_CATALOG[0]]
    for _ in range(50):
        los = _compute_los(age=70, diagnoses=diags, department="Kardiologi")
        assert los >= 1, f"LOS var {los}"


def test_los_elderly_higher():
    """Äldre patienter ska i genomsnitt ha längre LOS."""
    diags = [ICD10_CATALOG[0]]
    young_los = [_compute_los(30, diags, "Akutmedicin") for _ in range(200)]
    old_los   = [_compute_los(85, diags, "Akutmedicin") for _ in range(200)]
    assert sum(old_los) / len(old_los) > sum(young_los) / len(young_los)


def test_readmission_high_risk_profile():
    """Patienter med hjärtsvikt + hög ålder + låg SES ska ha hög readmission."""
    heart_failure = [d for d in ICD10_CATALOG if d[0] == "I50.9"]
    risks = [
        _compute_readmission_risk(age=85, diagnoses=heart_failure, los=15, ses_level=1)
        for _ in range(100)
    ]
    # Minst 40% ska flaggas som återinskrivna
    assert sum(risks) / len(risks) >= 0.40, f"Risk för låg: {sum(risks)/len(risks):.2%}"


def test_encounters_has_los():
    patients = generate_patients(50)
    encounters = generate_encounters(patients, encounters_per_patient=1.0)
    assert (encounters["los_days"] > 0).all()


def test_diagnoses_icd10_format():
    """ICD-10-koder ska matcha känt format."""
    patients = generate_patients(20)
    encounters = generate_encounters(patients)
    diagnoses = generate_diagnoses(encounters)
    known_codes = {d[0] for d in ICD10_CATALOG}
    assert set(diagnoses["icd10_code"].unique()).issubset(known_codes)


def test_full_pipeline_integrity():
    """Referensintegritet: alla encounter_id i diagnoses ska finnas i encounters."""
    patients = generate_patients(50)
    encounters = generate_encounters(patients)
    diagnoses = generate_diagnoses(encounters)
    vitals = generate_vitals_labs(encounters)
    encounters_clean = encounters.drop(columns=["_diagnoses"])

    enc_ids = set(encounters_clean["encounter_id"])
    assert set(diagnoses["encounter_id"]).issubset(enc_ids)
    assert set(vitals["encounter_id"]).issubset(enc_ids)
