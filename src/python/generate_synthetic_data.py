"""
generate_synthetic_data.py
Healthcare Analytics — LOS & Readmission Predictor
Genererar 10 000 kliniskt realistiska syntetiska patienter/encounters.

Kör: python src/python/generate_synthetic_data.py --rows 10000 --output data/raw
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

fake = Faker("sv_SE")
rng = np.random.default_rng(42)

# ── Konstanter ──────────────────────────────────────────────────────────────
DEPARTMENTS = ["Akutmedicin", "Kirurgi", "Kardiologi", "Neurologi",
               "Ortopedi", "Onkologi", "Geriatrik", "Infektion", "Psykiatri"]
ADMISSION_SOURCES = ["Akutmottagning", "Remiss", "Planerat", "Ambulans", "Överflyttning"]
DISCHARGE_DISPOSITIONS = ["Hemgång", "Hemgång_hemtjänst", "Korttidsboende",
                           "Annan_vårdenhet", "Avliden"]
SMOKING_STATUSES = ["Never", "Former", "Current", "Unknown"]

# ICD-10: (kod, beskrivning, charlson_weight, los_modifier, readmission_risk)
ICD10_CATALOG: list[tuple[str, str, int, float, float]] = [
    ("I50.9",  "Hjärtsvikt, ospecificerad",           2, 2.5, 0.30),
    ("E11.9",  "Diabetes mellitus typ 2",              1, 1.5, 0.20),
    ("J18.9",  "Pneumoni, ospecificerad",              1, 1.8, 0.15),
    ("N18.3",  "Kronisk njursjukdom, steg 3",          2, 2.0, 0.25),
    ("F32.1",  "Depressiv episod, medelsvår",          0, 3.0, 0.35),
    ("I21.9",  "Akut hjärtinfarkt, ospecificerad",    2, 3.5, 0.28),
    ("J44.1",  "KOL med akut exacerbation",            1, 2.0, 0.22),
    ("K92.1",  "Melena",                               0, 1.2, 0.10),
    ("M16.1",  "Primär koxartros, bilateral",          0, 4.0, 0.05),
    ("S72.00", "Höftfraktur",                          0, 5.0, 0.18),
    ("C34.1",  "Lungcancer, övre lob",                 6, 3.0, 0.40),
    ("I63.9",  "Hjärninfarkt, ospecificerad",          2, 4.5, 0.32),
    ("A41.9",  "Sepsis, ospecificerad",                3, 6.0, 0.45),
]

ATC_MEDICATIONS: list[tuple[str, str]] = [
    ("C09AA01", "Enalapril"),
    ("C10AA01", "Simvastatin"),
    ("A10BA02", "Metformin"),
    ("B01AC06", "Acetylsalicylsyra"),
    ("C07AB02", "Metoprolol"),
    ("N02BE01", "Paracetamol"),
    ("J01CA04", "Amoxicillin"),
    ("C03CA01", "Furosemid"),
    ("A02BC01", "Omeprazol"),
    ("N05BA01", "Diazepam"),
]


# ── Hjälpfunktioner ─────────────────────────────────────────────────────────

def _seasonal_admission_date(year_range: tuple[int, int] = (2021, 2024)) -> date:
    """Genererar datum med vinter-peak för luftvägsinfektioner."""
    start = date(year_range[0], 1, 1)
    end = date(year_range[1], 12, 31)
    delta_days = (end - start).days
    day = rng.integers(0, delta_days)
    d = start + timedelta(days=int(day))
    # Säsongsmodifierare: dec–feb = ökad volym
    if d.month in (12, 1, 2):
        if rng.random() < 0.4:  # 40% extra chans att sampla om till vinter
            winter_day = rng.integers(0, 90)
            if d.month == 12:
                d = date(d.year, 12, 1) + timedelta(days=int(winter_day) % 31)
            else:
                d = date(d.year, 1, 1) + timedelta(days=int(winter_day) % 59)
    return d


def _compute_los(
    age: int,
    diagnoses: list[tuple],
    department: str,
) -> int:
    """Beräknar LOS (dagar) baserat på ålder, diagnoser och avdelning.
    Följer Poisson-fördelning — de flesta stannar kort, ett fåtal länge.
    """
    base_lambda = 3.5  # genomsnittlig LOS

    # Åldersmodifierare
    if age > 80:
        base_lambda += 3.0
    elif age > 65:
        base_lambda += 1.5
    elif age > 50:
        base_lambda += 0.5

    # Diagnosmodifierare
    for _, _, _, los_mod, _ in diagnoses:
        base_lambda += los_mod * 0.6  # primärdiagnos väger tyngre

    # Avdelningsmodifierare
    dept_modifiers = {
        "Kirurgi": 2.0, "Ortopedi": 3.5, "Onkologi": 2.5,
        "Neurologi": 2.0, "Geriatrik": 4.0, "Infektion": 1.5,
    }
    base_lambda += dept_modifiers.get(department, 0.5)

    los = int(rng.poisson(base_lambda))
    return max(1, min(los, 90))  # clamp: 1–90 dagar


def _compute_readmission_risk(
    age: int,
    diagnoses: list[tuple],
    los: int,
    ses_level: int,
) -> bool:
    """Beräknar 30-dagars återinskrivningsrisk som binär outcome."""
    risk = 0.05  # basrisk

    if age > 75:
        risk += 0.08
    if los > 10:
        risk += 0.06
    if ses_level == 1:
        risk += 0.05  # låg SES = högre risk

    for _, _, _, _, r_risk in diagnoses:
        risk += r_risk * 0.5

    risk = min(risk, 0.85)
    return bool(rng.random() < risk)


def _add_measurement_noise(value: float | None, noise_prob: float = 0.05) -> float | None:
    """Simulerar saknade värden (missing at random)."""
    if value is None:
        return None
    if rng.random() < noise_prob:
        return None
    return value


# ── Generatorer ─────────────────────────────────────────────────────────────

def generate_patients(n: int) -> pd.DataFrame:
    log.info("Genererar %d patienter...", n)
    records = []
    for _ in range(n):
        age = int(rng.integers(18, 95))
        birth_year = date.today().year - age
        birth_date = date(birth_year, int(rng.integers(1, 13)), int(rng.integers(1, 29)))
        records.append({
            "patient_id": str(uuid.uuid4()),
            "birth_date": birth_date.isoformat(),
            "gender": rng.choice(["M", "F", "X"], p=[0.48, 0.48, 0.04]),
            "ses_level": int(rng.choice([1, 2, 3], p=[0.25, 0.50, 0.25])),
            "postal_code": fake.postcode(),
            "region": rng.choice(["Region Halland", "Region Skåne", "VGR", "Region Stockholm"], p=[0.15, 0.25, 0.35, 0.25]),
            "smoking_status": rng.choice(SMOKING_STATUSES, p=[0.45, 0.30, 0.15, 0.10]),
            "created_at": datetime.utcnow().isoformat(),
        })
    return pd.DataFrame(records)


def generate_encounters(patients_df: pd.DataFrame, encounters_per_patient: float = 1.5) -> pd.DataFrame:
    log.info("Genererar encounters...")
    records = []
    for _, patient in patients_df.iterrows():
        n_enc = max(1, int(rng.poisson(encounters_per_patient)))
        for _ in range(n_enc):
            department = str(rng.choice(DEPARTMENTS))
            admission_date = _seasonal_admission_date()
            age = date.today().year - int(patient["birth_date"][:4])

            # Välj 1–4 diagnoser
            n_diag = int(rng.choice([1, 2, 3, 4], p=[0.5, 0.3, 0.15, 0.05]))
            selected = random.sample(ICD10_CATALOG, min(n_diag, len(ICD10_CATALOG)))

            los = _compute_los(age, selected, department)
            discharge_date = admission_date + timedelta(days=los)
            readmission = _compute_readmission_risk(age, selected, los, int(patient["ses_level"]))

            encounter_id = str(uuid.uuid4())
            records.append({
                "encounter_id": encounter_id,
                "patient_id": patient["patient_id"],
                "admission_date": admission_date.isoformat(),
                "discharge_date": discharge_date.isoformat(),
                "department": department,
                "admission_source": str(rng.choice(ADMISSION_SOURCES)),
                "discharge_disposition": str(rng.choice(DISCHARGE_DISPOSITIONS, p=[0.60, 0.20, 0.10, 0.07, 0.03])),
                "los_days": los,
                "readmission_30d": int(readmission),
                "_diagnoses": selected,          # temp — tas bort vid export
                "created_at": datetime.utcnow().isoformat(),
            })
    return pd.DataFrame(records)


def generate_diagnoses(encounters_df: pd.DataFrame) -> pd.DataFrame:
    log.info("Genererar diagnoser...")
    records = []
    for _, enc in encounters_df.iterrows():
        for i, (code, desc, _, _, _) in enumerate(enc["_diagnoses"]):
            records.append({
                "diagnosis_id": str(uuid.uuid4()),
                "encounter_id": enc["encounter_id"],
                "icd10_code": code,
                "icd10_description": desc,
                "diagnosis_type": "Primary" if i == 0 else "Secondary",
                "confirmed_date": enc["admission_date"],
            })
    return pd.DataFrame(records)


def generate_vitals_labs(encounters_df: pd.DataFrame) -> pd.DataFrame:
    log.info("Genererar vitals & labs...")
    records = []
    for _, enc in encounters_df.iterrows():
        n_measurements = int(rng.integers(1, max(2, int(enc["los_days"]) // 2 + 1)))
        for j in range(n_measurements):
            measured_at = datetime.fromisoformat(enc["admission_date"]) + timedelta(hours=j * 12)
            records.append({
                "measurement_id": str(uuid.uuid4()),
                "encounter_id": enc["encounter_id"],
                "measured_at": measured_at.isoformat(),
                "systolic_bp": _add_measurement_noise(int(rng.integers(90, 180))),
                "diastolic_bp": _add_measurement_noise(int(rng.integers(55, 110))),
                "heart_rate": _add_measurement_noise(int(rng.integers(45, 130))),
                "temperature_c": _add_measurement_noise(round(float(rng.normal(37.0, 0.8)), 1)),
                "oxygen_saturation": _add_measurement_noise(round(float(rng.normal(96.5, 2.5)), 2)),
                "glucose_mmol": _add_measurement_noise(round(float(rng.normal(6.5, 2.5)), 2), noise_prob=0.08),
                "creatinine_umol": _add_measurement_noise(round(float(rng.normal(85.0, 30.0)), 2), noise_prob=0.10),
                "hemoglobin_g": _add_measurement_noise(round(float(rng.normal(12.5, 2.0)), 2)),
                "sodium_mmol": _add_measurement_noise(round(float(rng.normal(139.0, 4.0)), 2)),
                "potassium_mmol": _add_measurement_noise(round(float(rng.normal(4.1, 0.5)), 2)),
                "bmi": _add_measurement_noise(round(float(rng.normal(27.0, 5.5)), 2), noise_prob=0.12),
                "weight_kg": _add_measurement_noise(round(float(rng.normal(78.0, 18.0)), 2)),
            })
    return pd.DataFrame(records)


def generate_medications(encounters_df: pd.DataFrame) -> pd.DataFrame:
    log.info("Genererar mediciner...")
    records = []
    for _, enc in encounters_df.iterrows():
        n_meds = int(rng.integers(1, 7))
        meds = random.sample(ATC_MEDICATIONS, min(n_meds, len(ATC_MEDICATIONS)))
        for atc_code, drug_name in meds:
            records.append({
                "medication_id": str(uuid.uuid4()),
                "encounter_id": enc["encounter_id"],
                "atc_code": atc_code,
                "drug_name": drug_name,
                "dose_mg": round(float(rng.choice([5, 10, 25, 50, 100, 200, 500])), 3),
                "frequency": rng.choice(["1 gång/dag", "2 gånger/dag", "3 gånger/dag", "Vid behov"]),
                "route": rng.choice(["Oralt", "IV", "SC", "Inhalation"]),
                "start_date": enc["admission_date"],
                "end_date": enc["discharge_date"],
            })
    return pd.DataFrame(records)


# ── Main ────────────────────────────────────────────────────────────────────

def main(n_patients: int, output_dir: str) -> None:
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    patients = generate_patients(n_patients)

    encounters = generate_encounters(patients)
    diagnoses = generate_diagnoses(encounters)
    vitals = generate_vitals_labs(encounters)
    medications = generate_medications(encounters)

    # Ta bort intern temp-kolumn
    encounters = encounters.drop(columns=["_diagnoses"])

    # Spara som CSV (laddas sedan upp till Azure SQL / Fabric Lakehouse)
    for name, df in [
        ("patients", patients),
        ("encounters", encounters),
        ("diagnoses", diagnoses),
        ("vitals_labs", vitals),
        ("medications", medications),
    ]:
        path = Path(output_dir) / f"{name}.csv"
        df.to_csv(path, index=False)
        log.info("✅ Sparad: %s (%d rader)", path, len(df))

    log.info(
        "Datamängder: %d patienter, %d encounters, %d diagnoser, %d mätningar, %d mediciner",
        len(patients), len(encounters), len(diagnoses), len(vitals), len(medications),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generera syntetisk healthcare-data")
    parser.add_argument("--rows", type=int, default=10_000, help="Antal patienter")
    parser.add_argument("--output", type=str, default="data/raw", help="Utdatakatalog")
    args = parser.parse_args()
    main(args.rows, args.output)
