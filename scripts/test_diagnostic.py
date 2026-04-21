"""Upload and run a diagnostic silver notebook that wraps each step in try/except.
After completion, reads silver_lakehouse.diagnostic_log to find which step crashes."""
import base64
import json
import logging
import time

import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
SILVER_LH_ID = "270a6614-2a07-463d-94de-0c55b26ec6de"
BRONZE_LH_ID = "e1f2c38d-3f87-48ed-9769-6d2c8de22595"
NB_ID = "a65f0278-9dc0-402c-a1aa-c49c3e424a8f"


def get_token():
    cred = AzureCliCredential(process_timeout=30)
    return cred.get_token("https://api.fabric.microsoft.com/.default").token


DIAG_NOTEBOOK = r'''# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "''' + SILVER_LH_ID + r'''",
# META       "default_lakehouse_name": "silver_lakehouse",
# META       "default_lakehouse_workspace_id": "''' + WORKSPACE_ID + r'''",
# META       "known_lakehouses": [
# META         {
# META           "id": "''' + BRONZE_LH_ID + r'''"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ── Setup and diagnostic framework ──
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
import traceback

spark = SparkSession.builder.getOrCreate()
print("STEP 0: Spark session ready")
status_log = []

def log_step(step, ok, msg=""):
    status_log.append((step, "OK" if ok else "FAIL", str(msg)[:500]))
    print(f"  {'OK' if ok else 'FAIL'} step={step} {str(msg)[:200]}")

BRONZE = "bronze_lakehouse"
SILVER = "silver_lakehouse"
log_step("spark_init", True, spark.version)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Step 1: Read bronze tables ──
try:
    encounters  = spark.table(f"{BRONZE}.hca_encounters")
    patients    = spark.table(f"{BRONZE}.hca_patients")
    diagnoses   = spark.table(f"{BRONZE}.hca_diagnoses")
    vitals      = spark.table(f"{BRONZE}.hca_vitals_labs")
    medications = spark.table(f"{BRONZE}.hca_medications")
    enc_count = encounters.count()
    pat_count = patients.count()
    log_step("read_bronze", True, f"enc={enc_count} pat={pat_count}")
except Exception as e:
    log_step("read_bronze", False, traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Step 2: Diagnosis aggregation (no UDF yet) ──
diag_agg = None
try:
    diag_agg = (
        diagnoses
        .groupBy("encounter_id")
        .agg(
            F.collect_list("icd10_code").alias("icd10_codes"),
            F.sum(F.when(F.col("diagnosis_type") == "Primary", 1).otherwise(0)).alias("n_primary_diagnoses"),
            F.count("*").alias("n_diagnoses"),
        )
    )
    dc = diag_agg.count()
    log_step("diag_agg", True, f"rows={dc}")
except Exception as e:
    log_step("diag_agg", False, traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Step 3: CCI UDF ──
try:
    CCI_WEIGHTS = {
        "I21": 1, "I22": 1, "I25": 1, "I50": 1,
        "I70": 1, "I71": 1, "I60": 1, "I61": 1, "I62": 1, "I63": 1,
        "F00": 1, "F01": 1, "F02": 1, "F03": 1,
        "J40": 1, "J41": 1, "J42": 1, "J43": 1, "J44": 1,
        "M05": 1, "M06": 1, "K25": 1, "K26": 1, "K27": 1,
        "B18": 1, "K70": 1, "K71": 1, "E10": 1, "E11": 1,
        "E102": 2, "E112": 2, "I12": 2, "I13": 2, "N18": 2, "N19": 2,
        "C0": 2, "C1": 2, "C2": 2, "C3": 2,
        "C77": 6, "C78": 6, "C79": 6, "C80": 6,
        "B20": 6, "B21": 6, "B22": 6, "K721": 3, "K729": 3,
    }
    _CCI = dict(CCI_WEIGHTS)

    @F.udf(returnType=IntegerType())
    def calc_cci_score(codes):
        if not codes:
            return 0
        t = 0
        for c in codes:
            p = c.replace(".", "")[:4]
            t += _CCI.get(p, _CCI.get(p[:3], 0))
        return min(t, 33)

    if diag_agg is not None:
        diag_agg = diag_agg.withColumn("cci_score", calc_cci_score(F.col("icd10_codes")))
        sample = diag_agg.select("cci_score").limit(3).collect()
        log_step("cci_udf", True, f"sample={sample}")
    else:
        log_step("cci_udf", False, "skipped-diag_agg None")
except Exception as e:
    log_step("cci_udf", False, traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Step 4: Vitals latest ──
vitals_latest = None
try:
    w = Window.partitionBy("encounter_id").orderBy(F.col("measured_at").desc())
    vitals_latest = (
        vitals
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            "encounter_id",
            F.col("systolic_bp").alias("latest_systolic_bp"),
            F.col("glucose_mmol").alias("latest_glucose_mmol"),
            F.col("creatinine_umol").alias("latest_creatinine_umol"),
            F.col("bmi").alias("latest_bmi"),
            F.col("hemoglobin_g").alias("latest_hemoglobin_g"),
        )
    )
    vc = vitals_latest.count()
    log_step("vitals_latest", True, f"rows={vc}")
except Exception as e:
    log_step("vitals_latest", False, traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Step 5: Prior admissions (self-join) ──
prior_admissions = None
try:
    enc_slim = encounters.select("encounter_id", "patient_id", "admission_date")
    prior_admissions = (
        enc_slim.alias("cur")
        .join(
            enc_slim.alias("pri"),
            on=(
                (F.col("cur.patient_id") == F.col("pri.patient_id")) &
                (F.col("pri.admission_date") < F.col("cur.admission_date")) &
                (F.col("pri.admission_date") >= F.date_sub(F.col("cur.admission_date"), 365))
            ),
            how="left"
        )
        .groupBy(F.col("cur.encounter_id").alias("encounter_id"))
        .agg(F.count("pri.encounter_id").alias("prior_admissions_12m"))
    )
    pc = prior_admissions.count()
    log_step("prior_admissions", True, f"rows={pc}")
except Exception as e:
    log_step("prior_admissions", False, traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Step 6: Medication count ──
med_count = None
try:
    med_count = medications.groupBy("encounter_id").agg(F.count("*").alias("medication_count"))
    mc = med_count.count()
    log_step("med_count", True, f"rows={mc}")
except Exception as e:
    log_step("med_count", False, traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Step 7: Build silver feature table ──
silver_df = None
try:
    silver_df = encounters.join(patients, on="patient_id", how="left")
    log_step("join_patients", True, "joined")
except Exception as e:
    log_step("join_patients", False, traceback.format_exc())

try:
    if silver_df is not None and diag_agg is not None:
        silver_df = silver_df.join(diag_agg, on="encounter_id", how="left")
        log_step("join_diag", True, "joined")
except Exception as e:
    log_step("join_diag", False, traceback.format_exc())

try:
    if silver_df is not None and vitals_latest is not None:
        silver_df = silver_df.join(vitals_latest, on="encounter_id", how="left")
        log_step("join_vitals", True, "joined")
except Exception as e:
    log_step("join_vitals", False, traceback.format_exc())

try:
    if silver_df is not None and prior_admissions is not None:
        silver_df = silver_df.join(prior_admissions, on="encounter_id", how="left")
        log_step("join_prior", True, "joined")
except Exception as e:
    log_step("join_prior", False, traceback.format_exc())

try:
    if silver_df is not None and med_count is not None:
        silver_df = silver_df.join(med_count, on="encounter_id", how="left")
        log_step("join_meds", True, "joined")
except Exception as e:
    log_step("join_meds", False, traceback.format_exc())

try:
    if silver_df is not None:
        silver_df = (
            silver_df
            .withColumn("age_at_admission",
                F.datediff(F.col("admission_date"), F.col("birth_date").cast("date")) / 365.25)
            .withColumn("prior_admissions_12m",
                F.coalesce(F.col("prior_admissions_12m"), F.lit(0)))
            .withColumn("medication_count",
                F.coalesce(F.col("medication_count"), F.lit(0)))
            .withColumn("cci_score",
                F.coalesce(F.col("cci_score"), F.lit(0)))
            .withColumn("polypharmacy", (F.col("medication_count") >= 5).cast(IntegerType()))
            .withColumn("age_group",
                F.when(F.col("age_at_admission") < 40, "18-39")
                 .when(F.col("age_at_admission") < 65, "40-64")
                 .when(F.col("age_at_admission") < 80, "65-79")
                 .otherwise("80+"))
            .withColumn("_processed_at", F.current_timestamp())
            .select(
                "encounter_id", "patient_id", "admission_date", "discharge_date",
                "department", "admission_source", "los_days", "readmission_30d",
                "age_at_admission", "age_group", "gender", "ses_level", "smoking_status",
                "cci_score", "n_diagnoses", "prior_admissions_12m", "medication_count", "polypharmacy",
                "latest_systolic_bp", "latest_glucose_mmol", "latest_creatinine_umol",
                "latest_bmi", "latest_hemoglobin_g",
                "icd10_codes", "_processed_at",
            )
        )
        rc = silver_df.count()
        log_step("build_features", True, f"rows={rc} cols={len(silver_df.columns)}")
except Exception as e:
    log_step("build_features", False, traceback.format_exc())
    silver_df = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Step 8: Write ml_features ──
try:
    if silver_df is not None:
        silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.ml_features")
        log_step("write_ml_features", True, "written")
    else:
        log_step("write_ml_features", False, "silver_df was None")
except Exception as e:
    log_step("write_ml_features", False, traceback.format_exc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Step 9: Write diagnostic log ──
schema = StructType([
    StructField("step", StringType()),
    StructField("status", StringType()),
    StructField("detail", StringType()),
])
log_df = spark.createDataFrame(status_log, schema)
log_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER}.diagnostic_log")
print("=== DIAGNOSTIC RESULTS ===")
for row in log_df.collect():
    print(f"  {row['status']:4s} | {row['step']:20s} | {row['detail'][:100]}")
print("DIAGNOSTIC NOTEBOOK COMPLETE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

'''


def run():
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Upload diagnostic notebook
    log.info("=== Uploading DIAGNOSTIC notebook ===")
    content_b64 = base64.b64encode(DIAG_NOTEBOOK.encode("utf-8")).decode("ascii")
    platform = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": "02_silver_features"},
        "config": {"version": "2.0", "logicalId": "00000000-0000-0000-0000-000000000000"},
    })
    platform_b64 = base64.b64encode(platform.encode("utf-8")).decode("ascii")

    payload = {
        "definition": {
            "parts": [
                {"path": "notebook-content.py", "payload": content_b64, "payloadType": "InlineBase64"},
                {"path": ".platform", "payload": platform_b64, "payloadType": "InlineBase64"},
            ]
        }
    }

    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NB_ID}/updateDefinition",
        headers=headers, json=payload,
    )
    log.info("Upload status: %d", resp.status_code)

    if resp.status_code == 202:
        loc = resp.headers.get("Location")
        if loc:
            for _ in range(20):
                time.sleep(3)
                p = requests.get(loc, headers=headers)
                if p.status_code == 200 and p.json().get("status") == "Succeeded":
                    break
    elif resp.status_code != 200:
        log.error("Upload failed: %d %s", resp.status_code, resp.text[:500])
        return

    log.info("Notebook uploaded, waiting 5s...")
    time.sleep(5)

    # Refresh token and run
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    run_url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NB_ID}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(run_url, headers=headers)
    log.info("Run status: %d", resp.status_code)
    if resp.status_code not in (200, 202):
        log.error("Failed to start: %s", resp.text[:500])
        return

    location = resp.headers.get("Location")
    job_id = location.rsplit("/", 1)[-1] if location else "?"
    log.info("Job: %s", job_id)
    log.info("Polling: %s", location)

    elapsed = 0
    while elapsed < 600:
        time.sleep(15)
        elapsed += 15
        poll = requests.get(location, headers=headers)
        body = poll.json() if poll.status_code == 200 else {}
        status = body.get("status", "Unknown")
        log.info("  %ds: %s", elapsed, status)

        if status == "Completed":
            log.info("Diagnostic notebook COMPLETED in %ds", elapsed)
            log.info("Now check silver_lakehouse.diagnostic_log in Fabric!")
            return True

        if status in ("Failed", "Cancelled"):
            log.error("Diagnostic notebook FAILED after %ds", elapsed)
            log.error("Body: %s", json.dumps(body, indent=2))
            log.info("Check silver_lakehouse.diagnostic_log - it may have partial results")
            return False

    log.error("Timed out at %ds", elapsed)
    return False


if __name__ == "__main__":
    run()
