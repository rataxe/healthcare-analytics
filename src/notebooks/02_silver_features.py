# Fabric Notebook — Silver Layer Feature Engineering
# Bronze → Silver: feature engineering, Charlson Comorbidity Index, data quality
# Kör efter 01_bronze_ingestion.py

# ── PARAMETERCELL ─────────────────────────────────────────────────────────────
BRONZE_LAKEHOUSE = "bronze_lakehouse"
SILVER_LAKEHOUSE = "silver_lakehouse"

# ── CELL 1: Setup ─────────────────────────────────────────────────────────────
import logging
import traceback
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, ArrayType, StringType

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("SilverFeatures")
spark = SparkSession.builder.getOrCreate()
print("✅ Spark session ready")

# ── CELL 2: Läs Bronze-tabeller ───────────────────────────────────────────────
print(f"Reading bronze tables from {BRONZE_LAKEHOUSE}...")
encounters  = spark.table(f"{BRONZE_LAKEHOUSE}.hca_encounters")
patients    = spark.table(f"{BRONZE_LAKEHOUSE}.hca_patients")
diagnoses   = spark.table(f"{BRONZE_LAKEHOUSE}.hca_diagnoses")
vitals      = spark.table(f"{BRONZE_LAKEHOUSE}.hca_vitals_labs")
medications = spark.table(f"{BRONZE_LAKEHOUSE}.hca_medications")

enc_count = encounters.count()
print(f"✅ Bronze encounters: {enc_count} rader")
print(f"✅ Bronze patients: {patients.count()} rader")
print(f"✅ Bronze diagnoses: {diagnoses.count()} rader")

# ── CELL 3: Charlson Comorbidity Index (CCI) ──────────────────────────────────
# Mappar ICD-10-prefix till CCI-vikt
# Referens: Charlson et al. 1987, ICD-10-kodning enligt Quan et al. 2005

CCI_WEIGHTS = {
    "I21": 1, "I22": 1, "I25": 1,
    "I50": 1,
    "I70": 1, "I71": 1,
    "I60": 1, "I61": 1, "I62": 1, "I63": 1,
    "F00": 1, "F01": 1, "F02": 1, "F03": 1,
    "J40": 1, "J41": 1, "J42": 1, "J43": 1, "J44": 1,
    "M05": 1, "M06": 1,
    "K25": 1, "K26": 1, "K27": 1,
    "B18": 1, "K70": 1, "K71": 1,
    "E10": 1, "E11": 1,
    "E102": 2, "E112": 2,
    "I12": 2, "I13": 2, "N18": 2, "N19": 2,
    "C0": 2, "C1": 2, "C2": 2, "C3": 2,
    "C77": 6, "C78": 6, "C79": 6, "C80": 6,
    "B20": 6, "B21": 6, "B22": 6,
    "K721": 3, "K729": 3,
}

# Use a plain Python UDF (no broadcast) to avoid serialization issues
_CCI = dict(CCI_WEIGHTS)  # plain dict copy for closure

@F.udf(returnType=IntegerType())
def calc_cci_score(icd10_codes_list):
    if not icd10_codes_list:
        return 0
    total = 0
    for code in icd10_codes_list:
        prefix = code.replace(".", "")[:4]
        total += _CCI.get(prefix, _CCI.get(prefix[:3], 0))
    return min(total, 33)

# Aggregera diagnoser per encounter
diag_agg = (
    diagnoses
    .groupBy("encounter_id")
    .agg(
        F.collect_list("icd10_code").alias("icd10_codes"),
        F.sum(F.when(F.col("diagnosis_type") == "Primary", 1).otherwise(0)).alias("n_primary_diagnoses"),
        F.count("*").alias("n_diagnoses"),
    )
    .withColumn("cci_score", calc_cci_score(F.col("icd10_codes")))
)

print(f"✅ Diagnosis aggregation ready, sample: {diag_agg.limit(3).collect()}")

# ── CELL 4: Aggregera vitals/labs (senaste mätning per encounter) ─────────────
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
print(f"✅ Vitals aggregation ready")

# ── CELL 5: Tidigare inläggningar senaste 12 månader ─────────────────────────
# Use window function instead of self-join to avoid Cartesian explosion
enc_for_prior = encounters.select("encounter_id", "patient_id", "admission_date")

prior_admissions = (
    enc_for_prior.alias("current")
    .join(
        enc_for_prior.alias("prior"),
        on=(
            (F.col("current.patient_id") == F.col("prior.patient_id")) &
            (F.col("prior.admission_date") < F.col("current.admission_date")) &
            (F.col("prior.admission_date") >= F.date_sub(F.col("current.admission_date"), 365))
        ),
        how="left"
    )
    .groupBy(F.col("current.encounter_id").alias("encounter_id"))
    .agg(F.count("prior.encounter_id").alias("prior_admissions_12m"))
)
print(f"✅ Prior admissions ready")

# ── CELL 6: Antal mediciner vid inskrivning ───────────────────────────────────
med_count = (
    medications
    .groupBy("encounter_id")
    .agg(F.count("*").alias("medication_count"))
)
print(f"✅ Medication count ready")

# ── CELL 7: Bygg Silver feature-tabell ───────────────────────────────────────
silver_df = (
    encounters
    .join(patients, on="patient_id", how="left")
    .join(diag_agg, on="encounter_id", how="left")
    .join(vitals_latest, on="encounter_id", how="left")
    .join(prior_admissions, on="encounter_id", how="left")
    .join(med_count, on="encounter_id", how="left")
    .withColumn("age_at_admission",
        F.datediff(F.col("admission_date"), F.col("birth_date").cast("date")) / 365.25
    )
    .withColumn("prior_admissions_12m",
        F.coalesce(F.col("prior_admissions_12m"), F.lit(0))
    )
    .withColumn("medication_count",
        F.coalesce(F.col("medication_count"), F.lit(0))
    )
    .withColumn("cci_score",
        F.coalesce(F.col("cci_score"), F.lit(0))
    )
    .withColumn("polypharmacy", (F.col("medication_count") >= 5).cast(IntegerType()))
    .withColumn("age_group",
        F.when(F.col("age_at_admission") < 40, "18-39")
         .when(F.col("age_at_admission") < 65, "40-64")
         .when(F.col("age_at_admission") < 80, "65-79")
         .otherwise("80+")
    )
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

row_count = silver_df.count()
col_count = len(silver_df.columns)
print(f"✅ Silver feature-tabell: {row_count} rader, {col_count} kolumner")

# ── CELL 8: Data-kvalitetsvalidering ──────────────────────────────────────────
print("=== DATA QUALITY CHECKS ===")
total = silver_df.count()
los_null = silver_df.filter(F.col("los_days").isNull()).count()
readm_null = silver_df.filter(F.col("readmission_30d").isNull()).count()
if los_null > 0:
    print(f"⚠️ {los_null} rader saknar los_days")
if readm_null > 0:
    print(f"⚠️ {readm_null} rader saknar readmission_30d")
print(f"✅ {total} rader validerade")
readm_rate = silver_df.agg(F.mean("readmission_30d")).collect()[0][0]
print(f"   Readmission-rate: {readm_rate:.4f}")

# ── CELL 9: Spara till Silver Lakehouse ───────────────────────────────────────
print(f"Writing to {SILVER_LAKEHOUSE}.ml_features ...")
(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER_LAKEHOUSE}.ml_features")
)
print(f"✅ Silver tabell sparad: {SILVER_LAKEHOUSE}.ml_features")
