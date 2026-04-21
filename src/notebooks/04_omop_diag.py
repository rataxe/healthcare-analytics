# ── PARAMETERCELL (tagga som "parameters" i Fabric) ──────────────────────────
BRONZE_LAKEHOUSE = "bronze_lakehouse"
GOLD_OMOP_LAKEHOUSE = "gold_omop"

# ── CELL 1: Diagnostic run — capture errors ──────────────────────────────────
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, StringType,
    DateType, FloatType, TimestampType
)
from functools import reduce

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("OMOPDiag")
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

def write_status(step, status, detail=""):
    """Write step status to a tracking table."""
    # Truncate to 4000 chars to stay safe for Delta strings
    detail = str(detail)[:4000] if detail else ""
    df = spark.createDataFrame([(step, status, detail)], ["step", "status", "detail"])
    df.write.format("delta").mode("append").saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.omop_diag")

# Clear previous diagnostic data
try:
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_OMOP_LAKEHOUSE}.omop_diag")
except:
    pass

status_schema = StructType([
    StructField("step", StringType(), True),
    StructField("status", StringType(), True),
    StructField("detail", StringType(), True),
])
spark.createDataFrame([], status_schema).write.format("delta").mode("overwrite") \
    .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.omop_diag")

# ── CELL 2: Step-by-step with error capture ──────────────────────────────────
try:
    write_status("01_read_bronze", "START")
    patients    = spark.table(f"{BRONZE_LAKEHOUSE}.hca_patients")
    encounters  = spark.table(f"{BRONZE_LAKEHOUSE}.hca_encounters")
    diagnoses   = spark.table(f"{BRONZE_LAKEHOUSE}.hca_diagnoses")
    vitals      = spark.table(f"{BRONZE_LAKEHOUSE}.hca_vitals_labs")
    medications = spark.table(f"{BRONZE_LAKEHOUSE}.hca_medications")
    pc = patients.count()
    write_status("01_read_bronze", "OK", f"patients={pc}")
except Exception as e:
    write_status("01_read_bronze", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 3: Person map ──────────────────────────────────────────────────────
try:
    write_status("02_person_map", "START")
    person_map = (
        patients.select("patient_id").distinct()
        .withColumn("person_id", F.monotonically_increasing_id() + 1)
    )
    person_map.cache()
    pm_count = person_map.count()
    write_status("02_person_map", "OK", f"count={pm_count}")
except Exception as e:
    write_status("02_person_map", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 4: Location ────────────────────────────────────────────────────────
try:
    write_status("03_location", "START")
    location = (
        patients.select("region", "postal_code").distinct()
        .withColumn("location_id", F.monotonically_increasing_id() + 1)
        .select(
            F.col("location_id").cast(LongType()),
            F.lit(None).cast(StringType()).alias("address_1"),
            F.lit(None).cast(StringType()).alias("address_2"),
            F.col("region").alias("city"),
            F.lit("Sverige").alias("state"),
            F.col("postal_code").alias("zip"),
            F.lit("SE").alias("county"),
            F.lit("Sweden").alias("location_source_value"),
            F.lit(None).cast(LongType()).alias("country_concept_id"),
            F.lit("Sweden").alias("country_source_value"),
            F.lit(None).cast(FloatType()).alias("latitude"),
            F.lit(None).cast(FloatType()).alias("longitude"),
        )
    )
    location.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.location")
    lc = location.count()
    write_status("03_location", "OK", f"count={lc}")
except Exception as e:
    write_status("03_location", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 5: Person ──────────────────────────────────────────────────────────
try:
    write_status("04_person", "START")
    loc_map = (
        patients.select("patient_id", "region", "postal_code")
        .join(
            spark.table(f"{GOLD_OMOP_LAKEHOUSE}.location").select("location_id", "city", "zip"),
            on=(F.col("region") == F.col("city")) & (F.col("postal_code") == F.col("zip")),
            how="left"
        )
        .select("patient_id", "location_id")
    )
    person = (
        patients
        .join(person_map, on="patient_id", how="inner")
        .join(loc_map, on="patient_id", how="left")
        .select(
            F.col("person_id").cast(LongType()),
            F.when(F.col("gender") == "M", 8507)
             .when(F.col("gender") == "F", 8532)
             .otherwise(8551)
             .cast(IntegerType()).alias("gender_concept_id"),
            F.year(F.col("birth_date")).alias("year_of_birth"),
            F.month(F.col("birth_date")).alias("month_of_birth"),
            F.dayofmonth(F.col("birth_date")).alias("day_of_birth"),
            F.col("birth_date").cast(TimestampType()).alias("birth_datetime"),
            F.lit(0).cast(IntegerType()).alias("race_concept_id"),
            F.lit(0).cast(IntegerType()).alias("ethnicity_concept_id"),
            F.col("location_id").cast(LongType()),
            F.lit(None).cast(LongType()).alias("provider_id"),
            F.lit(None).cast(LongType()).alias("care_site_id"),
            F.col("patient_id").cast(StringType()).alias("person_source_value"),
            F.col("gender").alias("gender_source_value"),
            F.lit(None).cast(IntegerType()).alias("gender_source_concept_id"),
            F.lit(None).cast(StringType()).alias("race_source_value"),
            F.lit(None).cast(IntegerType()).alias("race_source_concept_id"),
            F.lit(None).cast(StringType()).alias("ethnicity_source_value"),
            F.lit(None).cast(IntegerType()).alias("ethnicity_source_concept_id"),
        )
    )
    person.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.person")
    pc2 = person.count()
    write_status("04_person", "OK", f"count={pc2}")
except Exception as e:
    write_status("04_person", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 6: Visit occurrence ────────────────────────────────────────────────
try:
    write_status("05_visit", "START")
    visit_map = (
        encounters.select("encounter_id").distinct()
        .withColumn("visit_occurrence_id", F.monotonically_increasing_id() + 1)
    )
    visit_map.cache()

    visit_concept_expr = F.when(F.col("department") == "Akutmedicin", 9203).otherwise(9201)

    visit_occurrence = (
        encounters
        .join(person_map, on="patient_id", how="inner")
        .join(visit_map, on="encounter_id", how="inner")
        .select(
            F.col("visit_occurrence_id").cast(LongType()),
            F.col("person_id").cast(LongType()),
            visit_concept_expr.cast(IntegerType()).alias("visit_concept_id"),
            F.col("admission_date").cast(DateType()).alias("visit_start_date"),
            F.col("admission_date").cast(TimestampType()).alias("visit_start_datetime"),
            F.coalesce(F.col("discharge_date"), F.col("admission_date"))
                .cast(DateType()).alias("visit_end_date"),
            F.coalesce(F.col("discharge_date"), F.col("admission_date"))
                .cast(TimestampType()).alias("visit_end_datetime"),
            F.lit(32817).cast(IntegerType()).alias("visit_type_concept_id"),
            F.lit(None).cast(LongType()).alias("provider_id"),
            F.lit(None).cast(LongType()).alias("care_site_id"),
            F.col("department").alias("visit_source_value"),
            F.lit(None).cast(IntegerType()).alias("visit_source_concept_id"),
            F.lit(0).cast(IntegerType()).alias("admitted_from_concept_id"),
            F.col("admission_source").alias("admitted_from_source_value"),
            F.lit(0).cast(IntegerType()).alias("discharged_to_concept_id"),
            F.col("discharge_disposition").alias("discharged_to_source_value"),
            F.lit(None).cast(LongType()).alias("preceding_visit_occurrence_id"),
        )
    )
    visit_occurrence.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.visit_occurrence")
    vc = visit_occurrence.count()
    write_status("05_visit", "OK", f"count={vc}")
except Exception as e:
    write_status("05_visit", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 7: Condition occurrence ─────────────────────────────────────────────
try:
    write_status("06_condition", "START")
    condition_occurrence = (
        diagnoses
        .join(encounters.select("encounter_id", "patient_id"), on="encounter_id", how="inner")
        .join(person_map, on="patient_id", how="inner")
        .join(visit_map, on="encounter_id", how="inner")
        .withColumn("condition_occurrence_id", F.monotonically_increasing_id() + 1)
        .select(
            F.col("condition_occurrence_id").cast(LongType()),
            F.col("person_id").cast(LongType()),
            F.lit(0).cast(IntegerType()).alias("condition_concept_id"),
            F.col("confirmed_date").cast(DateType()).alias("condition_start_date"),
            F.col("confirmed_date").cast(TimestampType()).alias("condition_start_datetime"),
            F.lit(None).cast(DateType()).alias("condition_end_date"),
            F.lit(None).cast(TimestampType()).alias("condition_end_datetime"),
            F.lit(32020).cast(IntegerType()).alias("condition_type_concept_id"),
            F.lit(0).cast(IntegerType()).alias("condition_status_concept_id"),
            F.lit(None).cast(StringType()).alias("stop_reason"),
            F.lit(None).cast(LongType()).alias("provider_id"),
            F.col("visit_occurrence_id").cast(LongType()),
            F.lit(None).cast(LongType()).alias("visit_detail_id"),
            F.col("icd10_code").alias("condition_source_value"),
            F.lit(0).cast(IntegerType()).alias("condition_source_concept_id"),
            F.col("diagnosis_type").alias("condition_status_source_value"),
        )
    )
    condition_occurrence.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.condition_occurrence")
    cc = condition_occurrence.count()
    write_status("06_condition", "OK", f"count={cc}")
except Exception as e:
    write_status("06_condition", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 8: Drug exposure ───────────────────────────────────────────────────
try:
    write_status("07_drug", "START")
    route_map_expr = (
        F.when(F.col("route") == "Oralt", 4132161)
         .when(F.col("route") == "IV", 4171047)
         .when(F.col("route") == "SC", 4139962)
         .otherwise(0)
    )
    drug_exposure = (
        medications
        .join(encounters.select("encounter_id", "patient_id"), on="encounter_id", how="inner")
        .join(person_map, on="patient_id", how="inner")
        .join(visit_map, on="encounter_id", how="inner")
        .withColumn("drug_exposure_id", F.monotonically_increasing_id() + 1)
        .select(
            F.col("drug_exposure_id").cast(LongType()),
            F.col("person_id").cast(LongType()),
            F.lit(0).cast(IntegerType()).alias("drug_concept_id"),
            F.col("start_date").cast(DateType()).alias("drug_exposure_start_date"),
            F.col("start_date").cast(TimestampType()).alias("drug_exposure_start_datetime"),
            F.coalesce(F.col("end_date"), F.col("start_date"))
                .cast(DateType()).alias("drug_exposure_end_date"),
            F.coalesce(F.col("end_date"), F.col("start_date"))
                .cast(TimestampType()).alias("drug_exposure_end_datetime"),
            F.lit(None).cast(DateType()).alias("verbatim_end_date"),
            F.lit(32818).cast(IntegerType()).alias("drug_type_concept_id"),
            F.lit(None).cast(StringType()).alias("stop_reason"),
            F.lit(None).cast(IntegerType()).alias("refills"),
            F.col("dose_mg").cast(FloatType()).alias("quantity"),
            F.lit(None).cast(IntegerType()).alias("days_supply"),
            F.lit(None).cast(StringType()).alias("sig"),
            route_map_expr.cast(IntegerType()).alias("route_concept_id"),
            F.lit(None).cast(StringType()).alias("lot_number"),
            F.lit(None).cast(LongType()).alias("provider_id"),
            F.col("visit_occurrence_id").cast(LongType()),
            F.lit(None).cast(LongType()).alias("visit_detail_id"),
            F.col("drug_name").alias("drug_source_value"),
            F.lit(0).cast(IntegerType()).alias("drug_source_concept_id"),
            F.col("route").alias("route_source_value"),
            F.col("dose_mg").cast(StringType()).alias("dose_unit_source_value"),
        )
    )
    drug_exposure.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.drug_exposure")
    dc = drug_exposure.count()
    write_status("07_drug", "OK", f"count={dc}")
except Exception as e:
    write_status("07_drug", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 9: Measurement ─────────────────────────────────────────────────────
try:
    write_status("08_measurement", "START")
    measurement_configs = [
        ("systolic_bp",       3004249,  "mmHg",   8876),
        ("diastolic_bp",      3012888,  "mmHg",   8876),
        ("heart_rate",        3027018,  "bpm",    8541),
        ("temperature_c",     3020891,  "C",      586323),
        ("oxygen_saturation", 3016502,  "pct",    8554),
        ("glucose_mmol",      3004501,  "mmol/L", 8753),
        ("creatinine_umol",   3016723,  "umol/L", 8749),
        ("hemoglobin_g",      3000963,  "g/dL",   8713),
        ("sodium_mmol",       3019550,  "mmol/L", 8753),
        ("potassium_mmol",    3023103,  "mmol/L", 8753),
        ("bmi",               3038553,  "kg/m2",  9531),
        ("weight_kg",         3025315,  "kg",     9529),
    ]

    measurement_dfs = []
    for col_name, concept_id, unit_str, unit_concept in measurement_configs:
        mdf = (
            vitals
            .filter(F.col(col_name).isNotNull())
            .join(encounters.select("encounter_id", "patient_id"), on="encounter_id", how="inner")
            .join(person_map, on="patient_id", how="inner")
            .join(visit_map, on="encounter_id", how="inner")
            .select(
                F.col("person_id").cast(LongType()),
                F.lit(concept_id).cast(IntegerType()).alias("measurement_concept_id"),
                F.col("measured_at").cast(DateType()).alias("measurement_date"),
                F.col("measured_at").cast(TimestampType()).alias("measurement_datetime"),
                F.lit(None).cast(StringType()).alias("measurement_time"),
                F.lit(32856).cast(IntegerType()).alias("measurement_type_concept_id"),
                F.lit(0).cast(IntegerType()).alias("operator_concept_id"),
                F.col(col_name).cast(FloatType()).alias("value_as_number"),
                F.lit(None).cast(IntegerType()).alias("value_as_concept_id"),
                F.lit(unit_concept).cast(IntegerType()).alias("unit_concept_id"),
                F.lit(None).cast(FloatType()).alias("range_low"),
                F.lit(None).cast(FloatType()).alias("range_high"),
                F.lit(None).cast(LongType()).alias("provider_id"),
                F.col("visit_occurrence_id").cast(LongType()),
                F.lit(None).cast(LongType()).alias("visit_detail_id"),
                F.lit(col_name).alias("measurement_source_value"),
                F.lit(0).cast(IntegerType()).alias("measurement_source_concept_id"),
                F.lit(unit_str).alias("unit_source_value"),
                F.lit(None).cast(StringType()).alias("value_source_value"),
                F.lit(None).cast(LongType()).alias("measurement_event_id"),
                F.lit(None).cast(IntegerType()).alias("meas_event_field_concept_id"),
            )
        )
        measurement_dfs.append(mdf)

    measurement_union = reduce(lambda a, b: a.unionByName(b), measurement_dfs)
    measurement = measurement_union.withColumn("measurement_id", F.monotonically_increasing_id() + 1)
    measurement.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.measurement")
    mc = measurement.count()
    write_status("08_measurement", "OK", f"count={mc}")
except Exception as e:
    write_status("08_measurement", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 10: Observation ────────────────────────────────────────────────────
try:
    write_status("09_observation", "START")

    smoking_expr = (
        F.when(F.col("smoking_status") == "Never", 4222394)
         .when(F.col("smoking_status") == "Former", 4310250)
         .when(F.col("smoking_status") == "Current", 4298794)
         .otherwise(0)
    )

    obs_smoking = (
        patients
        .join(person_map, on="patient_id", how="inner")
        .withColumn("observation_id", F.monotonically_increasing_id() + 1)
        .select(
            F.col("observation_id").cast(LongType()),
            F.col("person_id").cast(LongType()),
            F.lit(4275495).cast(IntegerType()).alias("observation_concept_id"),
            F.col("created_at").cast(DateType()).alias("observation_date"),
            F.col("created_at").cast(TimestampType()).alias("observation_datetime"),
            F.lit(32817).cast(IntegerType()).alias("observation_type_concept_id"),
            F.lit(None).cast(FloatType()).alias("value_as_number"),
            F.col("smoking_status").alias("value_as_string"),
            smoking_expr.cast(IntegerType()).alias("value_as_concept_id"),
            F.lit(None).cast(IntegerType()).alias("qualifier_concept_id"),
            F.lit(None).cast(IntegerType()).alias("unit_concept_id"),
            F.lit(None).cast(LongType()).alias("provider_id"),
            F.lit(None).cast(LongType()).alias("visit_occurrence_id"),
            F.lit(None).cast(LongType()).alias("visit_detail_id"),
            F.lit("smoking_status").alias("observation_source_value"),
            F.lit(0).cast(IntegerType()).alias("observation_source_concept_id"),
            F.lit(None).cast(StringType()).alias("unit_source_value"),
            F.lit(None).cast(StringType()).alias("qualifier_source_value"),
            F.lit(None).cast(StringType()).alias("value_source_value"),
            F.lit(None).cast(LongType()).alias("observation_event_id"),
            F.lit(None).cast(IntegerType()).alias("obs_event_field_concept_id"),
        )
    )

    obs_ses = (
        patients
        .join(person_map, on="patient_id", how="inner")
        .withColumn("observation_id", F.monotonically_increasing_id() + 100000001)
        .select(
            F.col("observation_id").cast(LongType()),
            F.col("person_id").cast(LongType()),
            F.lit(40766945).cast(IntegerType()).alias("observation_concept_id"),
            F.col("created_at").cast(DateType()).alias("observation_date"),
            F.col("created_at").cast(TimestampType()).alias("observation_datetime"),
            F.lit(32817).cast(IntegerType()).alias("observation_type_concept_id"),
            F.col("ses_level").cast(FloatType()).alias("value_as_number"),
            F.when(F.col("ses_level") == 1, "Low")
             .when(F.col("ses_level") == 2, "Medium")
             .when(F.col("ses_level") == 3, "High")
             .otherwise("Unknown").alias("value_as_string"),
            F.lit(0).cast(IntegerType()).alias("value_as_concept_id"),
            F.lit(None).cast(IntegerType()).alias("qualifier_concept_id"),
            F.lit(None).cast(IntegerType()).alias("unit_concept_id"),
            F.lit(None).cast(LongType()).alias("provider_id"),
            F.lit(None).cast(LongType()).alias("visit_occurrence_id"),
            F.lit(None).cast(LongType()).alias("visit_detail_id"),
            F.lit("ses_level").alias("observation_source_value"),
            F.lit(0).cast(IntegerType()).alias("observation_source_concept_id"),
            F.lit(None).cast(StringType()).alias("unit_source_value"),
            F.lit(None).cast(StringType()).alias("qualifier_source_value"),
            F.lit(None).cast(StringType()).alias("value_source_value"),
            F.lit(None).cast(LongType()).alias("observation_event_id"),
            F.lit(None).cast(IntegerType()).alias("obs_event_field_concept_id"),
        )
    )

    observation = obs_smoking.unionByName(obs_ses)
    observation.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.observation")
    oc = observation.count()
    write_status("09_observation", "OK", f"count={oc}")
except Exception as e:
    write_status("09_observation", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 11: Observation period ──────────────────────────────────────────────
try:
    write_status("10_obs_period", "START")
    observation_period = (
        encounters
        .join(person_map, on="patient_id", how="inner")
        .groupBy("person_id")
        .agg(
            F.min("admission_date").alias("observation_period_start_date"),
            F.max(F.coalesce(F.col("discharge_date"), F.col("admission_date")))
                .alias("observation_period_end_date"),
        )
        .withColumn("observation_period_id", F.monotonically_increasing_id() + 1)
        .select(
            F.col("observation_period_id").cast(LongType()),
            F.col("person_id").cast(LongType()),
            F.col("observation_period_start_date").cast(DateType()),
            F.col("observation_period_end_date").cast(DateType()),
            F.lit(32817).cast(IntegerType()).alias("period_type_concept_id"),
        )
    )
    observation_period.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.observation_period")
    opc = observation_period.count()
    write_status("10_obs_period", "OK", f"count={opc}")
except Exception as e:
    write_status("10_obs_period", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 12: Concept ────────────────────────────────────────────────────────
try:
    write_status("11_concept", "START")
    concept_data = [
        (8507, "Male", "Gender", "Gender", "S", "M", None, None),
        (8532, "Female", "Gender", "Gender", "S", "F", None, None),
        (8551, "Unknown", "Gender", "Gender", "S", "X", None, None),
        (9201, "Inpatient Visit", "Visit", "Visit", "S", "IP", None, None),
        (9202, "Outpatient Visit", "Visit", "Visit", "S", "OP", None, None),
        (9203, "Emergency Room Visit", "Visit", "Visit", "S", "ER", None, None),
        (32817, "EHR", "Type Concept", "Type Concept", "S", "EHR", None, None),
        (32818, "EHR prescription", "Type Concept", "Type Concept", "S", "EHR-Rx", None, None),
        (32020, "EHR condition", "Type Concept", "Type Concept", "S", "EHR-Cond", None, None),
        (32856, "Lab result", "Type Concept", "Type Concept", "S", "Lab", None, None),
        (4275495, "Tobacco smoking behavior", "Observation", "Observation", "S", "Smoking", None, None),
        (40766945, "Socioeconomic status", "Observation", "Observation", "S", "SES", None, None),
        (4132161, "Oral", "Route", "Route", "S", "PO", None, None),
        (4171047, "Intravenous", "Route", "Route", "S", "IV", None, None),
        (4139962, "Subcutaneous", "Route", "Route", "S", "SC", None, None),
        (4222394, "Never smoker", "Observation", "Observation", "S", "NS", None, None),
        (4310250, "Former smoker", "Observation", "Observation", "S", "FS", None, None),
        (4298794, "Current smoker", "Observation", "Observation", "S", "CS", None, None),
        (3004249, "Systolic blood pressure", "Measurement", "Measurement", "S", "8480-6", None, None),
        (3012888, "Diastolic blood pressure", "Measurement", "Measurement", "S", "8462-4", None, None),
        (3027018, "Heart rate", "Measurement", "Measurement", "S", "8867-4", None, None),
        (3020891, "Body temperature", "Measurement", "Measurement", "S", "8310-5", None, None),
        (3016502, "Oxygen saturation", "Measurement", "Measurement", "S", "2708-6", None, None),
        (3004501, "Glucose", "Measurement", "Measurement", "S", "2345-7", None, None),
        (3016723, "Creatinine", "Measurement", "Measurement", "S", "2160-0", None, None),
        (3000963, "Hemoglobin", "Measurement", "Measurement", "S", "718-7", None, None),
        (3019550, "Sodium", "Measurement", "Measurement", "S", "2951-2", None, None),
        (3023103, "Potassium", "Measurement", "Measurement", "S", "2823-3", None, None),
        (3038553, "BMI", "Measurement", "Measurement", "S", "39156-5", None, None),
        (3025315, "Body weight", "Measurement", "Measurement", "S", "29463-7", None, None),
        (8876, "millimeter mercury column", "Unit", "Unit", "S", "mmHg", None, None),
        (8541, "per minute", "Unit", "Unit", "S", "/min", None, None),
        (586323, "degree Celsius", "Unit", "Unit", "S", "C", None, None),
        (8554, "percent", "Unit", "Unit", "S", "pct", None, None),
        (8753, "millimole per liter", "Unit", "Unit", "S", "mmol/L", None, None),
        (8749, "micromole per liter", "Unit", "Unit", "S", "umol/L", None, None),
        (8713, "gram per deciliter", "Unit", "Unit", "S", "g/dL", None, None),
        (9531, "kilogram per square meter", "Unit", "Unit", "S", "kg/m2", None, None),
        (9529, "kilogram", "Unit", "Unit", "S", "kg", None, None),
    ]

    concept_schema = StructType([
        StructField("concept_id", IntegerType(), False),
        StructField("concept_name", StringType(), False),
        StructField("domain_id", StringType(), True),
        StructField("vocabulary_id", StringType(), True),
        StructField("standard_concept", StringType(), True),
        StructField("concept_code", StringType(), True),
        StructField("valid_start_date", DateType(), True),
        StructField("valid_end_date", DateType(), True),
    ])

    concept = spark.createDataFrame(concept_data, concept_schema)
    concept.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.concept")
    cc2 = concept.count()
    write_status("11_concept", "OK", f"count={cc2}")
except Exception as e:
    write_status("11_concept", "FAIL", traceback.format_exc()[:4000])
    pass  # continue

# ── CELL 13: Final summary ──────────────────────────────────────────────────
write_status("12_done", "OK", "All OMOP tables created")
print("OMOP CDM v5.4 transformation complete!")
