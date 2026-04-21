# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8adebdbb-1cde-40e8-bcde-1691f0e7e2b2",
# META       "default_lakehouse_name": "gold_omop",
# META       "default_lakehouse_workspace_id": "afda4639-34ce-4ee9-a82f-ab7b5cfd7334",
# META       "known_lakehouses": [
# META         {
# META           "id": "e1f2c38d-3f87-48ed-9769-6d2c8de22595"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Fabric Notebook — OMOP CDM v5.4 Transformation
# Silver → Gold OMOP: Transformerar HCA-data till OMOP Common Data Model
# Referens: https://ohdsi.github.io/CommonDataModel/cdm54.html
# Microsoft HDS: https://learn.microsoft.com/en-us/industry/healthcare/healthcare-data-solutions/omop-transformations-overview
#
# OMOP-tabeller som skapas:
#   - location, person, visit_occurrence, condition_occurrence,
#     drug_exposure, measurement, observation, observation_period,
#     concept, concept_ancestor (stub-vocabularies)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BRONZE_LAKEHOUSE = "bronze_lakehouse"
GOLD_OMOP_LAKEHOUSE = "gold_omop"   # monterat som default lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, LongType, StringType,
    DateType, FloatType, TimestampType
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("OMOPTransformation")
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
log.info("OMOP CDM v5.4 Transformation — Start")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

patients    = spark.table(f"{BRONZE_LAKEHOUSE}.hca_patients")
encounters  = spark.table(f"{BRONZE_LAKEHOUSE}.hca_encounters")
diagnoses   = spark.table(f"{BRONZE_LAKEHOUSE}.hca_diagnoses")
vitals      = spark.table(f"{BRONZE_LAKEHOUSE}.hca_vitals_labs")
medications = spark.table(f"{BRONZE_LAKEHOUSE}.hca_medications")

log.info("Patients: %d", patients.count())
log.info("Encounters: %d", encounters.count())
log.info("Diagnoses: %d", diagnoses.count())
log.info("Vitals/Labs: %d", vitals.count())
log.info("Medications: %d", medications.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Generera en stabil integer-mappning från UUID patient_id
person_map = (
    patients
    .select("patient_id")
    .distinct()
    .withColumn("person_id", F.monotonically_increasing_id() + 1)
)
person_map.cache()
log.info("Person map: %d patienter", person_map.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OMOP CDM: location representerar geografisk plats
location = (
    patients
    .select("region", "postal_code")
    .distinct()
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
log.info("✅ location: %d rader", location.count())

# Location-mappning för person-tabellen
loc_map = (
    patients.select("patient_id", "region", "postal_code")
    .join(
        spark.table(f"{GOLD_OMOP_LAKEHOUSE}.location")
            .select("location_id", "city", "zip"),
        on=(F.col("region") == F.col("city")) & (F.col("postal_code") == F.col("zip")),
        how="left"
    )
    .select("patient_id", "location_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# OMOP CDM v5.4: person — en rad per unik patient
# gender_concept_id: 8507=Male, 8532=Female, 8551=Unknown
# race_concept_id: 0 (not specified for Swedish data)

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
log.info("✅ person: %d rader", person.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mappar encounters → visit_occurrence
# visit_concept_id: 9201=Inpatient, 9202=Outpatient, 9203=ER
# visit_type_concept_id: 32817=EHR

visit_map = (
    encounters
    .select("encounter_id")
    .distinct()
    .withColumn("visit_occurrence_id", F.monotonically_increasing_id() + 1)
)
visit_map.cache()

# Mappa department till visit_concept_id
dept_to_visit_concept = {
    "Akutmedicin": 9203,       # Emergency Room Visit
    "Kirurgi": 9201,           # Inpatient Visit
    "Kardiologi": 9201,
    "Neurologi": 9201,
    "Ortopedi": 9201,
    "Onkologi": 9201,
    "Geriatrik": 9201,
    "Infektion": 9201,
    "Psykiatri": 9201,
}

visit_concept_expr = F.lit(9201)  # default: Inpatient
for dept, concept_id in dept_to_visit_concept.items():
    visit_concept_expr = F.when(
        F.col("department") == dept, concept_id
    ).otherwise(visit_concept_expr)

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
        F.lit(32817).cast(IntegerType()).alias("visit_type_concept_id"),  # EHR
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
log.info("✅ visit_occurrence: %d rader", visit_occurrence.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mappar diagnoses → condition_occurrence
# condition_type_concept_id: 32020=EHR condition
# ICD-10 → SNOMED via condition_source_value (direkt mappning)

condition_occurrence = (
    diagnoses
    .join(
        encounters.select("encounter_id", "patient_id"),
        on="encounter_id", how="inner"
    )
    .join(person_map, on="patient_id", how="inner")
    .join(visit_map, on="encounter_id", how="inner")
    .withColumn("condition_occurrence_id", F.monotonically_increasing_id() + 1)
    .select(
        F.col("condition_occurrence_id").cast(LongType()),
        F.col("person_id").cast(LongType()),
        F.lit(0).cast(IntegerType()).alias("condition_concept_id"),  # Kräver vocab-mappning
        F.col("confirmed_date").cast(DateType()).alias("condition_start_date"),
        F.col("confirmed_date").cast(TimestampType()).alias("condition_start_datetime"),
        F.lit(None).cast(DateType()).alias("condition_end_date"),
        F.lit(None).cast(TimestampType()).alias("condition_end_datetime"),
        F.lit(32020).cast(IntegerType()).alias("condition_type_concept_id"),  # EHR
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
log.info("✅ condition_occurrence: %d rader", condition_occurrence.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mappar medications → drug_exposure
# drug_type_concept_id: 32818=EHR prescription
# route_concept_id: 4132161=Oral, 4171047=Intravenous, 4139962=Subcutaneous

route_map_expr = (
    F.when(F.col("route") == "Oralt", 4132161)
     .when(F.col("route") == "IV", 4171047)
     .when(F.col("route") == "SC", 4139962)
     .otherwise(0)
)

drug_exposure = (
    medications
    .join(
        encounters.select("encounter_id", "patient_id"),
        on="encounter_id", how="inner"
    )
    .join(person_map, on="patient_id", how="inner")
    .join(visit_map, on="encounter_id", how="inner")
    .withColumn("drug_exposure_id", F.monotonically_increasing_id() + 1)
    .select(
        F.col("drug_exposure_id").cast(LongType()),
        F.col("person_id").cast(LongType()),
        F.lit(0).cast(IntegerType()).alias("drug_concept_id"),  # Kräver vocab-mappning
        F.col("start_date").cast(DateType()).alias("drug_exposure_start_date"),
        F.col("start_date").cast(TimestampType()).alias("drug_exposure_start_datetime"),
        F.coalesce(F.col("end_date"), F.col("start_date"))
            .cast(DateType()).alias("drug_exposure_end_date"),
        F.coalesce(F.col("end_date"), F.col("start_date"))
            .cast(TimestampType()).alias("drug_exposure_end_datetime"),
        F.lit(None).cast(DateType()).alias("verbatim_end_date"),
        F.lit(32818).cast(IntegerType()).alias("drug_type_concept_id"),  # EHR prescription
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
log.info("✅ drug_exposure: %d rader", drug_exposure.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mappar vitals_labs → measurement (unpivot: varje mätning = en rad)
# measurement_type_concept_id: 32856=Lab result
# LOINC-koncept per mättyp:
#   systolic_bp    → 3004249 (LOINC 8480-6)
#   diastolic_bp   → 3012888 (LOINC 8462-4)
#   heart_rate     → 3027018 (LOINC 8867-4)
#   temperature_c  → 3020891 (LOINC 8310-5)
#   oxygen_sat     → 3016502 (LOINC 2708-6)
#   glucose_mmol   → 3004501 (LOINC 2345-7)
#   creatinine     → 3016723 (LOINC 2160-0)
#   hemoglobin     → 3000963 (LOINC 718-7)
#   sodium         → 3019550 (LOINC 2951-2)
#   potassium      → 3023103 (LOINC 2823-3)
#   bmi            → 3038553 (LOINC 39156-5)
#   weight_kg      → 3025315 (LOINC 29463-7)

measurement_configs = [
    ("systolic_bp",       3004249,  "mmHg",   8876),
    ("diastolic_bp",      3012888,  "mmHg",   8876),
    ("heart_rate",        3027018,  "bpm",    8541),
    ("temperature_c",     3020891,  "°C",     586323),
    ("oxygen_saturation", 3016502,  "%",      8554),
    ("glucose_mmol",      3004501,  "mmol/L", 8753),
    ("creatinine_umol",   3016723,  "µmol/L", 8749),
    ("hemoglobin_g",      3000963,  "g/dL",   8713),
    ("sodium_mmol",       3019550,  "mmol/L", 8753),
    ("potassium_mmol",    3023103,  "mmol/L", 8753),
    ("bmi",               3038553,  "kg/m²",  9531),
    ("weight_kg",         3025315,  "kg",     9529),
]

# Skapa en union av alla mätningstyper
measurement_dfs = []
for col_name, concept_id, unit_str, unit_concept in measurement_configs:
    mdf = (
        vitals
        .filter(F.col(col_name).isNotNull())
        .join(
            encounters.select("encounter_id", "patient_id"),
            on="encounter_id", how="inner"
        )
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

from functools import reduce
measurement_union = reduce(lambda a, b: a.unionByName(b), measurement_dfs)
measurement = measurement_union.withColumn(
    "measurement_id", F.monotonically_increasing_id() + 1
)

measurement.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.measurement")
log.info("✅ measurement: %d rader", measurement.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mappar patient-attribut som OMOP observations
# smoking_status → observation (concept_id: 4275495 = Tobacco smoking behavior)
# ses_level      → observation (concept_id: 40766945 = Socioeconomic status)

smoking_concept_map = {
    "Never":   4222394,   # Never smoker
    "Former":  4310250,   # Former smoker
    "Current": 4298794,   # Current smoker
    "Unknown": 0,
}

smoking_expr = F.lit(0)
for status, cid in smoking_concept_map.items():
    smoking_expr = F.when(F.col("smoking_status") == status, cid).otherwise(smoking_expr)

# Smoking observation
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
        F.lit(32817).cast(IntegerType()).alias("observation_type_concept_id"),  # EHR
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

# SES observation
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
         .alias("value_as_string"),
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
log.info("✅ observation: %d rader", observation.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# En rad per person — total observationsperiod (min admission → max discharge)

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
        F.lit(32817).cast(IntegerType()).alias("period_type_concept_id"),  # EHR
    )
)

observation_period.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_OMOP_LAKEHOUSE}.observation_period")
log.info("✅ observation_period: %d rader", observation_period.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Minsta nödvändiga concepts för att semantiska modellen ska fungera
# I produktion: ladda från Athena vocabulary download

concept_data = [
    (8507,     "Male",                  "Gender",    "Gender",    "S", "M",      None, None),
    (8532,     "Female",                "Gender",    "Gender",    "S", "F",      None, None),
    (8551,     "Unknown",               "Gender",    "Gender",    "S", "X",      None, None),
    (9201,     "Inpatient Visit",       "Visit",     "Visit",     "S", "IP",     None, None),
    (9202,     "Outpatient Visit",      "Visit",     "Visit",     "S", "OP",     None, None),
    (9203,     "Emergency Room Visit",  "Visit",     "Visit",     "S", "ER",     None, None),
    (32817,    "EHR",                   "Type Concept","Type Concept","S","EHR", None, None),
    (32818,    "EHR prescription",      "Type Concept","Type Concept","S","EHR-Rx", None, None),
    (32020,    "EHR condition",         "Type Concept","Type Concept","S","EHR-Cond", None, None),
    (32856,    "Lab result",            "Type Concept","Type Concept","S","Lab",  None, None),
    (4275495,  "Tobacco smoking behavior","Observation","Observation","S","Smoking", None, None),
    (40766945, "Socioeconomic status",  "Observation","Observation","S","SES",   None, None),
    (4132161,  "Oral",                  "Route",     "Route",     "S", "PO",     None, None),
    (4171047,  "Intravenous",           "Route",     "Route",     "S", "IV",     None, None),
    (4139962,  "Subcutaneous",          "Route",     "Route",     "S", "SC",     None, None),
    (4222394,  "Never smoker",          "Observation","Observation","S","NS",    None, None),
    (4310250,  "Former smoker",         "Observation","Observation","S","FS",    None, None),
    (4298794,  "Current smoker",        "Observation","Observation","S","CS",    None, None),
    # Measurement concepts (LOINC)
    (3004249,  "Systolic blood pressure",  "Measurement","Measurement","S","8480-6",  None, None),
    (3012888,  "Diastolic blood pressure", "Measurement","Measurement","S","8462-4",  None, None),
    (3027018,  "Heart rate",               "Measurement","Measurement","S","8867-4",  None, None),
    (3020891,  "Body temperature",         "Measurement","Measurement","S","8310-5",  None, None),
    (3016502,  "Oxygen saturation",        "Measurement","Measurement","S","2708-6",  None, None),
    (3004501,  "Glucose",                  "Measurement","Measurement","S","2345-7",  None, None),
    (3016723,  "Creatinine",               "Measurement","Measurement","S","2160-0",  None, None),
    (3000963,  "Hemoglobin",               "Measurement","Measurement","S","718-7",   None, None),
    (3019550,  "Sodium",                   "Measurement","Measurement","S","2951-2",  None, None),
    (3023103,  "Potassium",                "Measurement","Measurement","S","2823-3",  None, None),
    (3038553,  "BMI",                      "Measurement","Measurement","S","39156-5", None, None),
    (3025315,  "Body weight",              "Measurement","Measurement","S","29463-7", None, None),
    # Unit concepts
    (8876,     "millimeter mercury column","Unit",   "Unit",      "S", "mmHg",   None, None),
    (8541,     "per minute",               "Unit",   "Unit",      "S", "/min",   None, None),
    (586323,   "degree Celsius",           "Unit",   "Unit",      "S", "°C",     None, None),
    (8554,     "percent",                  "Unit",   "Unit",      "S", "%",      None, None),
    (8753,     "millimole per liter",      "Unit",   "Unit",      "S", "mmol/L", None, None),
    (8749,     "micromole per liter",      "Unit",   "Unit",      "S", "µmol/L", None, None),
    (8713,     "gram per deciliter",       "Unit",   "Unit",      "S", "g/dL",   None, None),
    (9531,     "kilogram per square meter","Unit",   "Unit",      "S", "kg/m²",  None, None),
    (9529,     "kilogram",                 "Unit",   "Unit",      "S", "kg",     None, None),
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
log.info("✅ concept: %d rader", concept.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n=== OMOP CDM v5.4 TRANSFORMATION SUMMARY ===")
omop_tables = [
    "location", "person", "visit_occurrence", "condition_occurrence",
    "drug_exposure", "measurement", "observation", "observation_period", "concept"
]

for tbl in omop_tables:
    cnt = spark.table(f"{GOLD_OMOP_LAKEHOUSE}.{tbl}").count()
    print(f"  {tbl}: {cnt:,} rader")

print("\n✅ OMOP CDM v5.4 transformation klar!")
print("Semantisk modell kan nu skapas baserat på dessa Delta-tabeller.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
