# Fabric Notebook — Batch Scoring: Kör ML-modeller mot patientbatch
# Laddar registrerade MLflow-modeller (LOS + Readmission) och kör prediktion
# mot en ny batch patienter från Silver Lakehouse.
# Kör efter 03_ml_training.py (modellerna måste vara registrerade i MLflow)

# ── PARAMETERCELL (tagga som "parameters" i Fabric) ──────────────────────────
SILVER_LAKEHOUSE     = "silver_lakehouse"
GOLD_LAKEHOUSE       = "gold_lakehouse"
LOS_MODEL_NAME       = "los-predictor-lgbm"
READM_MODEL_NAME     = "readmission-classifier-rf"
LOS_MODEL_VERSION    = None          # None = senaste version
READM_MODEL_VERSION  = None          # None = senaste version
HIGH_RISK_THRESHOLD  = 0.40          # Återinskrivningsrisk-gräns
BATCH_SIZE           = None          # None = alla patienter, eller ange t.ex. 500

# ── CELL 1: Importer & setup ────────────────────────────────────────────────
import logging
from datetime import datetime

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("BatchScoring")
spark = SparkSession.builder.getOrCreate()
run_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
log.info("Batch scoring start: %s", run_ts)

# ── CELL 2: Ladda registrerade modeller från MLflow ─────────────────────────
def load_model(name, version=None):
    """Ladda en MLflow-modell via models:/-URI."""
    if version:
        uri = f"models:/{name}/{version}"
    else:
        uri = f"models:/{name}/latest"
    log.info("Laddar modell: %s", uri)
    return mlflow.sklearn.load_model(uri)

los_pipeline   = load_model(LOS_MODEL_NAME, LOS_MODEL_VERSION)
readm_pipeline = load_model(READM_MODEL_NAME, READM_MODEL_VERSION)
log.info("✅ Båda modellerna laddade")

# ── CELL 3: Läs Silver-features (patient-batch) ─────────────────────────────
NUMERIC_FEATURES = [
    "age_at_admission", "cci_score", "n_diagnoses",
    "prior_admissions_12m", "medication_count",
    "latest_systolic_bp", "latest_glucose_mmol",
    "latest_creatinine_umol", "latest_bmi", "latest_hemoglobin_g",
]
CATEGORICAL_FEATURES = [
    "department", "admission_source", "gender", "age_group", "smoking_status",
]
FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
ID_COLS  = ["patient_id", "encounter_id"]

df_spark = spark.table(f"{SILVER_LAKEHOUSE}.ml_features")
if BATCH_SIZE:
    df_spark = df_spark.limit(BATCH_SIZE)
total_rows = df_spark.count()
log.info("Batch storlek: %d rader", total_rows)

df = df_spark.toPandas()

# Behåll ID-kolumner + meta-kolumner som finns
meta_cols = [c for c in ID_COLS if c in df.columns]
extra_keep = [c for c in ["los_days", "readmission_30d", "department",
                           "admission_date", "discharge_date"] if c in df.columns]

X = df[FEATURES]
log.info("Features: %d numeriska, %d kategoriska", len(NUMERIC_FEATURES), len(CATEGORICAL_FEATURES))

# ── CELL 4: Kör prediktioner ────────────────────────────────────────────────
log.info("Kör LOS-prediktion...")
df["predicted_los_days"] = los_pipeline.predict(X)

log.info("Kör återinskrivnings-prediktion...")
df["readmission_probability"] = readm_pipeline.predict_proba(X)[:, 1]
df["readmission_predicted"]   = (df["readmission_probability"] >= HIGH_RISK_THRESHOLD).astype(int)

# Riskkategori
df["risk_category"] = pd.cut(
    df["readmission_probability"],
    bins=[0, 0.20, 0.40, 0.60, 1.0],
    labels=["Låg", "Medel", "Hög", "Mycket hög"],
    include_lowest=True,
)

df["scoring_timestamp"] = run_ts
df["los_model"]   = LOS_MODEL_NAME
df["readm_model"] = READM_MODEL_NAME

log.info("✅ Prediktioner klara för %d patienter", len(df))

# ── CELL 5: Sammanfattning ──────────────────────────────────────────────────
print("=" * 60)
print(f"  BATCH SCORING RESULTAT  ({run_ts})")
print("=" * 60)

print(f"\n  Antal patienter i batch:  {len(df)}")
print(f"  Medel LOS-prediktion:     {df['predicted_los_days'].mean():.1f} dagar")
print(f"  Median LOS-prediktion:    {df['predicted_los_days'].median():.1f} dagar")

risk_dist = df["risk_category"].value_counts().sort_index()
print(f"\n  Återinskrivningsrisk:")
for cat, cnt in risk_dist.items():
    pct = cnt / len(df) * 100
    print(f"    {cat:15s}  {cnt:5d}  ({pct:.1f}%)")

high_risk = df[df["readmission_predicted"] == 1]
print(f"\n  Högriskpatienter (>={HIGH_RISK_THRESHOLD:.0%}):  {len(high_risk)}")

if "los_days" in df.columns:
    from sklearn.metrics import mean_absolute_error
    try:
        from sklearn.metrics import root_mean_squared_error
    except ImportError:
        from sklearn.metrics import mean_squared_error
        def root_mean_squared_error(y_true, y_pred):
            return mean_squared_error(y_true, y_pred) ** 0.5
    actual = df["los_days"].dropna()
    pred   = df.loc[actual.index, "predicted_los_days"]
    if len(actual) > 0:
        mae  = mean_absolute_error(actual, pred)
        rmse = root_mean_squared_error(actual, pred)
        print(f"\n  LOS-modell utvärdering (batch):")
        print(f"    MAE:  {mae:.2f} dagar")
        print(f"    RMSE: {rmse:.2f} dagar")

if "readmission_30d" in df.columns:
    from sklearn.metrics import roc_auc_score, classification_report
    actual_r = df["readmission_30d"].dropna().astype(int)
    prob_r   = df.loc[actual_r.index, "readmission_probability"]
    if len(actual_r) > 0 and actual_r.nunique() > 1:
        auc = roc_auc_score(actual_r, prob_r)
        print(f"\n  Readmission-modell utvärdering (batch):")
        print(f"    ROC-AUC: {auc:.4f}")

# ── CELL 6: Top 10 högriskpatienter ─────────────────────────────────────────
top_risk = (
    df.nlargest(10, "readmission_probability")[
        meta_cols + ["predicted_los_days", "readmission_probability", "risk_category"]
        + ([c for c in ["department", "cci_score", "age_at_admission"] if c in df.columns])
    ]
)
print("\n  TOP 10 HÖGRISKPATIENTER:")
print(top_risk.to_string(index=False))

# ── CELL 7: Spara resultat → Gold Lakehouse ─────────────────────────────────
output_cols = (
    meta_cols
    + ["predicted_los_days", "readmission_probability",
       "readmission_predicted", "risk_category",
       "scoring_timestamp", "los_model", "readm_model"]
    + extra_keep
)
# Filtera bort kolumner som inte finns
output_cols = [c for c in output_cols if c in df.columns]
result_df = df[output_cols]

result_spark = spark.createDataFrame(result_df)
(
    result_spark
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_LAKEHOUSE}.batch_scoring_results")
)
log.info("✅ Resultat sparade: %s.batch_scoring_results (%d rader)", GOLD_LAKEHOUSE, len(result_df))

# ── CELL 8: Spara högriskpatienter separat ──────────────────────────────────
if len(high_risk) > 0:
    hr_cols = [c for c in output_cols if c in high_risk.columns]
    hr_spark = spark.createDataFrame(high_risk[hr_cols])
    (
        hr_spark
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_LAKEHOUSE}.high_risk_patients")
    )
    log.info("✅ Högriskpatienter: %s.high_risk_patients (%d rader)", GOLD_LAKEHOUSE, len(high_risk))

print("\n✅ Batch scoring klar!")
