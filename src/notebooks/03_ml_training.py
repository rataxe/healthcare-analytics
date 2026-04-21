# Fabric Notebook — ML Training: LOS Regressor + Readmission Classifier
# Kör efter 02_silver_features.py
# MLflow-experiment loggas automatiskt i Fabric ML Experiments

# ── PARAMETERCELL ──────────────────────────────────────────────────────────────
SILVER_LAKEHOUSE     = "silver_lakehouse"
GOLD_LAKEHOUSE       = "gold_lakehouse"
MLFLOW_EXPERIMENT    = "healthcare-analytics-v1"
LOS_MODEL_NAME       = "los-predictor-lgbm"
READM_MODEL_NAME     = "readmission-classifier-rf"
TEST_SIZE            = 0.20
RANDOM_STATE         = 42

# ── CELL 1: Importer ───────────────────────────────────────────────────────────
import logging
import mlflow
import mlflow.sklearn
import mlflow.lightgbm
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    mean_absolute_error,
    roc_auc_score, classification_report, confusion_matrix,
)
try:
    from sklearn.metrics import root_mean_squared_error
except ImportError:
    from sklearn.metrics import mean_squared_error
    def root_mean_squared_error(y_true, y_pred):
        return mean_squared_error(y_true, y_pred) ** 0.5
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
import lightgbm as lgb

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("MLTraining")
spark = SparkSession.builder.getOrCreate()

mlflow.set_experiment(MLFLOW_EXPERIMENT)

# ── CELL 2: Läs Silver features ────────────────────────────────────────────────
df_spark = spark.table(f"{SILVER_LAKEHOUSE}.ml_features")
df = df_spark.toPandas()
log.info("Dataset: %d rader, %d kolumner", *df.shape)

# ── CELL 3: Feature-definition ─────────────────────────────────────────────────
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
LOS_TARGET     = "los_days"
READM_TARGET   = "readmission_30d"

X = df[FEATURES]
y_los   = df[LOS_TARGET].astype(float)
y_readm = df[READM_TARGET].astype(int)

# ── CELL 4: Preprocessor pipeline ─────────────────────────────────────────────
numeric_transformer = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler()),
])
categorical_transformer = Pipeline([
    ("imputer", SimpleImputer(strategy="most_frequent")),
    ("encoder", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
])
preprocessor = ColumnTransformer([
    ("num", numeric_transformer, NUMERIC_FEATURES),
    ("cat", categorical_transformer, CATEGORICAL_FEATURES),
])

X_train, X_test, y_los_train, y_los_test, y_readm_train, y_readm_test = (
    train_test_split(X, y_los, y_readm, test_size=TEST_SIZE, random_state=RANDOM_STATE)
)
log.info("Train: %d | Test: %d", len(X_train), len(X_test))

# ── CELL 5: MODELL 1 — LOS Regression (LightGBM) ─────────────────────────────
with mlflow.start_run(run_name="LOS_LightGBM") as run_los:
    mlflow.set_tag("model_type", "regression")
    mlflow.set_tag("target", "los_days")
    mlflow.log_param("test_size", TEST_SIZE)
    mlflow.log_param("n_train", len(X_train))

    lgbm_params = {
        "objective": "poisson",        # Poisson-fördelning passar LOS
        "num_leaves": 63,
        "learning_rate": 0.05,
        "n_estimators": 500,
        "min_child_samples": 30,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "reg_alpha": 0.1,
        "reg_lambda": 0.1,
        "random_state": RANDOM_STATE,
        "verbose": -1,
    }
    mlflow.log_params(lgbm_params)

    los_pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("model", lgb.LGBMRegressor(**lgbm_params)),
    ])
    los_pipeline.fit(X_train, y_los_train)

    y_pred_los = los_pipeline.predict(X_test)
    mae  = mean_absolute_error(y_los_test, y_pred_los)
    rmse = root_mean_squared_error(y_los_test, y_pred_los)

    mlflow.log_metric("mae", mae)
    mlflow.log_metric("rmse", rmse)
    mlflow.sklearn.log_model(los_pipeline, artifact_path="model",
                              registered_model_name=LOS_MODEL_NAME)

    log.info("LOS Model — MAE: %.2f dagar | RMSE: %.2f dagar", mae, rmse)
    LOS_RUN_ID = run_los.info.run_id

# ── CELL 6: MODELL 2 — Readmission Klassificering (Random Forest) ─────────────
with mlflow.start_run(run_name="Readmission_RandomForest") as run_readm:
    mlflow.set_tag("model_type", "classification")
    mlflow.set_tag("target", "readmission_30d")
    mlflow.log_param("test_size", TEST_SIZE)
    mlflow.log_param("n_train", len(X_train))

    rf_params = {
        "n_estimators": 300,
        "max_depth": 12,
        "min_samples_split": 20,
        "min_samples_leaf": 10,
        "class_weight": "balanced",    # hanterar obalanserat dataset
        "random_state": RANDOM_STATE,
        "n_jobs": -1,
    }
    mlflow.log_params(rf_params)

    readm_pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("model", RandomForestClassifier(**rf_params)),
    ])
    readm_pipeline.fit(X_train, y_readm_train)

    y_pred_readm = readm_pipeline.predict(X_test)
    y_prob_readm = readm_pipeline.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_readm_test, y_prob_readm)

    mlflow.log_metric("roc_auc", auc)
    mlflow.sklearn.log_model(readm_pipeline, artifact_path="model",
                              registered_model_name=READM_MODEL_NAME)

    log.info("Readmission Model — ROC-AUC: %.4f", auc)
    print("\nClassification Report:")
    print(classification_report(y_readm_test, y_pred_readm, target_names=["Ej återinskriven", "Återinskriven"]))
    READM_RUN_ID = run_readm.info.run_id

# ── CELL 7: Generera prediktioner → Gold Lakehouse ────────────────────────────
df_test = X_test.copy()
df_test["actual_los"]          = y_los_test.values
df_test["predicted_los"]       = y_pred_los
df_test["actual_readmission"]  = y_readm_test.values
df_test["readmission_prob"]    = y_prob_readm
df_test["high_risk_readmission"] = (y_prob_readm >= 0.40).astype(int)
df_test["los_run_id"]          = LOS_RUN_ID
df_test["readm_run_id"]        = READM_RUN_ID

predictions_spark = spark.createDataFrame(df_test)
(
    predictions_spark
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_LAKEHOUSE}.ml_predictions")
)
log.info("✅ Prediktioner sparade i %s.ml_predictions", GOLD_LAKEHOUSE)

# ── CELL 8: Feature Importance (LightGBM) ─────────────────────────────────────
feature_names = (
    NUMERIC_FEATURES +
    list(los_pipeline.named_steps["preprocessor"]
         .named_transformers_["cat"]
         .named_steps["encoder"]
         .get_feature_names_out(CATEGORICAL_FEATURES))
)
lgbm_model = los_pipeline.named_steps["model"]
importance_df = pd.DataFrame({
    "feature": feature_names[:len(lgbm_model.feature_importances_)],
    "importance": lgbm_model.feature_importances_,
}).sort_values("importance", ascending=False).head(15)

print("\n=== TOP 15 FEATURES (LOS Model) ===")
print(importance_df.to_string(index=False))
