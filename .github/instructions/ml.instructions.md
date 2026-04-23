---
applyTo: "**/*ml*,**/*training*,**/*scoring*,**/*model*,**/*prediction*,**/03_ml_training.py,**/05_batch_scoring.py,**/06_scoring_dashboard.py"
---

# ML Pipeline-instruktioner — Healthcare Analytics

## ML-pipeline i projektet

| Notebook | Steg | Beskrivning |
|----------|------|-------------|
| `03_ml_training.py` | Träning | LightGBM/Scikit-learn modellträning med MLflow-tracking |
| `05_batch_scoring.py` | Scoring | Batch-prediktion på nya patienter |
| `06_scoring_dashboard.py` | Dashboard | Förberedelse av scoringresultat för Power BI |

## Prediktionsmodeller

| Modell | Målvariabel | Algoritm | Metriker |
|--------|-------------|----------|----------|
| Length of Stay (LOS) | `los_days` | LightGBM Regressor | MAE, RMSE, R² |
| Readmission Risk | `readmitted_30d` | LightGBM Classifier | AUC-ROC, F1, Precision, Recall |

## Feature Engineering-mönster

```python
from pyspark.sql import functions as F

# Kategoriska features → one-hot encoding
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# Numeriska features → standardscaling
from pyspark.ml.feature import StandardScaler, VectorAssembler

# Audit: spåra feature-versioner
features_df = features_df.withColumn("_feature_version", F.lit("v2.1")) \
                          .withColumn("_feature_created_at", F.current_timestamp())
```

## MLflow-tracking (obligatoriskt)

```python
import mlflow

# Fabric har inbyggd MLflow-integration
mlflow.set_experiment("healthcare-analytics-los-prediction")

with mlflow.start_run(run_name="lightgbm-los-v2"):
    # Logga parametrar
    mlflow.log_params({
        "n_estimators": 500,
        "learning_rate": 0.05,
        "max_depth": 6,
        "feature_version": "v2.1"
    })

    # Träna modell
    model = lgb.train(params, train_data, valid_sets=[valid_data])

    # Logga metriker
    mlflow.log_metrics({
        "mae": mae_score,
        "rmse": rmse_score,
        "r2": r2_score
    })

    # Logga modell
    mlflow.lightgbm.log_model(model, "model")
```

## Data-split med temporalt fönster

```python
# ✅ RÄTT – temporal split (undviker dataläckage)
train_df = df.filter(F.col("admission_date") < "2024-01-01")
test_df = df.filter(F.col("admission_date") >= "2024-01-01")

# ❌ FEL – random split (läckage vid tidsseriedata)
train_df, test_df = df.randomSplit([0.8, 0.2])
```

## OMOP CDM-features

```python
# Vanliga features från OMOP-tabeller
CLINICAL_FEATURES = [
    "age_at_admission",         # person.year_of_birth → visit_occurrence.visit_start_date
    "gender_concept_id",        # person.gender_concept_id
    "condition_count",          # COUNT(condition_occurrence)
    "drug_exposure_count",      # COUNT(drug_exposure)
    "measurement_count",        # COUNT(measurement)
    "previous_visit_count",     # COUNT(visit_occurrence) senaste 12 mån
    "charlson_comorbidity_index" # Beräknad från condition_occurrence
]
```

## Batch Scoring-mönster

```python
# Ladda registrerad modell
model_uri = "models:/healthcare-los-prediction/Production"
model = mlflow.pyfunc.load_model(model_uri)

# Batch-prediktion
predictions_df = spark.createDataFrame(
    model.predict(features_pd),
    schema="prediction DOUBLE"
)

# Skriv till Gold-lager
predictions_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.readmission_predictions")
```

## Viktiga regler

- **MLflow** – ALLTID tracka experiment, parametrar och metriker
- **Temporal split** – aldrig random split för tidsseriedata
- **Feature-versioner** – tagga med `_feature_version` för reproducerbarhet
- **ALDRIG** inkludera PII (personnummer, namn) som features
- **OMOP CDM** – använd concept_id:s, inte råtext för kategoriska features
- **Modellregister** – registrera modeller i MLflow Model Registry med staging
