# Healthcare ML Skill

## Purpose
Machine learning engineering for healthcare predictive analytics – LOS prediction and readmission classification.

## When to Use
- Building or modifying ML training pipelines
- Feature engineering from Gold layer tables
- MLflow experiment tracking and model registry
- Model evaluation, explainability (SHAP), and deployment
- EDA and statistical analysis on healthcare data

## Key Context

### Models
| Model | Task | Algorithm | Target |
|---|---|---|---|
| LOS Predictor | Regression | LightGBM / SynapseML | `length_of_stay_days` |
| Readmission Classifier | Binary Classification | LightGBM / Scikit-learn | `readmitted_30d` |

### Data Pipeline
```
Azure SQL → Bronze (raw) → Silver (OMOP CDM) → Gold (ML features) → ML Model → Power BI
```

### Feature Engineering Rules
- Features come from **Gold layer only** – never read raw Bronze/Silver for training
- ICD-10 codes: use category-level grouping (first 3 chars) as categorical features
- Temporal features: day of week, month, season from admission date
- Comorbidity index: calculated from condition_occurrence table
- Lab results: latest value before admission from measurement table
- Drug exposure: active medications count at admission

### ICD-10 Code Groups
```python
ICD10_GROUPS = {
    "I50": "heart_failure",
    "J44": "copd",
    "E11": "diabetes_type2",
    "I63": "stroke",
    "N18": "ckd",
}
```

### MLflow Configuration
```python
import mlflow

mlflow.set_experiment(f"/healthcare-analytics/{model_name}")

with mlflow.start_run(run_name=run_name):
    mlflow.log_params(params)
    mlflow.log_metrics(metrics)
    mlflow.log_artifact(confusion_matrix_path)
    mlflow.sklearn.log_model(model, artifact_path="model")
```

### Evaluation Metrics
- **Classification**: AUC-ROC, F1, precision, recall, confusion matrix, calibration curve
- **Regression**: RMSE, MAE, R², residual plot
- **Explainability**: SHAP summary plot, feature importance ranking

### Key Notebooks
Training notebooks live in `src/notebooks/` following the naming convention:
- `NB_XX_description.py` where XX is a sequential number
- Gold layer feature notebooks prepare data for training
- Evaluation notebooks produce model comparison reports

## Rules
1. All experiments tracked in MLflow – no unlogged training runs
2. Temporal train/test split – never random split for time-dependent data
3. Stratified sampling for imbalanced classification targets
4. Document imputation strategy in notebook markdown cells
5. SHAP analysis required for model interpretability
6. No data leakage – features must be available at prediction time
7. Handle class imbalance explicitly (SMOTE, class weights, or threshold tuning)
8. Log all hyperparameters, even defaults
9. Use `logging` module, never `print()` in production code
10. Type hints on all functions
