---
mode: "ml-engineer"
description: "Scaffold a new ML training pipeline for healthcare prediction"
---

# Train Model Pipeline

Create a new ML training pipeline for a healthcare predictive model.

## Input
- **Model name**: ${input:modelName:Model name (e.g. los_predictor, readmission_classifier)}
- **Task type**: ${input:taskType:Task type (regression/classification)}
- **Target column**: ${input:targetColumn:Target variable column name}
- **Algorithm**: ${input:algorithm:Algorithm (lightgbm/scikit-learn/synapseml)}

## Requirements
1. Create a Fabric notebook in `src/notebooks/` following existing naming conventions
2. Read features from Gold layer tables
3. Temporal train/test split (no data leakage)
4. Hyperparameter tuning with cross-validation
5. MLflow experiment tracking:
   - Log all parameters and hyperparameters
   - Log metrics: AUC-ROC, F1, precision, recall (classification) or RMSE, MAE, R² (regression)
   - Log confusion matrix / residual plots as artifacts
   - Register model in MLflow model registry
6. SHAP explainability analysis
7. Handle ICD-10 codes as categorical features
8. Document imputation strategy for missing values

## Output
- Training notebook with full pipeline
- MLflow experiment configuration
- Model evaluation summary
