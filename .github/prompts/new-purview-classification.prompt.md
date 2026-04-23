---
mode: "purview-governance"
description: "Register a new Purview classification rule for a healthcare data column"
---

# New Purview Classification

Create a new custom classification rule in Microsoft Purview for the Healthcare Analytics platform.

## Input
- **Column name**: ${input:columnName:Column name to classify}
- **Classification type**: ${input:classificationType:Classification (e.g. PHI, PII, ICD-10, Clinical)}
- **Sensitivity label**: ${input:sensitivityLabel:Sensitivity label (Confidential/Internal/Public)}
- **Table layer**: ${input:layer:Medallion layer (bronze/silver/gold)}

## Requirements
1. Use `azure-purview-catalog` SDK with `DefaultAzureCredential()`
2. Check if classification already exists before creating
3. Apply to the specified column in the `${layer}_*` table pattern
4. Set the correct sensitivity label mapping
5. Add the classification to the governance audit log
6. Follow existing patterns in `scripts/configure_purview.py`

## Output
- Python script that registers the classification
- Validation that the rule was applied correctly
- Update to governance documentation if needed
