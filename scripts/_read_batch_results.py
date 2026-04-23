"""Read batch scoring results from Gold Lakehouse via OneLake."""
import pandas as pd
from deltalake import DeltaTable
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://storage.azure.com/.default").token

WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
GOLD = "2960eef0-5de6-4117-80b1-6ee783cdaeec"
BASE = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{GOLD}/Tables"

storage_options = {"bearer_token": token, "use_fabric_endpoint": "true"}

# --- batch_scoring_results ---
print("=" * 70)
print("  BATCH SCORING RESULTS  (gold_lakehouse.batch_scoring_results)")
print("=" * 70)
dt = DeltaTable(f"{BASE}/batch_scoring_results", storage_options=storage_options)
df = dt.to_pandas()
print(f"Rows: {len(df)}   Columns: {list(df.columns)}\n")

# Summary stats
if "predicted_los" in df.columns:
    print(f"  Predicted LOS (days):")
    print(f"    Mean:   {df['predicted_los'].mean():.2f}")
    print(f"    Median: {df['predicted_los'].median():.2f}")
    print(f"    Min:    {df['predicted_los'].min():.2f}")
    print(f"    Max:    {df['predicted_los'].max():.2f}")
    print()

if "readmission_probability" in df.columns:
    print(f"  Readmission probability:")
    print(f"    Mean:   {df['readmission_probability'].mean():.3f}")
    print(f"    Median: {df['readmission_probability'].median():.3f}")
    print(f"    Min:    {df['readmission_probability'].min():.3f}")
    print(f"    Max:    {df['readmission_probability'].max():.3f}")
    print()

if "risk_category" in df.columns:
    dist = df["risk_category"].value_counts()
    print(f"  Risk distribution:")
    for cat, cnt in dist.items():
        pct = cnt / len(df) * 100
        print(f"    {cat:10s}: {cnt:5d}  ({pct:.1f}%)")
    print()

print("  Top 10 predictions (highest readmission risk):")
sort_col = "readmission_probability" if "readmission_probability" in df.columns else df.columns[0]
top10 = df.nlargest(10, sort_col)
pd.set_option("display.max_columns", 20)
pd.set_option("display.width", 140)
pd.set_option("display.float_format", lambda x: f"{x:.3f}")
print(top10.to_string(index=False))

# --- high_risk_patients ---
print("\n")
print("=" * 70)
print("  HIGH RISK PATIENTS  (gold_lakehouse.high_risk_patients)")
print("=" * 70)
dt2 = DeltaTable(f"{BASE}/high_risk_patients", storage_options=storage_options)
df2 = dt2.to_pandas()
print(f"Rows: {len(df2)}   Columns: {list(df2.columns)}\n")

if len(df2) > 0:
    sort_col2 = "readmission_probability" if "readmission_probability" in df2.columns else df2.columns[0]
    top_hr = df2.nlargest(min(15, len(df2)), sort_col2)
    print(top_hr.to_string(index=False))
else:
    print("  (no high-risk patients)")

print("\n\nDONE")
