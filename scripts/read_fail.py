"""Read the FAIL parquet file to get full traceback."""
import io
import requests
import pandas as pd
from azure.identity import AzureCliCredential

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
GOLD_OMOP_LH_ID = "8adebdbb-1cde-40e8-bcde-1691f0e7e2b2"

token = AzureCliCredential(process_timeout=30).get_token("https://storage.azure.com/.default").token
headers = {"Authorization": f"Bearer {token}"}

# The last file has the FAIL entry
pf = f"{GOLD_OMOP_LH_ID}/Tables/omop_diag/part-00007-f33d437c-1f58-464c-a839-3877fc700fee-c000.snappy.parquet"
url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{pf}"
resp = requests.get(url, headers=headers)
df = pd.read_parquet(io.BytesIO(resp.content))
print("Columns:", list(df.columns))
for _, row in df.iterrows():
    print(f"\n=== [{row.get('status')}] {row.get('step')} ===")
    print(row.get('detail', ''))
