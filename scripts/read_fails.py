"""Read FAIL entries from omop_diag with FULL detail."""
import io
import requests
import pandas as pd
from azure.identity import AzureCliCredential

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
GOLD_OMOP_LH_ID = "8adebdbb-1cde-40e8-bcde-1691f0e7e2b2"
TABLE_PATH = f"{GOLD_OMOP_LH_ID}/Tables/omop_diag"

token = AzureCliCredential(process_timeout=30).get_token("https://storage.azure.com/.default").token
headers = {"Authorization": f"Bearer {token}"}

# List files
url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{TABLE_PATH}"
resp = requests.get(url, headers=headers, params={"resource": "filesystem", "recursive": "true"})
files = resp.json().get("paths", [])

data_files = [
    f["name"] for f in files
    if f["name"].endswith(".parquet")
    and "_delta_log" not in f["name"]
    and f.get("isDirectory", "false") != "true"
]

all_rows = []
for pf in data_files:
    file_url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{pf}"
    resp = requests.get(file_url, headers=headers)
    if resp.status_code == 200:
        df = pd.read_parquet(io.BytesIO(resp.content))
        if len(df) > 0:
            all_rows.append(df)

combined = pd.concat(all_rows, ignore_index=True)
fails = combined[combined["status"] == "FAIL"]

for _, row in fails.iterrows():
    print(f"\n{'='*80}")
    print(f"STEP: {row['step']}")
    print(f"{'='*80}")
    print(row["detail"])
    print()
