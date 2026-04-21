"""Download and read diagnostic_log parquet files from OneLake."""
import requests
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
SILVER_LAKEHOUSE_ID = "270a6614-2a07-463d-94de-0c55b26ec6de"
BASE = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}"

def get_token():
    from azure.identity import AzureCliCredential
    cred = AzureCliCredential(process_timeout=30)
    return cred.get_token("https://storage.azure.com/.default").token

token = get_token()
headers = {"Authorization": f"Bearer {token}"}

# Full paths from OneLake listing (relative to workspace filesystem)
parquet_files = [
    f"{SILVER_LAKEHOUSE_ID}/Tables/Tables/diagnostic_log/part-00000-2827cb95-139d-4e4f-b70e-96fa727e8efa-c000.snappy.parquet",
    f"{SILVER_LAKEHOUSE_ID}/Tables/Tables/diagnostic_log/part-00001-2b3fdfe9-5b4f-4f0c-84f2-fd01b77fe9a0-c000.snappy.parquet",
    f"{SILVER_LAKEHOUSE_ID}/Tables/Tables/diagnostic_log/part-00002-bb928cc3-3142-43e7-b600-552611628d49-c000.snappy.parquet",
    f"{SILVER_LAKEHOUSE_ID}/Tables/Tables/diagnostic_log/part-00003-eebcc2a3-1836-4e4d-b5af-332bf4e05cc3-c000.snappy.parquet",
    f"{SILVER_LAKEHOUSE_ID}/Tables/Tables/diagnostic_log/part-00004-fd324c9e-e9ad-4b6d-9561-22e3b0a682bb-c000.snappy.parquet",
    f"{SILVER_LAKEHOUSE_ID}/Tables/Tables/diagnostic_log/part-00005-c7e8c80e-6155-4b7a-b9b4-0734e71209f3-c000.snappy.parquet",
    f"{SILVER_LAKEHOUSE_ID}/Tables/Tables/diagnostic_log/part-00006-8afec180-98dd-4ea1-8041-156772b74c62-c000.snappy.parquet",
    f"{SILVER_LAKEHOUSE_ID}/Tables/Tables/diagnostic_log/part-00007-5596ab50-0797-43d2-a19e-dbfc18e0b9ca-c000.snappy.parquet",
]

out_dir = r"c:\code\healthcare-analytics\healthcare-analytics\scripts\diag_parts"
os.makedirs(out_dir, exist_ok=True)

import pandas as pd

all_dfs = []
for i, pf in enumerate(parquet_files):
    url = f"{BASE}/{pf}"
    resp = requests.get(url, headers=headers)
    logger.info(f"Part {i}: {resp.status_code} ({len(resp.content)} bytes)")
    
    if resp.status_code == 200:
        local = os.path.join(out_dir, f"part_{i}.parquet")
        with open(local, 'wb') as f:
            f.write(resp.content)
        try:
            df = pd.read_parquet(local)
            all_dfs.append(df)
        except Exception as e:
            logger.error(f"  Failed to parse: {e}")

if all_dfs:
    combined = pd.concat(all_dfs, ignore_index=True)
    combined = combined.sort_values('step_order') if 'step_order' in combined.columns else combined
    
    print("\n" + "="*70)
    print("DIAGNOSTIC LOG RESULTS")
    print("="*70)
    print(f"Columns: {list(combined.columns)}")
    print(f"Total rows: {len(combined)}")
    print()
    
    for _, row in combined.iterrows():
        order = row.get('step_order', '?')
        name = row.get('step_name', '?')
        status = row.get('status', '?')
        msg = row.get('message', '')
        marker = "PASS" if status == "OK" else "FAIL" if status == "FAIL" else status
        icon = "✓" if marker == "PASS" else "✗" if marker == "FAIL" else "?"
        print(f"  {icon} Step {order:>2}: [{marker:>4s}] {name:25s} | {msg}")
    
    print()
    failures = combined[combined['status'] == 'FAIL'] if 'status' in combined.columns else pd.DataFrame()
    if len(failures) > 0:
        print("!" * 70)
        print(f"  {len(failures)} FAILED STEP(S):")
        print("!" * 70)
        for _, row in failures.iterrows():
            print(f"\n  >>> Step {row.get('step_order', '?')}: {row.get('step_name', '?')}")
            print(f"      Error: {row.get('message', 'no message')}")
    else:
        print("ALL STEPS PASSED!")
else:
    print("No data could be read.")
