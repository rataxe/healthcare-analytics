"""Read the omop_diag table - data files only."""
import io
import logging
import requests
import pandas as pd
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
GOLD_OMOP_LH_ID = "8adebdbb-1cde-40e8-bcde-1691f0e7e2b2"
TABLE_PATH = f"{GOLD_OMOP_LH_ID}/Tables/omop_diag"

def main():
    token = AzureCliCredential(process_timeout=30).get_token("https://storage.azure.com/.default").token
    headers = {"Authorization": f"Bearer {token}"}

    # List all files
    url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{TABLE_PATH}"
    resp = requests.get(url, headers=headers, params={"resource": "filesystem", "recursive": "true"})
    files = resp.json().get("paths", [])

    # Filter only data parquet files (not in _delta_log)
    data_files = [
        f["name"] for f in files
        if f["name"].endswith(".parquet")
        and "_delta_log" not in f["name"]
        and f.get("isDirectory", "false") != "true"
    ]
    log.info("Found %d data parquet files", len(data_files))

    all_rows = []
    for pf in data_files:
        file_url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{pf}"
        resp = requests.get(file_url, headers=headers)
        if resp.status_code == 200:
            df = pd.read_parquet(io.BytesIO(resp.content))
            all_rows.append(df)

    if all_rows:
        combined = pd.concat(all_rows, ignore_index=True)
        log.info("\nColumns: %s", list(combined.columns))
        log.info("Total rows: %d\n", len(combined))
        # Sort by timestamp if available
        if "ts" in combined.columns:
            combined = combined.sort_values("ts")
        for _, row in combined.iterrows():
            step = row.get("step", "?")
            status = row.get("status", "?")
            detail = str(row.get("detail", ""))
            # Truncate detail for readability
            if len(detail) > 500:
                detail = detail[:500] + "..."
            log.info("[%s] %s: %s", status, step, detail)
    else:
        log.warning("No data found!")

if __name__ == "__main__":
    main()
