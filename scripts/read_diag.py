"""Read the omop_diag Delta table to see which steps succeeded/failed."""
import io
import json
import logging
import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
GOLD_OMOP_LH_ID = "8adebdbb-1cde-40e8-bcde-1691f0e7e2b2"
TABLE_PATH = f"{GOLD_OMOP_LH_ID}/Tables/omop_diag"

def get_storage_token():
    cred = AzureCliCredential(process_timeout=30)
    return cred.get_token("https://storage.azure.com/.default").token

def list_files(headers, path, recursive=True):
    """List all files in a OneLake path."""
    url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{path}"
    params = {"resource": "filesystem", "recursive": str(recursive).lower()}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code == 200:
        return resp.json().get("paths", [])
    return []

def read_file(headers, path):
    """Read a file from OneLake."""
    url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{path}"
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.content
    log.error("Failed to read %s: %d", path, resp.status_code)
    return None

def main():
    token = get_storage_token()
    headers = {"Authorization": f"Bearer {token}"}

    # List all files
    files = list_files(headers, TABLE_PATH)
    log.info("Files in omop_diag:")
    parquet_files = []
    delta_logs = []
    for f in files:
        name = f["name"]
        size = f.get("contentLength", "?")
        is_dir = f.get("isDirectory", "false") == "true"
        if not is_dir:
            log.info("  %s (%s bytes)", name.split("/Tables/omop_diag/")[-1], size)
            if name.endswith(".parquet"):
                parquet_files.append(name)
            if name.endswith(".json") and "_delta_log" in name:
                delta_logs.append(name)

    # Read delta log entries to understand the table
    log.info("\n--- Delta Log Entries ---")
    for dl in sorted(delta_logs):
        content = read_file(headers, dl)
        if content:
            short_name = dl.split("/")[-1]
            # Parse NDJSON
            for line in content.decode("utf-8").strip().split("\n"):
                entry = json.loads(line)
                if "add" in entry:
                    log.info("  %s: ADD %s", short_name, entry["add"].get("path", ""))
                elif "remove" in entry:
                    log.info("  %s: REMOVE %s", short_name, entry["remove"].get("path", ""))
                elif "commitInfo" in entry:
                    op = entry["commitInfo"].get("operation", "")
                    log.info("  %s: %s", short_name, op)

    # Read parquet files using pyarrow
    log.info("\n--- Reading Parquet Data ---")
    try:
        import pyarrow.parquet as pq
        for pf in parquet_files:
            content = read_file(headers, pf)
            if content:
                table = pq.read_table(io.BytesIO(content))
                df = table.to_pandas()
                log.info("\nFile: %s", pf.split("/Tables/omop_diag/")[-1])
                for _, row in df.iterrows():
                    step = row.get("step", "?")
                    status = row.get("status", "?")
                    detail = row.get("detail", "")
                    log.info("  [%s] %s: %s", status, step, detail)
    except ImportError:
        log.warning("pyarrow not installed, trying pandas...")
        try:
            import pandas as pd
            for pf in parquet_files:
                content = read_file(headers, pf)
                if content:
                    df = pd.read_parquet(io.BytesIO(content))
                    log.info("\nFile: %s", pf.split("/Tables/omop_diag/")[-1])
                    for _, row in df.iterrows():
                        log.info("  [%s] %s: %s", row.get("status", "?"), row.get("step", "?"), row.get("detail", ""))
        except ImportError:
            log.error("Neither pyarrow nor pandas available. Printing raw parquet bytes.")
            for pf in parquet_files:
                content = read_file(headers, pf)
                if content:
                    log.info("File %s: %d bytes", pf.split("/")[-1], len(content))

if __name__ == "__main__":
    main()
