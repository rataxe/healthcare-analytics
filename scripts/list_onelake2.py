"""List silver lakehouse contents with correct path format."""
import requests
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
SILVER_LAKEHOUSE_ID = "270a6614-2a07-463d-94de-0c55b26ec6de"
BASE = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{SILVER_LAKEHOUSE_ID}"

def get_token():
    from azure.identity import AzureCliCredential
    cred = AzureCliCredential(process_timeout=30)
    return cred.get_token("https://storage.azure.com/.default").token

def list_recursive(token, directory=""):
    """List all paths recursively."""
    headers = {"Authorization": f"Bearer {token}"}
    params = {"resource": "filesystem", "recursive": "true"}
    if directory:
        params["directory"] = directory
    
    resp = requests.get(BASE, headers=headers, params=params)
    logger.info(f"List recursive '{directory}': {resp.status_code}")
    
    if resp.status_code == 200:
        body = resp.json()
        paths = body.get('paths', [])
        for p in paths:
            name = p.get('name', '')
            is_dir = p.get('isDirectory', 'false') == 'true'
            size = p.get('contentLength', '0')
            marker = '[DIR]' if is_dir else f'[{size}B]'
            logger.info(f"  {marker:>12s} {name}")
        return paths
    else:
        logger.error(f"  {resp.text[:500]}")
    return []

def read_onelake_file(token, path):
    """Read file content."""
    url = f"{BASE}/{path}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    return resp

if __name__ == "__main__":
    token = get_token()
    
    # List Tables directory with full lakehouse prefix
    logger.info("=== Listing Tables (recursive) ===")
    tables_paths = list_recursive(token, f"{SILVER_LAKEHOUSE_ID}/Tables")
    
    if not tables_paths:
        # Try without lakehouse prefix
        logger.info("\n=== Trying without prefix ===")
        tables_paths = list_recursive(token, "Tables")
    
    if not tables_paths:
        # Try full recursive listing (may be large)
        logger.info("\n=== Full recursive listing ===")
        all_paths = list_recursive(token)
    
    # If we found delta log files, read the latest one
    delta_logs = [p for p in tables_paths if '_delta_log' in p.get('name', '') and p.get('name', '').endswith('.json')]
    if delta_logs:
        # Read the latest delta log to find parquet files
        latest = sorted(delta_logs, key=lambda x: x['name'])[-1]
        logger.info(f"\nReading delta log: {latest['name']}")
        resp = read_onelake_file(token, latest['name'])
        if resp.status_code == 200:
            logger.info(f"Content:\n{resp.text[:2000]}")
    
    # Try to read parquet files for diagnostic_log
    parquet_files = [p for p in tables_paths if 'diagnostic_log' in p.get('name', '') and p.get('name', '').endswith('.parquet')]
    if parquet_files:
        logger.info(f"\nFound {len(parquet_files)} parquet files for diagnostic_log")
        # Download first parquet and try to read with pandas
        pf = parquet_files[0]
        logger.info(f"Downloading: {pf['name']}")
        resp = read_onelake_file(token, pf['name'])
        if resp.status_code == 200:
            # Save locally and read with pandas
            local_path = "c:/code/healthcare-analytics/healthcare-analytics/scripts/diagnostic_log.parquet"
            with open(local_path, 'wb') as f:
                f.write(resp.content)
            logger.info(f"Saved to {local_path}")
            
            try:
                import pandas as pd
                df = pd.read_parquet(local_path)
                logger.info(f"\n{'='*60}")
                logger.info("DIAGNOSTIC LOG RESULTS")
                logger.info(f"{'='*60}")
                for _, row in df.iterrows():
                    logger.info(f"  Step {row.get('step_order', '?'):>2}: [{row.get('status', '?'):>4s}] {row.get('step_name', '?'):20s} | {row.get('message', '?')}")
                
                failures = df[df['status'] == 'FAIL'] if 'status' in df.columns else pd.DataFrame()
                if len(failures) > 0:
                    logger.info(f"\n{'!'*60}")
                    logger.info(f"  {len(failures)} FAILED STEP(S):")
                    for _, f in failures.iterrows():
                        logger.info(f"  >>> {f.get('step_name', '?')}: {f.get('message', '?')}")
                else:
                    logger.info("\n  ALL STEPS PASSED!")
            except ImportError:
                logger.error("pandas not installed, can't read parquet")
            except Exception as e:
                logger.error(f"Error reading parquet: {e}")
