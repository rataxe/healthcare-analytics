"""Quick script to read diagnostic results from silver lakehouse via OneLake API."""
import requests
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
SILVER_LAKEHOUSE_ID = "270a6614-2a07-463d-94de-0c55b26ec6de"

def get_token(scope="https://storage.azure.com/.default"):
    from azure.identity import AzureCliCredential
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token(scope)
    return token.token

def list_onelake(token, path=""):
    """List files/dirs in OneLake."""
    base = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{SILVER_LAKEHOUSE_ID}"
    url = f"{base}/{path}?resource=filesystem&recursive=false"
    if path:
        url = f"{base}?resource=filesystem&recursive=false&directory={path}"
    
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    logger.info(f"List '{path}': {resp.status_code}")
    
    if resp.status_code == 200:
        body = resp.json()
        paths = body.get('paths', [])
        for p in paths:
            name = p.get('name', '')
            is_dir = p.get('isDirectory', 'false')
            size = p.get('contentLength', 0)
            logger.info(f"  {'[DIR]' if is_dir == 'true' else f'[{size}B]':>10s} {name}")
        return paths
    else:
        logger.error(f"  Error: {resp.text[:500]}")
    return []

def read_file(token, path):
    """Read a file from OneLake."""
    url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{SILVER_LAKEHOUSE_ID}/{path}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    return resp

if __name__ == "__main__":
    token = get_token()
    
    # List root
    logger.info("=== Silver Lakehouse Contents ===")
    list_onelake(token, "")
    
    # List Tables
    logger.info("\n=== Tables/ ===")
    list_onelake(token, "Tables")
    
    # List Files
    logger.info("\n=== Files/ ===")
    list_onelake(token, "Files")
    
    # Check if diagnostic_log table exists
    logger.info("\n=== Tables/diagnostic_log/ ===")
    paths = list_onelake(token, "Tables/diagnostic_log")
    
    # Try to find and read the delta log
    if paths:
        logger.info("\n=== Reading _delta_log ===")
        log_paths = list_onelake(token, "Tables/diagnostic_log/_delta_log")
        
        # Read the latest delta log JSON file
        for p in (log_paths or []):
            name = p.get('name', '')
            if name.endswith('.json'):
                logger.info(f"\nReading {name}...")
                resp = read_file(token, name)
                if resp.status_code == 200:
                    # Delta log is NDJSON - parse each line
                    for line in resp.text.strip().split('\n'):
                        try:
                            entry = json.loads(line)
                            if 'add' in entry:
                                parquet_path = entry['add']['path']
                                logger.info(f"  Parquet file: {parquet_path}")
                        except json.JSONDecodeError:
                            pass
    
    # Also try to check ml_features table
    logger.info("\n=== Tables/ml_features/ ===")
    list_onelake(token, "Tables/ml_features")
    
    # Try reading diagnostic_output.json from various places
    logger.info("\n=== Checking for diagnostic_output.json ===")
    for path_try in [
        "Files/diagnostic_output.json",
        "diagnostic_output.json",
    ]:
        resp = read_file(token, path_try)
        logger.info(f"  {path_try}: {resp.status_code}")
        if resp.status_code == 200:
            logger.info(resp.text[:2000])
