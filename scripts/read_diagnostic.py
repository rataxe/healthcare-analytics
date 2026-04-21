"""Read the diagnostic_log table from silver lakehouse to see which steps passed/failed."""
import requests
import logging
import json
import base64
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
NOTEBOOK_ID = "a65f0278-9dc0-402c-a1aa-c49c3e424a8f"  # 02_silver_features (currently has diagnostic)

def get_token():
    from azure.identity import AzureCliCredential
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token("https://api.fabric.microsoft.com/.default")
    logger.info("AzureCliCredential.get_token succeeded")
    return token.token

def build_reader_notebook():
    """Build a minimal notebook that reads diagnostic_log and prints it."""
    cells = []
    
    # Cell 1: Read and display diagnostic_log
    cells.append({
        "id": "reader-cell-1",
        "cell_type": "code",
        "source": [
            "# Read diagnostic_log table\n",
            "try:\n",
            "    df = spark.sql('SELECT * FROM diagnostic_log ORDER BY step_order')\n",
            "    print('=== DIAGNOSTIC LOG ===')\n",
            "    rows = df.collect()\n",
            "    for row in rows:\n",
            "        print(f'Step {row.step_order}: {row.step_name} | {row.status} | {row.message}')\n",
            "    print(f'Total steps: {len(rows)}')\n",
            "    \n",
            "    # Also show any failures prominently\n",
            "    failures = [r for r in rows if r.status == 'FAIL']\n",
            "    if failures:\n",
            "        print('\\n=== FAILURES ===')\n",
            "        for f in failures:\n",
            "            print(f'FAILED: {f.step_name} -> {f.message}')\n",
            "    else:\n",
            "        print('\\nAll steps PASSED!')\n",
            "except Exception as e:\n",
            "    print(f'Error reading diagnostic_log: {e}')\n",
            "    # Try listing tables\n",
            "    print('Available tables:')\n",
            "    spark.sql('SHOW TABLES').show()\n"
        ],
        "metadata": {},
        "outputs": []
    })
    
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "a]]aakernel_info": {"name": "synapse_pyspark"},
            "microsoft": {
                "language": "python",
                "language_group": "synapse_pyspark"
            },
            "dependencies": {},
            "trident": {
                "lakehouse": {
                    "default_lakehouse": "270a6614-2a07-463d-94de-0c55b26ec6de",
                    "default_lakehouse_name": "silver_lakehouse",
                    "default_lakehouse_workspace_id": WORKSPACE_ID,
                    "known_lakehouses": [
                        {
                            "id": "270a6614-2a07-463d-94de-0c55b26ec6de"
                        }
                    ]
                }
            }
        },
        "cells": cells
    }
    return nb

def upload_and_run(token):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    nb = build_reader_notebook()
    nb_b64 = base64.b64encode(json.dumps(nb).encode()).decode()
    
    payload = {
        "displayName": "02_silver_features",
        "description": "Reading diagnostic log",
        "definition": {
            "format": "ipynb",
            "parts": [
                {
                    "path": "notebook-content.py",
                    "payload": nb_b64,
                    "payloadType": "InlineBase64"
                }
            ]
        }
    }
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/updateDefinition"
    resp = requests.post(url, headers=headers, json=payload)
    logger.info(f"Upload status: {resp.status_code}")
    
    if resp.status_code not in (200, 202):
        logger.error(f"Upload failed: {resp.text}")
        return
    
    # Wait for upload
    if resp.status_code == 202 and 'Location' in resp.headers:
        loc = resp.headers['Location']
        for _ in range(10):
            time.sleep(3)
            r2 = requests.get(loc, headers=headers)
            if r2.status_code == 200:
                body = r2.json()
                if body.get('status') in ('Succeeded', 'Failed'):
                    logger.info(f"Upload operation: {body.get('status')}")
                    break
    
    time.sleep(5)
    logger.info("Notebook uploaded, running...")
    
    # Run
    token2 = get_token()
    headers["Authorization"] = f"Bearer {token2}"
    
    run_url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(run_url, headers=headers)
    logger.info(f"Run status: {resp.status_code}")
    
    if resp.status_code != 202:
        logger.error(f"Run failed: {resp.text}")
        return
    
    location = resp.headers.get('Location', '')
    job_id = location.split('/')[-1] if location else 'unknown'
    logger.info(f"Job: {job_id}")
    logger.info(f"Polling: {location}")
    
    # Poll
    start = time.time()
    while True:
        elapsed = int(time.time() - start)
        if elapsed > 300:
            logger.error("Timeout after 300s")
            break
        time.sleep(15)
        elapsed = int(time.time() - start)
        
        if elapsed > 60:
            token3 = get_token() 
            headers["Authorization"] = f"Bearer {token3}"
        
        r = requests.get(location, headers=headers)
        if r.status_code == 200:
            body = r.json()
            status = body.get('status', 'Unknown')
            logger.info(f"  {elapsed}s: {status}")
            
            if status in ('Completed', 'Failed', 'Cancelled', 'Deduped'):
                logger.info(f"Final status: {status}")
                fr = body.get('failureReason', {})
                if fr:
                    logger.info(f"Failure: {json.dumps(fr)}")
                break
        else:
            logger.warning(f"  {elapsed}s: HTTP {r.status_code}")

if __name__ == "__main__":
    token = get_token()
    upload_and_run(token)
    logger.info("Done - check the notebook output in Fabric portal for diagnostic_log contents")
