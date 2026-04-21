"""
Read diagnostic_log from silver lakehouse by:
1. Uploading a notebook that reads the table and writes results to Files/diagnostic_output.json
2. Running the notebook
3. Downloading the output file via OneLake API
"""
import requests
import logging
import json
import base64
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
NOTEBOOK_ID = "a65f0278-9dc0-402c-a1aa-c49c3e424a8f"
SILVER_LAKEHOUSE_ID = "270a6614-2a07-463d-94de-0c55b26ec6de"

def get_token(scope="https://api.fabric.microsoft.com/.default"):
    from azure.identity import AzureCliCredential
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token(scope)
    logger.info(f"Token acquired for {scope}")
    return token.token

def build_reader_notebook():
    """Notebook that reads diagnostic_log and writes to Files/diagnostic_output.json"""
    cells = []
    
    cells.append({
        "id": "read-diag-1",
        "cell_type": "code",
        "source": [
            "import json\n",
            "\n",
            "results = []\n",
            "try:\n",
            "    df = spark.sql('SELECT * FROM diagnostic_log ORDER BY step_order')\n",
            "    rows = df.collect()\n",
            "    for row in rows:\n",
            "        results.append({\n",
            "            'step_order': row.step_order,\n",
            "            'step_name': row.step_name,\n",
            "            'status': row.status,\n",
            "            'message': row.message\n",
            "        })\n",
            "    print(f'Read {len(results)} diagnostic rows')\n",
            "except Exception as e:\n",
            "    results.append({'step_order': -1, 'step_name': 'READ_ERROR', 'status': 'FAIL', 'message': str(e)})\n",
            "    # Try to list tables\n",
            "    try:\n",
            "        tables = spark.sql('SHOW TABLES').collect()\n",
            "        for t in tables:\n",
            "            results.append({'step_order': -2, 'step_name': 'TABLE', 'status': 'INFO', 'message': f'{t.namespace}.{t.tableName}'})\n",
            "    except Exception as e2:\n",
            "        results.append({'step_order': -3, 'step_name': 'SHOW_TABLES_ERROR', 'status': 'FAIL', 'message': str(e2)})\n",
            "\n",
            "# Write to Files path in the default lakehouse\n",
            "output_path = 'abfss://270a6614-2a07-463d-94de-0c55b26ec6de@onelake.dfs.fabric.microsoft.com/Files/diagnostic_output.json'\n",
            "# Use Spark to write a single file\n",
            "from pyspark.sql import Row\n",
            "output_text = json.dumps(results, indent=2)\n",
            "# Write using mssparkutils or direct\n",
            "try:\n",
            "    mssparkutils.fs.put(output_path, output_text, True)\n",
            "    print(f'Wrote diagnostic output to {output_path}')\n",
            "except Exception as e:\n",
            "    print(f'mssparkutils write failed: {e}')\n",
            "    # Fallback: write as spark df\n",
            "    try:\n",
            "        rdd = spark.sparkContext.parallelize([output_text])\n",
            "        rdd.coalesce(1).saveAsTextFile(output_path + '_dir')\n",
            "        print('Wrote via RDD fallback')\n",
            "    except Exception as e2:\n",
            "        print(f'RDD write also failed: {e2}')\n",
            "\n",
            "print('DONE')\n",
            "for r in results:\n",
            "    print(f\"  {r['step_order']}: {r['step_name']} | {r['status']} | {r['message']}\")\n"
        ],
        "metadata": {},
        "outputs": []
    })
    
    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "language_info": {"name": "python"},
            "kernel_info": {"name": "synapse_pyspark"},
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

def upload_and_run():
    token = get_token()
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
        return False
    
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
    
    # Run notebook
    token2 = get_token()
    headers["Authorization"] = f"Bearer {token2}"
    
    run_url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(run_url, headers=headers)
    logger.info(f"Run status: {resp.status_code}")
    
    if resp.status_code != 202:
        logger.error(f"Run failed: {resp.text}")
        return False
    
    location = resp.headers.get('Location', '')
    job_id = location.split('/')[-1] if location else 'unknown'
    logger.info(f"Job: {job_id}")
    
    # Poll until done
    start = time.time()
    while True:
        elapsed = int(time.time() - start)
        if elapsed > 300:
            logger.error("Timeout")
            return False
        time.sleep(15)
        elapsed = int(time.time() - start)
        
        token3 = get_token()
        headers["Authorization"] = f"Bearer {token3}"
        
        r = requests.get(location, headers=headers)
        if r.status_code == 200:
            body = r.json()
            status = body.get('status', 'Unknown')
            logger.info(f"  {elapsed}s: {status}")
            
            if status == 'Completed':
                return True
            elif status in ('Failed', 'Cancelled'):
                logger.error(f"Job {status}: {json.dumps(body.get('failureReason', {}))}")
                return False
        else:
            logger.warning(f"  {elapsed}s: HTTP {r.status_code}")
    
    return False

def download_results():
    """Download diagnostic_output.json from OneLake."""
    token = get_token("https://storage.azure.com/.default")
    
    # OneLake path: https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/Files/diagnostic_output.json
    url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{SILVER_LAKEHOUSE_ID}/Files/diagnostic_output.json"
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    # First, read the file
    resp = requests.get(url, headers=headers)
    logger.info(f"Download status: {resp.status_code}")
    
    if resp.status_code == 200:
        try:
            results = json.loads(resp.text)
            logger.info(f"\n{'='*60}")
            logger.info("DIAGNOSTIC LOG RESULTS")
            logger.info(f"{'='*60}")
            
            for r in results:
                status_icon = "PASS" if r['status'] == 'OK' else "FAIL" if r['status'] == 'FAIL' else "INFO"
                logger.info(f"  Step {r['step_order']:2d}: [{status_icon:4s}] {r['step_name']:20s} | {r['message']}")
            
            failures = [r for r in results if r['status'] == 'FAIL']
            if failures:
                logger.info(f"\n{'!'*60}")
                logger.info(f"  {len(failures)} FAILED STEP(S):")
                for f in failures:
                    logger.info(f"  >>> {f['step_name']}: {f['message']}")
                logger.info(f"{'!'*60}")
            else:
                logger.info(f"\n  ALL STEPS PASSED!")
            
            return results
        except json.JSONDecodeError:
            logger.info(f"Raw content: {resp.text[:2000]}")
    else:
        logger.error(f"Download failed: {resp.status_code} - {resp.text[:500]}")
        
        # Try alternate path format
        url2 = f"https://onelake.dfs.fabric.microsoft.com/Healthcare-Analytics/{SILVER_LAKEHOUSE_ID}/Files/diagnostic_output.json"
        resp2 = requests.get(url2, headers=headers)
        logger.info(f"Alternate download: {resp2.status_code}")
        if resp2.status_code == 200:
            logger.info(resp2.text[:2000])
    
    return None

if __name__ == "__main__":
    logger.info("=== Step 1: Upload & run reader notebook ===")
    success = upload_and_run()
    
    if success:
        logger.info("\n=== Step 2: Download diagnostic results ===")
        time.sleep(5)  # Small delay for OneLake sync
        results = download_results()
        
        if not results:
            logger.info("Could not download via OneLake. Try checking the notebook output in Fabric portal.")
    else:
        logger.error("Reader notebook failed. Trying to download anyway (from previous run)...")
        download_results()
