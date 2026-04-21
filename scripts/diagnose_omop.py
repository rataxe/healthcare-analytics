"""Diagnose 04_omop_transformation failure by:
1. Fetching last job instance details
2. Downloading and verifying uploaded notebook content
"""
import base64
import json
import logging
import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
NOTEBOOK_ID = "c2c2a2f7-3d71-490e-94a4-1b42a9787c25"

def get_token():
    cred = AzureCliCredential(process_timeout=30)
    return cred.get_token("https://api.fabric.microsoft.com/.default").token

def main():
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}

    # 1. Get recent job instances
    log.info("=== RECENT JOB INSTANCES ===")
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/jobs/instances?limit=5"
    resp = requests.get(url, headers=headers)
    log.info("Job instances response: %d", resp.status_code)
    if resp.status_code == 200:
        data = resp.json()
        for inst in data.get("value", []):
            log.info("  Instance: %s", json.dumps(inst, indent=2, default=str))
    else:
        log.info("  Body: %s", resp.text[:1000])

    # 2. Download notebook definition
    log.info("\n=== NOTEBOOK DEFINITION ===")
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NOTEBOOK_ID}/getDefinition"
    resp = requests.post(url, headers=headers)
    log.info("getDefinition response: %d", resp.status_code)
    
    if resp.status_code == 200:
        data = resp.json()
        for part in data.get("definition", {}).get("parts", []):
            path = part.get("path", "")
            payload = part.get("payload", "")
            if path == "notebook-content.py":
                content = base64.b64decode(payload).decode("utf-8")
                lines = content.split("\n")
                log.info("Notebook content: %d lines, %d chars", len(lines), len(content))
                # Print first 30 lines and last 30 lines
                log.info("--- FIRST 30 LINES ---")
                for i, line in enumerate(lines[:30]):
                    print(f"  {i+1:4d}: {line}")
                log.info("--- LAST 30 LINES ---")
                for i, line in enumerate(lines[-30:]):
                    print(f"  {len(lines)-29+i:4d}: {line}")
                
                # Check for potential issues
                log.info("\n=== ISSUE CHECKS ===")
                for i, line in enumerate(lines):
                    if ".alias(" in line and ".cast(" in line:
                        idx = line.find(".alias(")
                        idx2 = line.find(".cast(")
                        if idx < idx2:
                            log.warning("  Line %d: .alias() before .cast() — %s", i+1, line.strip())
                    if "# Fabric notebook source" in line and i > 0:
                        log.warning("  Line %d: Fabric header not on line 1!", i+1)
                
                # Check cell count
                cell_count = content.count("# CELL ********************")
                meta_count = content.count("# METADATA ********************")
                log.info("  Cell markers: %d, Metadata markers: %d", cell_count, meta_count)
                
                # Check for syntax issues
                if "from functools import reduce" in content:
                    log.info("  ✓ functools reduce import found")
                
    elif resp.status_code == 202:
        # Long running operation
        location = resp.headers.get("Location", "")
        log.info("  Long running operation, polling: %s", location)
        import time
        for _ in range(10):
            time.sleep(3)
            resp2 = requests.get(location, headers=headers)
            if resp2.status_code == 200:
                body = resp2.json()
                status = body.get("status", "")
                log.info("  Operation status: %s", status)
                if status == "Succeeded":
                    result_loc = resp2.headers.get("Location", "")
                    if result_loc:
                        resp3 = requests.get(result_loc, headers=headers)
                        if resp3.status_code == 200:
                            data = resp3.json()
                            for part in data.get("definition", {}).get("parts", []):
                                path = part.get("path", "")
                                payload = part.get("payload", "")
                                if path == "notebook-content.py":
                                    content = base64.b64decode(payload).decode("utf-8")
                                    lines = content.split("\n")
                                    log.info("Notebook content: %d lines, %d chars", len(lines), len(content))
                                    log.info("--- FIRST 30 LINES ---")
                                    for i, line in enumerate(lines[:30]):
                                        print(f"  {i+1:4d}: {line}")
                                    log.info("--- LAST 30 LINES ---")
                                    for i, line in enumerate(lines[-30:]):
                                        print(f"  {len(lines)-29+i:4d}: {line}")
                                    
                                    cell_count = content.count("# CELL ********************")
                                    meta_count = content.count("# METADATA ********************")
                                    log.info("  Cell markers: %d, Metadata markers: %d", cell_count, meta_count)
                    break
                elif status == "Failed":
                    log.error("  Failed: %s", body)
                    break
            elif resp2.status_code == 202:
                continue
    else:
        log.info("  Body: %s", resp.text[:1000])

if __name__ == "__main__":
    main()
