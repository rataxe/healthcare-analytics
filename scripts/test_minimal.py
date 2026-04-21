"""Create and run a minimal test notebook to isolate the silver lakehouse failure."""
import base64
import json
import logging
import time

import requests
from azure.identity import AzureCliCredential

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
SILVER_LH_ID = "270a6614-2a07-463d-94de-0c55b26ec6de"
BRONZE_LH_ID = "e1f2c38d-3f87-48ed-9769-6d2c8de22595"
NB_ID = "a65f0278-9dc0-402c-a1aa-c49c3e424a8f"  # 02_silver_features


def get_token():
    cred = AzureCliCredential(process_timeout=30)
    return cred.get_token("https://api.fabric.microsoft.com/.default").token


def test_minimal_notebook():
    """Upload a minimal notebook and run it."""
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Step 1: Download current notebook to check what Fabric actually has
    log.info("=== Downloading current notebook from Fabric ===")
    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/notebooks/{NB_ID}/getDefinition",
        headers=headers,
    )

    if resp.status_code == 200:
        defn = resp.json()
    elif resp.status_code == 202:
        location = resp.headers.get("Location")
        defn = None
        for _ in range(20):
            time.sleep(3)
            poll = requests.get(location, headers=headers)
            if poll.status_code == 200:
                body = poll.json()
                if body.get("status") == "Succeeded":
                    result_loc = poll.headers.get("Location")
                    if result_loc:
                        result = requests.get(result_loc, headers=headers)
                        if result.status_code == 200:
                            defn = result.json()
                    break
    else:
        log.error("Failed to get definition: %d %s", resp.status_code, resp.text[:500])
        return

    if defn:
        parts = defn.get("definition", {}).get("parts", [])
        for p in parts:
            payload = base64.b64decode(p["payload"]).decode("utf-8")
            log.info("--- %s (%d chars) ---", p["path"], len(payload))
            if p["path"] == "notebook-content.py":
                # Show first 500 chars and metadata section
                log.info("Content preview:\n%s", payload[:1500])
                log.info("...")
                log.info("Full length: %d, cell count: %d", len(payload), payload.count("# CELL"))
            else:
                log.info("%s", payload[:500])

    # Step 2: Upload a MINIMAL notebook — just print hello
    log.info("\n=== Uploading minimal test notebook ===")
    minimal_content = """# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": \"""" + SILVER_LH_ID + """\",
# META       "default_lakehouse_name": "silver_lakehouse",
# META       "default_lakehouse_workspace_id": \"""" + WORKSPACE_ID + """\",
# META       "known_lakehouses": [
# META         {
# META           "id": \"""" + BRONZE_LH_ID + """\"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

print("Hello from minimal test notebook!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print("Spark session OK:", spark.version)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Try reading bronze lakehouse
try:
    df = spark.table("bronze_lakehouse.hca_patients")
    print("bronze_lakehouse.hca_patients count:", df.count())
except Exception as e:
    print("ERROR reading bronze_lakehouse.hca_patients:", str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Try writing a simple table to silver lakehouse
try:
    test_df = spark.createDataFrame([(1, "test")], ["id", "value"])
    test_df.write.format("delta").mode("overwrite").saveAsTable("silver_lakehouse.test_table")
    print("Successfully wrote test_table to silver_lakehouse!")
except Exception as e:
    print("ERROR writing to silver_lakehouse:", str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

"""

    content_b64 = base64.b64encode(minimal_content.encode("utf-8")).decode("ascii")
    platform = json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Notebook", "displayName": "02_silver_features"},
        "config": {"version": "2.0", "logicalId": NB_ID},
    }, indent=2)
    platform_b64 = base64.b64encode(platform.encode("utf-8")).decode("ascii")

    payload = {
        "definition": {
            "parts": [
                {"path": "notebook-content.py", "payload": content_b64, "payloadType": "InlineBase64"},
                {"path": ".platform", "payload": platform_b64, "payloadType": "InlineBase64"},
            ]
        }
    }

    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NB_ID}/updateDefinition"
    resp = requests.post(url, headers=headers, json=payload)
    log.info("Upload response: %d", resp.status_code)

    if resp.status_code == 202:
        location = resp.headers.get("Location")
        if location:
            for _ in range(20):
                time.sleep(3)
                poll = requests.get(location, headers=headers)
                if poll.status_code == 200:
                    body = poll.json()
                    if body.get("status") == "Succeeded":
                        log.info("✅ Minimal notebook uploaded")
                        break
                    elif body.get("status") == "Failed":
                        log.error("Upload failed: %s", body)
                        return
    elif resp.status_code == 200:
        log.info("✅ Minimal notebook uploaded")
    else:
        log.error("Upload failed: %d %s", resp.status_code, resp.text[:500])
        return

    # Step 3: Run the minimal notebook
    log.info("\n=== Running minimal notebook ===")
    time.sleep(5)  # Give Fabric a moment to process

    # Refresh token
    token = get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    run_url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{NB_ID}/jobs/instances?jobType=RunNotebook"
    resp = requests.post(run_url, headers=headers)
    log.info("Run response: %d", resp.status_code)

    if resp.status_code not in (200, 202):
        log.error("Failed to start: %s", resp.text[:500])
        return

    location = resp.headers.get("Location")
    if not location:
        log.error("No Location header")
        return

    log.info("Polling: %s", location)

    elapsed = 0
    while elapsed < 300:
        time.sleep(15)
        elapsed += 15
        poll = requests.get(location, headers=headers)
        body = poll.json() if poll.status_code == 200 else {}
        status = body.get("status", "Unknown")

        log.info("  %ds: status=%s", elapsed, status)

        if status == "Completed":
            log.info("✅ Minimal notebook completed in %ds!", elapsed)
            log.info("Body: %s", json.dumps(body, indent=2)[:2000])
            return True

        if status in ("Failed", "Cancelled"):
            log.error("❌ Minimal notebook %s after %ds", status, elapsed)
            log.error("Body: %s", json.dumps(body, indent=2))
            return False

    log.error("Timed out")
    return False


if __name__ == "__main__":
    test_minimal_notebook()
