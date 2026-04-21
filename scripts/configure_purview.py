"""Configure Purview: create healthcare classification ruleset, update scan, check status."""
import json
import requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
SCAN_ENDPOINT = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
token = cred.get_token("https://purview.azure.net/.default").token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Create a healthcare classification rule set
ruleset_name = "healthcare-hca-ruleset"
url = f"{SCAN_ENDPOINT}/scan/scanrulesets/{ruleset_name}?api-version=2023-09-01"
body = {
    "kind": "AzureSqlDatabase",
    "properties": {
        "description": "Classification rules for healthcare data - PHI, ICD-10, patient demographics",
        "excludedSystemClassifications": [],
        "includedCustomClassificationRuleNames": [],
    },
}
resp = requests.put(url, headers=headers, json=body)
print(f"Create classification ruleset: {resp.status_code}")
if resp.status_code in (200, 201):
    print("Ruleset created:", json.loads(resp.text).get("name"))
else:
    print(resp.text[:500])

# Update the scan to use the classification ruleset
scan_url = f"{SCAN_ENDPOINT}/scan/datasources/sql-hca-demo/scans/healthcare-scan?api-version=2023-09-01"
scan_body = {
    "kind": "AzureSqlDatabaseMsi",
    "properties": {
        "databaseName": "HealthcareAnalyticsDB",
        "serverEndpoint": "sql-hca-demo.database.windows.net",
        "scanRulesetName": ruleset_name,
        "collection": {
            "referenceName": "prviewacc",
            "type": "CollectionReference",
        },
    },
}
resp2 = requests.put(scan_url, headers=headers, json=scan_body)
print(f"Update scan with ruleset: {resp2.status_code}")
if resp2.status_code in (200, 201):
    print("Scan updated with healthcare ruleset")
else:
    print(resp2.text[:500])

# Check scan status
status_url = f"{SCAN_ENDPOINT}/scan/datasources/sql-hca-demo/scans/healthcare-scan/runs?api-version=2023-09-01"
resp3 = requests.get(status_url, headers=headers)
if resp3.status_code == 200:
    runs = json.loads(resp3.text).get("value", [])
    for run in runs:
        print(f"Scan run: {run.get('scanResultId')} - Status: {run.get('status')}")
else:
    print(f"Check status: {resp3.status_code} {resp3.text[:300]}")
