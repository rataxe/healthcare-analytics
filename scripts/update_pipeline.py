"""Update pipeline definition — all 4 notebooks with retry policies."""
import json, base64, time
import requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://api.fabric.microsoft.com/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
wsId = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
plId = "a163c4c5-376b-449a-9cad-50d45194370d"

pipeline = {"properties": {"activities": [
    {"name": "Bronze_Ingestion", "type": "TridentNotebook", "dependsOn": [],
     "policy": {"timeout": "0.02:00:00", "retry": 1, "retryIntervalInSeconds": 60},
     "typeProperties": {"notebookId": "41378393-200e-4cb7-8887-2209104393d6", "workspaceId": wsId}},
    {"name": "Silver_Features", "type": "TridentNotebook",
     "dependsOn": [{"activity": "Bronze_Ingestion", "dependencyConditions": ["Succeeded"]}],
     "policy": {"timeout": "0.02:00:00", "retry": 1, "retryIntervalInSeconds": 60},
     "typeProperties": {"notebookId": "a65f0278-9dc0-402c-a1aa-c49c3e424a8f", "workspaceId": wsId}},
    {"name": "Gold_ML_Training", "type": "TridentNotebook",
     "dependsOn": [{"activity": "Silver_Features", "dependencyConditions": ["Succeeded"]}],
     "policy": {"timeout": "0.02:00:00", "retry": 1, "retryIntervalInSeconds": 60},
     "typeProperties": {"notebookId": "094a9e43-55f0-4f11-a36f-fede8515d46c", "workspaceId": wsId}},
    {"name": "Gold_OMOP_Transformation", "type": "TridentNotebook",
     "dependsOn": [{"activity": "Silver_Features", "dependencyConditions": ["Succeeded"]}],
     "policy": {"timeout": "0.02:00:00", "retry": 1, "retryIntervalInSeconds": 60},
     "typeProperties": {"notebookId": "c2c2a2f7-3d71-490e-94a4-1b42a9787c25", "workspaceId": wsId}},
]}}

pl_b64 = base64.b64encode(json.dumps(pipeline).encode()).decode()
payload = {"definition": {"parts": [{"path": "pipeline-content.json", "payload": pl_b64, "payloadType": "InlineBase64"}]}}
url = f"https://api.fabric.microsoft.com/v1/workspaces/{wsId}/items/{plId}/updateDefinition"
resp = requests.post(url, headers=h, json=payload)
print(f"Update status: {resp.status_code}")

if resp.status_code == 202:
    loc = resp.headers.get("Location")
    for _ in range(15):
        time.sleep(2)
        p = requests.get(loc, headers=h)
        if p.status_code == 200:
            d = p.json()
            status = d.get("status", "unknown")
            print(f"Poll: {status}")
            if status == "Succeeded":
                break
elif resp.status_code != 200:
    print(resp.text[:500])

print("Done")
