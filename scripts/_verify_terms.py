"""Quick verification of term assignments."""
import requests
from azure.identity import AzureCliCredential
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
GG = "d939ea20-9c67-48af-98d9-b66965f7cde1"

all_terms = []
offset = 0
while True:
    r = requests.get(f"{ATLAS}/glossary/{GG}/terms?limit=100&offset={offset}",
                     headers=h, timeout=15)
    batch = r.json() if r.status_code == 200 else []
    if not batch:
        break
    all_terms.extend(batch)
    offset += len(batch)
    if len(batch) < 100:
        break

assigned = 0
unassigned_names = []
for t in all_terms:
    tg = t["guid"]
    r2 = requests.get(f"{ATLAS}/glossary/term/{tg}", headers=h, timeout=15)
    if r2.status_code == 200:
        ft = r2.json()
        ae = ft.get("assignedEntities", [])
        if ae:
            assigned += 1
        else:
            unassigned_names.append(ft["name"])

print(f"Assigned: {assigned}/{len(all_terms)}")
print(f"\nUnassigned ({len(unassigned_names)}):")
for n in sorted(unassigned_names):
    print(f"  - {n}")
