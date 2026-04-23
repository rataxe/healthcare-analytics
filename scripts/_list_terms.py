"""List all glossary terms to build category mapping."""
import requests, sys, os, json
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
ACCT = "https://prviewacc.purview.azure.com"
ATLAS = ACCT + "/catalog/api/atlas/v2"
GLOSSARY_GUID = "d939ea20-9c67-48af-98d9-b66965f7cde1"

# Get all terms (paginated)
all_terms = []
offset = 0
while True:
    r = requests.get(f"{ATLAS}/glossary/{GLOSSARY_GUID}/terms?limit=100&offset={offset}",
                     headers=h, timeout=15)
    if r.status_code != 200:
        print(f"Error: {r.status_code}")
        break
    batch = r.json()
    if not batch:
        break
    all_terms.extend(batch)
    offset += len(batch)
    if len(batch) < 100:
        break

print(f"Total terms: {len(all_terms)}")
print()
for t in sorted(all_terms, key=lambda x: x.get("name", "")):
    cats = t.get("categories", [])
    cat_name = cats[0].get("displayText", "?") if cats else "-"
    print(f"  {t['name']}  |  cat={cat_name}")
