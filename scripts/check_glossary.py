"""Check glossary state in Purview."""
import requests, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}"}

sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"

# List glossaries
r = sess.get(f"{ATLAS}/glossary", headers=h, timeout=30)
print(f"Glossary endpoint: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    glossaries = data if isinstance(data, list) else [data]
    for g in glossaries:
        gid = g["guid"]
        print(f"\n  Glossary: {g.get('name')} (guid={gid[:12]}...)")
        
        # Categories
        time.sleep(0.5)
        cr = sess.get(f"{ATLAS}/glossary/{gid}/categories", headers=h, timeout=30)
        cats = cr.json() if cr.status_code == 200 and isinstance(cr.json(), list) else []
        print(f"  Categories: {len(cats)}")
        for c in cats:
            print(f"    - {c.get('name')}")
        
        # Terms
        time.sleep(0.5)
        tr = sess.get(f"{ATLAS}/glossary/{gid}/terms?limit=500", headers=h, timeout=30)
        terms = tr.json() if tr.status_code == 200 and isinstance(tr.json(), list) else []
        print(f"  Terms: {len(terms)}")
        for t in terms:
            cat_name = ""
            cats_list = t.get("categories", [])
            if cats_list:
                cat_name = f" [{cats_list[0].get('displayText', '?')}]"
            print(f"    - {t.get('name')}{cat_name}")
else:
    print(f"Error: {r.text[:300]}")
