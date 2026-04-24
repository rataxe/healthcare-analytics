#!/usr/bin/env python3
"""Full audit of current Purview state - what exists and what is missing"""
import requests
import json
from pathlib import Path
from azure.identity import AzureCliCredential

# Load env for account/URL
env = {}
for line in Path("scripts/.env.purview").read_text().splitlines():
    if line.strip() and not line.startswith('#'):
        k, v = line.split('=', 1)
        env[k.strip()] = v.strip()

# Auth via AzureCliCredential (az login must be active)
credential = AzureCliCredential()
token = credential.get_token("https://purview.azure.net/.default").token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

base = f"https://{env['PURVIEW_ACCOUNT']}/datagovernance/catalog"
av = f"?api-version={env['API_VERSION']}"
atlas_base = f"https://{env['PURVIEW_ACCOUNT']}/catalog/api/atlas/v2"

print("=" * 70)
print("PURVIEW CURRENT STATE AUDIT")
print("=" * 70)

# --- Business Domains ---
print("\n=== BUSINESS DOMAINS ===")
r = requests.get(base + '/businessDomains' + av, headers=headers)
domains = r.json().get('value', [])
domain_ids = {}
print(f"Total: {len(domains)}")
for d in domains:
    print(f"  [{d.get('status','?')}] {d['name']} | {d['id']}")
    domain_ids[d['name']] = d['id']

# --- Data Products ---
print("\n=== DATA PRODUCTS ===")
r = requests.get(base + '/dataProducts' + av, headers=headers)
products = r.json().get('value', [])
print(f"Total: {len(products)}")
for p in products:
    print(f"  {p['name']} | domain: {p.get('domain','NONE')} | {p['id']}")

# --- Data Assets ---
print("\n=== DATA ASSETS ===")
r = requests.get(base + '/dataAssets' + av, headers=headers)
assets = r.json().get('value', [])
print(f"Total: {len(assets)}")
for a in assets[:5]:
    print(f"  {a.get('name','?')} | type: {a.get('objectType','?')}")
if len(assets) > 5:
    print(f"  ... and {len(assets)-5} more")

# --- Glossary via Unified Catalog ---
print("\n=== GLOSSARY TERMS (Unified Catalog) ===")
# Try the correct endpoint for glossary terms
for ep in ['/glossaryTerms', '/glossary/terms', '/terms']:
    r = requests.get(base + ep + av, headers=headers)
    print(f"  {ep}: {r.status_code}")
    if r.status_code == 200:
        terms = r.json().get('value', r.json() if isinstance(r.json(), list) else [])
        print(f"    Found {len(terms)} terms")
        for t in terms[:5]:
            print(f"    - {t.get('name','?')}")
        break

# --- Glossary via Atlas ---
print("\n=== GLOSSARY (Atlas API) ===")
r = requests.get(atlas_base + '/glossary', headers=headers)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    if isinstance(data, list):
        for g in data:
            terms = g.get('terms', [])
            print(f"  Glossary: {g.get('name','?')} | {len(terms)} terms")
    elif isinstance(data, dict):
        terms = data.get('terms', [])
        print(f"  Glossary: {data.get('name','?')} | {len(terms)} terms")

# Glossary terms
r2 = requests.get(atlas_base + '/glossary/terms', headers=headers)
print(f"\n  Atlas /glossary/terms: {r2.status_code}")
if r2.status_code == 200:
    terms = r2.json()
    if isinstance(terms, list):
        print(f"  Found {len(terms)} terms")
        for t in terms[:10]:
            print(f"    - {t.get('name','?')}")

# --- OKRs ---
print("\n=== OKRs ===")
r = requests.get(base + '/okrs' + av, headers=headers)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    okrs = r.json().get('value', [])
    print(f"  Total: {len(okrs)}")
    for o in okrs[:5]:
        print(f"    - {o.get('name','?')}")

# --- Critical Data Elements ---
print("\n=== CRITICAL DATA ELEMENTS ===")
r = requests.get(base + '/criticalDataElements' + av, headers=headers)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    cdes = r.json().get('value', [])
    print(f"  Total: {len(cdes)}")
    for c in cdes[:5]:
        print(f"    - {c.get('name','?')}")

# --- Data Access Policies ---
print("\n=== DATA ACCESS POLICIES ===")
# Try different endpoints
for ep in ['/dataAccessPolicies', '/policies', '/accessPolicies']:
    r = requests.get(base + ep + av, headers=headers)
    if r.status_code == 200:
        policies = r.json().get('value', [])
        print(f"  {ep}: {len(policies)} policies")
        break
    else:
        print(f"  {ep}: {r.status_code}")

print("\n" + "=" * 70)
print("AUDIT COMPLETE")
print("=" * 70)
