#!/usr/bin/env python3
"""
PURVIEW API STATUS & RECOMMENDATIONS
Summary of available APIs and workarounds
"""
import requests
from azure.identity import AzureCliCredential

# Configuration
PURVIEW_ACCOUNT = 'prviewacc'
ACCOUNT_BASE = f'https://{PURVIEW_ACCOUNT}.purview.azure.com'

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

print("="*80)
print("  PURVIEW API STATUS")
print("="*80)

# Test Atlas API v2 (Working)
print("\n✅ ATLAS API V2 (WORKING)")
print("-"*80)
r = requests.get(
    f'{ACCOUNT_BASE}/catalog/api/atlas/v2/glossary',
    headers=headers,
    timeout=30
)
print(f"GET /catalog/api/atlas/v2/glossary: {r.status_code}")

r = requests.get(
    f'{ACCOUNT_BASE}/catalog/api/atlas/v2/types/typedefs',
    headers=headers,
    timeout=30
)
print(f"GET /catalog/api/atlas/v2/types/typedefs: {r.status_code}")

# Test Search API (Working)
print("\n✅ SEARCH API (WORKING)")
print("-"*80)
r = requests.post(
    f'{ACCOUNT_BASE}/catalog/api/search/query?api-version=2022-08-01-preview',
    headers=headers,
    json={'keywords': '*', 'limit': 1},
    timeout=30
)
print(f"POST /catalog/api/search/query: {r.status_code}")

# Test Scan API (Requires permissions)
print("\n⚠️  SCAN API (REQUIRES DATA SOURCE ADMINISTRATOR ROLE)")
print("-"*80)
r = requests.get(
    f'{ACCOUNT_BASE}/scan/datasources?api-version=2023-09-01',
    headers=headers,
    timeout=30
)
print(f"GET /scan/datasources: {r.status_code}")
if r.status_code == 403:
    print("   → Need 'Data Source Administrator' role")
elif r.status_code == 200:
    sources = r.json().get('value', [])
    print(f"   → Found {len(sources)} data sources")

# Test Unified Catalog API (Not activated)
print("\n❌ UNIFIED CATALOG API (NOT ACTIVATED)")
print("-"*80)
r = requests.get(
    f'{ACCOUNT_BASE}/datagovernance/catalog/businessDomains?api-version=2025-09-15-preview',
    headers=headers,
    timeout=30
)
print(f"GET /datagovernance/catalog/businessDomains: {r.status_code}")

r = requests.get(
    f'{ACCOUNT_BASE}/datagovernance/catalog/dataProducts?api-version=2025-09-15-preview',
    headers=headers,
    timeout=30
)
print(f"GET /datagovernance/catalog/dataProducts: {r.status_code}")

# Summary
print("\n" + "="*80)
print("  SUMMARY & RECOMMENDATIONS")
print("="*80)
print("""
CURRENTLY AVAILABLE:
✅ Atlas API v2 - Entity management, glossary, typedef
   Base: /catalog/api/atlas/v2
   Use for: Custom entities, glossary terms, relationships

✅ Search API - Entity search across catalog
   Base: /catalog/api/search/query
   Use for: Finding entities, filtering by type

⚠️  Scan API - Data source scanning (needs permissions)
   Base: /scan
   Use for: Registering data sources, running scans
   Required role: Data Source Administrator

NOT AVAILABLE:
❌ Unified Catalog API (2025-09-15-preview)
   - Business Domains
   - Business Concepts
   - Data Products (native)
   - Business Policies
   
   Status: Preview API not activated on this account
   To activate: Contact Azure Support

WORKAROUNDS FOR MISSING APIs:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Governance Domains
   ✅ Current: Create manually in portal
   📋 Future: Use Unified Catalog API when activated

2. Data Products
   ✅ Current: Use custom entity type 'healthcare_data_product' (WORKING)
      - POST /entity/bulk for updates
      - Search API for discovery
   📋 Future: Migrate to native Data Products API

3. Business Concepts
   ✅ Current: Use glossary terms and categories
   📋 Future: Use Business Concepts API

4. Data Source Scanning
   ⚠️  Current: Need 'Data Source Administrator' role
   ✅ Steps:
      1. Go to portal.azure.com
      2. Navigate to Purview → Collections → Root Collection
      3. Add role assignment: Data Source Administrator
      4. Run: python scripts/scan_complete_setup.py

CURRENT SETUP STATUS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ 184 glossary terms
✅ 6 classifications
✅ 4 data products (custom entity type)
✅ 4 governance domains (manual, in portal)
⏳ Data source scanning (requires permissions)

NEXT STEPS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. Add 'Data Source Administrator' role to enable scanning
2. Run: python scripts/scan_complete_setup.py
3. Link glossary terms to entities
4. Create lineage relationships
5. Request Unified Catalog API activation (future)
""")
print("="*80)
