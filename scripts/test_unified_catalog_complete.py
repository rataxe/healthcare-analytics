#!/usr/bin/env python3
"""
TEST PURVIEW UNIFIED CATALOG API (2025-09-15-preview)
Complete test of all operation groups

Documentation:
https://learn.microsoft.com/en-us/rest/api/purview/purview-unified-catalog/operation-groups

Operation Groups:
1. Business Concepts - Manage business concepts and their relationships
2. Business Domains - Create and manage governance domains
3. Business Policies - Define and enforce data policies
4. Data Assets - Register and manage data assets
5. Data Products - Create and manage data products
6. Glossaries - Manage business glossaries
7. Lineage - Track data lineage and relationships
"""
import requests
import json
from azure.identity import AzureCliCredential

# Configuration
PURVIEW_ACCOUNT = 'prviewacc'
TENANT_ID = '71c4b6d5-0065-4c6c-a125-841a582754eb'

# API Endpoints
ACCOUNT_BASE = f'https://{PURVIEW_ACCOUNT}.purview.azure.com'
TENANT_BASE = f'https://{TENANT_ID}-api.purview-service.microsoft.com'
API_VERSION = '2025-09-15-preview'

# Authentication
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

def test_endpoint(name: str, method: str, url: str, body: dict = None):
    """Test a single endpoint"""
    print(f"\n📋 {name}")
    print(f"   {method} {url}")
    
    try:
        if method == 'GET':
            r = requests.get(url, headers=headers, timeout=30)
        elif method == 'POST':
            r = requests.post(url, headers=headers, json=body, timeout=30)
        elif method == 'PUT':
            r = requests.put(url, headers=headers, json=body, timeout=30)
        elif method == 'DELETE':
            r = requests.delete(url, headers=headers, timeout=30)
        else:
            print(f"   ⚠️  Unknown method: {method}")
            return None
        
        status_code = r.status_code
        
        if status_code == 200:
            print(f"   ✅ {status_code} OK")
            try:
                data = r.json()
                if isinstance(data, list):
                    print(f"      Items: {len(data)}")
                elif isinstance(data, dict):
                    if 'value' in data:
                        print(f"      Items: {len(data.get('value', []))}")
                    elif 'id' in data:
                        print(f"      ID: {data.get('id', '?')}")
                return data
            except:
                print(f"      Response: {r.text[:100]}")
                return r.text
        elif status_code == 201:
            print(f"   ✅ {status_code} Created")
            return r.json() if r.text else None
        elif status_code == 204:
            print(f"   ✅ {status_code} No Content")
            return None
        elif status_code == 403:
            print(f"   ⚠️  {status_code} Forbidden - API not activated or missing permissions")
            return None
        elif status_code == 404:
            print(f"   ℹ️  {status_code} Not Found")
            return None
        else:
            print(f"   ❌ {status_code}")
            print(f"      {r.text[:300]}")
            return None
            
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        return None

print("="*80)
print("  PURVIEW UNIFIED CATALOG API TEST")
print("  API Version: 2025-09-15-preview")
print("="*80)

# Test both account and tenant endpoints
for endpoint_type, base_url in [('Account', ACCOUNT_BASE), ('Tenant', TENANT_BASE)]:
    print(f"\n{'='*80}")
    print(f"  TESTING {endpoint_type.upper()} ENDPOINT")
    print(f"  {base_url}")
    print("="*80)
    
    # ========================================================================
    # 1. BUSINESS DOMAINS
    # ========================================================================
    print("\n" + "="*80)
    print("  1. BUSINESS DOMAINS")
    print("="*80)
    
    # List domains
    domains = test_endpoint(
        "List Business Domains",
        "GET",
        f"{base_url}/datagovernance/catalog/businessDomains?api-version={API_VERSION}"
    )
    
    # Get specific domain (if any exist)
    if domains and isinstance(domains, dict) and domains.get('value'):
        domain_id = domains['value'][0].get('id')
        test_endpoint(
            "Get Business Domain",
            "GET",
            f"{base_url}/datagovernance/catalog/businessDomains/{domain_id}?api-version={API_VERSION}"
        )
    
    # ========================================================================
    # 2. BUSINESS CONCEPTS
    # ========================================================================
    print("\n" + "="*80)
    print("  2. BUSINESS CONCEPTS")
    print("="*80)
    
    # List concepts
    concepts = test_endpoint(
        "List Business Concepts",
        "GET",
        f"{base_url}/datagovernance/catalog/businessConcepts?api-version={API_VERSION}"
    )
    
    # ========================================================================
    # 3. DATA PRODUCTS
    # ========================================================================
    print("\n" + "="*80)
    print("  3. DATA PRODUCTS")
    print("="*80)
    
    # List data products
    products = test_endpoint(
        "List Data Products",
        "GET",
        f"{base_url}/datagovernance/catalog/dataProducts?api-version={API_VERSION}"
    )
    
    # Get specific product (if any exist)
    if products and isinstance(products, dict) and products.get('value'):
        product_id = products['value'][0].get('id')
        test_endpoint(
            "Get Data Product",
            "GET",
            f"{base_url}/datagovernance/catalog/dataProducts/{product_id}?api-version={API_VERSION}"
        )
    
    # ========================================================================
    # 4. DATA ASSETS
    # ========================================================================
    print("\n" + "="*80)
    print("  4. DATA ASSETS")
    print("="*80)
    
    # List data assets
    assets = test_endpoint(
        "List Data Assets",
        "GET",
        f"{base_url}/datagovernance/catalog/dataAssets?api-version={API_VERSION}"
    )
    
    # ========================================================================
    # 5. GLOSSARIES
    # ========================================================================
    print("\n" + "="*80)
    print("  5. GLOSSARIES")
    print("="*80)
    
    # List glossaries
    glossaries = test_endpoint(
        "List Glossaries",
        "GET",
        f"{base_url}/datagovernance/catalog/glossaries?api-version={API_VERSION}"
    )
    
    # ========================================================================
    # 6. BUSINESS POLICIES
    # ========================================================================
    print("\n" + "="*80)
    print("  6. BUSINESS POLICIES")
    print("="*80)
    
    # List policies
    policies = test_endpoint(
        "List Business Policies",
        "GET",
        f"{base_url}/datagovernance/catalog/businessPolicies?api-version={API_VERSION}"
    )
    
    # ========================================================================
    # 7. LINEAGE
    # ========================================================================
    print("\n" + "="*80)
    print("  7. LINEAGE")
    print("="*80)
    
    # Query lineage (need asset ID)
    if assets and isinstance(assets, dict) and assets.get('value'):
        asset_id = assets['value'][0].get('id')
        test_endpoint(
            "Get Asset Lineage",
            "GET",
            f"{base_url}/datagovernance/catalog/lineage/{asset_id}?api-version={API_VERSION}"
        )

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*80)
print("  UNIFIED CATALOG API SUMMARY")
print("="*80)
print("""
Unified Catalog API (2025-09-15-preview) Operation Groups:

1. Business Domains
   - List/Get/Create/Update/Delete governance domains
   - Assign concepts and policies to domains
   
2. Business Concepts  
   - Manage business concepts and relationships
   - Link concepts to domains and assets
   
3. Data Products
   - Register and manage data products
   - Define product contracts and SLAs
   - Track product versions and owners
   
4. Data Assets
   - Register and manage data assets
   - Link assets to products and domains
   - Track asset metadata and classifications
   
5. Glossaries
   - Manage business glossaries
   - Create terms and categories
   - Link glossary terms to assets
   
6. Business Policies
   - Define data governance policies
   - Enforce policy rules
   - Track policy compliance
   
7. Lineage
   - Track data lineage between assets
   - Visualize data flows
   - Impact analysis

API AVAILABILITY:
If you see 403 errors, the Unified Catalog API is not yet activated.

To activate:
1. Contact Azure support to enable Unified Catalog preview
2. Or wait for general availability
3. In the meantime, use Atlas API v2 for entity management

Working Alternative APIs:
- Atlas API v2: /catalog/api/atlas/v2 (entities, typedef, glossary)
- Search API: /catalog/api/search/query (search entities)
- Scan API: /scan (data source scanning)
""")
print("="*80)

# Additional endpoint checks
print("\n" + "="*80)
print("  ADDITIONAL CHECKS")
print("="*80)

# Check if we can create a business domain
print("\n📋 Test: Create Business Domain")
print("   (This will fail if API not activated)")

create_domain_body = {
    "name": "Test Domain",
    "description": "Test domain created via API",
    "properties": {}
}

test_endpoint(
    "Create Business Domain",
    "POST",
    f"{ACCOUNT_BASE}/datagovernance/catalog/businessDomains?api-version={API_VERSION}",
    create_domain_body
)

# Check data product creation
print("\n📋 Test: Create Data Product")
print("   (This will fail if API not activated)")

create_product_body = {
    "name": "Test Data Product",
    "description": "Test product created via API",
    "properties": {
        "owner": "test@example.com"
    }
}

test_endpoint(
    "Create Data Product",
    "POST",
    f"{ACCOUNT_BASE}/datagovernance/catalog/dataProducts?api-version={API_VERSION}",
    create_product_body
)

print("\n" + "="*80)
print("  TEST COMPLETE")
print("="*80)
