#!/usr/bin/env python3
"""
Discover how native governance domains are stored in Purview.
Screenshots show 4 domains under Data products but 0 in Enterprise glossary.
"""

import requests
import json
from azure.identity import AzureCliCredential

PURVIEW_ACCOUNT = "prviewacc"
BASE_URL = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"

def get_headers():
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token("https://purview.azure.net/.default").token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def explore_unified_catalog_domains():
    """Try ALL possible Unified Catalog API endpoints for domains."""
    headers = get_headers()
    
    print("="*80)
    print("  UNIFIED CATALOG - GOVERNANCE DOMAINS")
    print("="*80)
    
    # All known API versions
    api_versions = [
        "2025-09-15-preview",
        "2024-03-01-preview", 
        "2023-09-01-preview",
        "2023-03-01-preview"
    ]
    
    endpoints = [
        "/datagovernance/catalog/domains",
        "/datagovernance/catalog/governanceDomains",
        "/datagovernance/governance/domains",
        "/catalog/governance/domains",
        "/governance/domains"
    ]
    
    for endpoint in endpoints:
        for api_ver in api_versions:
            url = f"{BASE_URL}{endpoint}?api-version={api_ver}"
            try:
                print(f"\n📍 {endpoint} ({api_ver})")
                r = requests.get(url, headers=headers, timeout=15)
                print(f"   Status: {r.status_code}")
                
                if r.status_code == 200:
                    data = r.json()
                    print(f"   ✅ SUCCESS!")
                    print(f"   Response: {json.dumps(data, indent=2)[:500]}")
                    return data
                elif r.status_code in [401, 403]:
                    print(f"   🔒 Auth issue: {r.text[:100]}")
                elif r.status_code == 404:
                    print(f"   ❌ Not found")
                else:
                    print(f"   ⚠️  {r.text[:100]}")
            except Exception as e:
                print(f"   ❌ Error: {e}")
    
    return None

def search_for_domain_entities():
    """Search Atlas for any entity types that might be domains."""
    headers = get_headers()
    atlas_url = f"{BASE_URL}/catalog/api/atlas/v2"
    
    print("\n" + "="*80)
    print("  ATLAS SEARCH - DOMAIN ENTITIES")
    print("="*80)
    
    # Search for various domain-related keywords
    keywords = ["domain", "governance", "Data & Analytics", "Forskning"]
    
    for keyword in keywords:
        print(f"\n🔍 Searching for: {keyword}")
        
        body = {
            "keywords": keyword,
            "limit": 10,
            "filter": {}
        }
        
        try:
            r = requests.post(
                f"{BASE_URL}/catalog/api/search/query?api-version=2022-08-01-preview",
                headers=headers,
                json=body,
                timeout=15
            )
            
            if r.status_code == 200:
                results = r.json().get("value", [])
                print(f"   Found {len(results)} results")
                
                for res in results[:3]:
                    name = res.get("name", "?")
                    etype = res.get("entityType", "?")
                    qname = res.get("qualifiedName", "?")
                    print(f"   - {name} ({etype}) [{qname}]")
                    
        except Exception as e:
            print(f"   ❌ Error: {e}")

def get_all_entity_types():
    """List all entity type definitions to find domain types."""
    headers = get_headers()
    atlas_url = f"{BASE_URL}/catalog/api/atlas/v2"
    
    print("\n" + "="*80)
    print("  ATLAS ENTITY TYPES - DOMAIN RELATED")
    print("="*80)
    
    try:
        r = requests.get(f"{atlas_url}/types/typedefs", headers=headers, timeout=30)
        
        if r.status_code == 200:
            data = r.json()
            entity_defs = data.get("entityDefs", [])
            
            # Filter for domain-related types
            domain_types = [
                t for t in entity_defs 
                if "domain" in t.get("name", "").lower() 
                or "governance" in t.get("name", "").lower()
            ]
            
            print(f"\n✅ Found {len(domain_types)} domain-related entity types:")
            for t in domain_types:
                name = t.get("name", "?")
                desc = t.get("description", "")
                attrs = len(t.get("attributeDefs", []))
                print(f"\n   📦 {name}")
                print(f"      Description: {desc[:100]}")
                print(f"      Attributes: {attrs}")
                
                # Show attribute names
                attr_names = [a.get("name") for a in t.get("attributeDefs", [])]
                if attr_names:
                    print(f"      Fields: {', '.join(attr_names[:5])}")
            
            return domain_types
        else:
            print(f"❌ Failed: {r.status_code}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    return []

def check_data_product_domain_links():
    """Check how data products reference their domains."""
    headers = get_headers()
    atlas_url = f"{BASE_URL}/catalog/api/atlas/v2"
    
    print("\n" + "="*80)
    print("  DATA PRODUCT DOMAIN REFERENCES")
    print("="*80)
    
    # Get one of our data products
    product_guids = [
        "f8fe756c-6987-41ac-ab90-451237b946d5",  # BrainChild
        "e7010e17-8987-4c31-af29-b06fcf4b2142",  # Klinisk Patientanalys
    ]
    
    for guid in product_guids:
        try:
            print(f"\n📦 Checking product: {guid}")
            r = requests.get(
                f"{atlas_url}/entity/guid/{guid}",
                headers=headers,
                timeout=15
            )
            
            if r.status_code == 200:
                entity = r.json().get("entity", {})
                name = entity.get("attributes", {}).get("name", "?")
                print(f"   Name: {name}")
                
                # Check for domain-related attributes
                attrs = entity.get("attributes", {})
                rel_attrs = entity.get("relationshipAttributes", {})
                
                domain_keys = [k for k in attrs.keys() if "domain" in k.lower()]
                print(f"   Domain attributes: {domain_keys}")
                
                for key in domain_keys:
                    val = attrs.get(key)
                    print(f"      {key}: {val}")
                
                # Check relationship attributes
                domain_rels = [k for k in rel_attrs.keys() if "domain" in k.lower()]
                print(f"   Domain relationships: {domain_rels}")
                
                for key in domain_rels:
                    val = rel_attrs.get(key)
                    if isinstance(val, dict):
                        print(f"      {key}: {val.get('displayText', val.get('guid', val))}")
                    else:
                        print(f"      {key}: {val}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")

def main():
    print("\n")
    print("="*80)
    print("  DISCOVERING NATIVE GOVERNANCE DOMAINS")
    print("="*80)
    print(f"  Purview Account: {PURVIEW_ACCOUNT}")
    print(f"  Screenshots show: 4 domains under Data products")
    print(f"  But: 0 domains in Enterprise glossary")
    print("="*80)
    print()
    
    # Try all approaches
    explore_unified_catalog_domains()
    search_for_domain_entities()
    domain_types = get_all_entity_types()
    check_data_product_domain_links()
    
    print("\n" + "="*80)
    print("  ANALYSIS")
    print("="*80)
    print("Based on the discoveries above, we should be able to determine:")
    print("1. The entity type name for governance domains")
    print("2. The API endpoint to create/list domains")
    print("3. How data products reference their domains")
    print("="*80)

if __name__ == "__main__":
    main()
