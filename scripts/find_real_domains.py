#!/usr/bin/env python3
"""
Search for Purview_DataDomain entities - the native governance domains.
"""

import requests
import json
from azure.identity import AzureCliCredential

PURVIEW_ACCOUNT = "prviewacc"
BASE_URL = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
ATLAS_API = f"{BASE_URL}/catalog/api/atlas/v2"

def get_headers():
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token("https://purview.azure.net/.default").token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def search_data_domains():
    """Search for all Purview_DataDomain entities."""
    headers = get_headers()
    
    print("="*80)
    print("  SEARCHING FOR PURVIEW_DATADOMAIN ENTITIES")
    print("="*80)
    
    # Method 1: Search by entity type
    print("\n📍 Method 1: Search API with entityType filter")
    
    body = {
        "keywords": "*",
        "limit": 50,
        "filter": {
            "entityType": "Purview_DataDomain"
        }
    }
    
    try:
        r = requests.post(
            f"{BASE_URL}/catalog/api/search/query?api-version=2022-08-01-preview",
            headers=headers,
            json=body,
            timeout=30
        )
        
        print(f"   Status: {r.status_code}")
        
        if r.status_code == 200:
            results = r.json().get("value", [])
            print(f"   ✅ Found {len(results)} Purview_DataDomain entities:\n")
            
            for i, domain in enumerate(results, 1):
                name = domain.get("name", "?")
                guid = domain.get("id", "?")
                qual_name = domain.get("qualifiedName", "?")
                desc = domain.get("description", "")
                
                print(f"   {i}. {name}")
                print(f"      GUID: {guid}")
                print(f"      QualifiedName: {qual_name}")
                if desc:
                    print(f"      Description: {desc[:100]}")
                print()
                
            return results
        else:
            print(f"   ❌ Error: {r.text[:200]}")
            
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    # Method 2: DSL query
    print("\n📍 Method 2: DSL Query")
    
    dsl_body = {
        "query": "from Purview_DataDomain"
    }
    
    try:
        r2 = requests.post(
            f"{ATLAS_API}/search/dsl",
            headers=headers,
            json=dsl_body,
            timeout=30
        )
        
        print(f"   Status: {r2.status_code}")
        
        if r2.status_code == 200:
            data = r2.json()
            entities = data.get("entities", [])
            print(f"   ✅ Found {len(entities)} entities via DSL:\n")
            
            for entity in entities:
                attrs = entity.get("attributes", {})
                name = attrs.get("name", "?")
                guid = entity.get("guid", "?")
                print(f"   - {name} (GUID: {guid})")
            
            return entities
        else:
            print(f"   ⚠️  Error: {r2.text[:200]}")
            
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    
    return []

def get_domain_details(guid):
    """Get full details of a domain entity."""
    headers = get_headers()
    
    print(f"\n{'='*80}")
    print(f"  DOMAIN DETAILS: {guid}")
    print(f"{'='*80}")
    
    try:
        r = requests.get(
            f"{ATLAS_API}/entity/guid/{guid}",
            headers=headers,
            timeout=15
        )
        
        if r.status_code == 200:
            data = r.json()
            entity = data.get("entity", {})
            
            attrs = entity.get("attributes", {})
            rel_attrs = entity.get("relationshipAttributes", {})
            
            print(f"\n📋 Attributes:")
            for key, val in attrs.items():
                if val and key != "qualifiedName":
                    print(f"   {key}: {val}")
            
            print(f"\n🔗 Relationship Attributes:")
            for key, val in rel_attrs.items():
                if val:
                    if isinstance(val, list):
                        print(f"   {key}: [{len(val)} items]")
                        for item in val[:3]:
                            if isinstance(item, dict):
                                print(f"      - {item.get('displayText', item.get('guid', '?'))}")
                    elif isinstance(val, dict):
                        print(f"   {key}: {val.get('displayText', val.get('guid', '?'))}")
                    else:
                        print(f"   {key}: {val}")
            
            return entity
        else:
            print(f"❌ Error: {r.status_code} - {r.text[:200]}")
            
    except Exception as e:
        print(f"❌ Exception: {e}")
    
    return None

def check_data_product_domain_link():
    """Check how data products link to domains."""
    headers = get_headers()
    
    print(f"\n{'='*80}")
    print(f"  DATA PRODUCT DOMAIN LINKS")
    print(f"{'='*80}")
    
    # Get BrainChild product
    guid = "f8fe756c-6987-41ac-ab90-451237b946d5"
    
    try:
        r = requests.get(
            f"{ATLAS_API}/entity/guid/{guid}",
            headers=headers,
            timeout=15
        )
        
        if r.status_code == 200:
            entity = r.json().get("entity", {})
            attrs = entity.get("attributes", {})
            rel_attrs = entity.get("relationshipAttributes", {})
            
            print(f"\n📦 BrainChild Barncancerforskning")
            print(f"\n   All attributes:")
            for key in sorted(attrs.keys()):
                val = attrs[key]
                if "domain" in key.lower() or "governance" in key.lower():
                    print(f"   ⭐ {key}: {val}")
                elif val and key not in ["qualifiedName", "description"]:
                    print(f"      {key}: {str(val)[:80]}")
            
            print(f"\n   All relationship attributes:")
            for key in sorted(rel_attrs.keys()):
                val = rel_attrs[key]
                if "domain" in key.lower() or "governance" in key.lower():
                    print(f"   ⭐ {key}: {val}")
                elif val:
                    print(f"      {key}: {str(val)[:80] if not isinstance(val, (list, dict)) else type(val).__name__}")
                    
    except Exception as e:
        print(f"❌ Exception: {e}")

def main():
    print("\n")
    print("="*80)
    print("  PURVIEW NATIVE GOVERNANCE DOMAINS DISCOVERY")
    print("="*80)
    print(f"  Account: {PURVIEW_ACCOUNT}")
    print(f"  Entity Type: Purview_DataDomain")
    print("="*80)
    print()
    
    # Search for domains
    domains = search_data_domains()
    
    # Get details of each domain
    if domains:
        print(f"\n{'='*80}")
        print("  DETAILED DOMAIN INSPECTION")
        print(f"{'='*80}")
        
        for domain in domains[:5]:  # First 5
            guid = domain.get("id") or domain.get("guid")
            if guid:
                get_domain_details(guid)
    
    # Check data product links
    check_data_product_domain_link()
    
    print("\n" + "="*80)
    print("  CONCLUSION")
    print("="*80)
    print("If we found Purview_DataDomain entities above, those are the")
    print("native governance domains shown in the Portal UI.")
    print("\nTo link data products to domains, we need to identify:")
    print("1. The relationship name used to link products to domains")
    print("2. Whether it's done via attributes or relationships")
    print("3. How to update our existing products with domain links")
    print("="*80)

if __name__ == "__main__":
    main()
