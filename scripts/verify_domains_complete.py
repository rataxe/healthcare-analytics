#!/usr/bin/env python3
"""
Verify that governance domains have been created manually in Portal UI
and that data products are properly linked.
"""

import requests
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

def search_for_native_domains():
    """Search for Purview_DataDomain entities."""
    headers = get_headers()
    
    print("="*80)
    print("  SEARCHING FOR NATIVE GOVERNANCE DOMAINS")
    print("="*80)
    
    # Method 1: Search API
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
        
        if r.status_code == 200:
            results = r.json().get("value", [])
            
            print(f"\n✅ Found {len(results)} Purview_DataDomain entities:\n")
            
            if len(results) == 0:
                print("   ⚠️  No governance domains found!")
                print("   ❌ Domains have NOT been created yet")
                print("\n   📋 Next step: Create domains manually in Portal UI")
                print("      URL: https://purview.microsoft.com/governance/domains")
                return []
            
            for i, domain in enumerate(results, 1):
                name = domain.get("name", "?")
                guid = domain.get("id", "?")
                desc = domain.get("description", "")
                
                print(f"   {i}. {name}")
                print(f"      GUID: {guid}")
                if desc:
                    print(f"      Description: {desc[:80]}")
                print()
            
            return results
        else:
            print(f"   ❌ Search failed: {r.status_code}")
            return []
            
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        return []

def check_data_product_domains():
    """Check if data products have domain links."""
    headers = get_headers()
    
    print("="*80)
    print("  CHECKING DATA PRODUCT DOMAIN LINKS")
    print("="*80)
    
    products = [
        {"name": "BrainChild Barncancerforskning", "guid": "f8fe756c-6987-41ac-ab90-451237b946d5", "expected_domain": "Forskning & Genomik"},
        {"name": "Klinisk Patientanalys", "guid": "e7010e17-8987-4c31-af29-b06fcf4b2142", "expected_domain": "Klinisk Vård"},
        {"name": "ML Feature Store", "guid": "88b9bc57-41f8-4cc7-8f73-5f5f768ee1fc", "expected_domain": "Data & Analytics"},
        {"name": "OMOP Forskningsdata", "guid": "3cc1c10e-4c78-41cf-bd47-5269f18f2e72", "expected_domain": "Interoperabilitet & Standarder"}
    ]
    
    linked_count = 0
    
    for product in products:
        print(f"\n📦 {product['name']}")
        
        try:
            r = requests.get(
                f"{ATLAS_API}/entity/guid/{product['guid']}",
                headers=headers,
                timeout=15
            )
            
            if r.status_code != 200:
                print(f"   ❌ Failed to get product: {r.status_code}")
                continue
            
            entity = r.json().get("entity", {})
            attrs = entity.get("attributes", {})
            rel_attrs = entity.get("relationshipAttributes", {})
            
            # Check for domain attribute
            domain_attr = None
            for key, val in attrs.items():
                if "domain" in key.lower() and val:
                    domain_attr = val
                    break
            
            # Check for domain relationship
            domain_rel = None
            for key, val in rel_attrs.items():
                if "domain" in key.lower() and val:
                    domain_rel = val
                    break
            
            if domain_attr or domain_rel:
                linked_count += 1
                domain_name = domain_attr or (domain_rel.get("displayText") if isinstance(domain_rel, dict) else "?")
                print(f"   ✅ Linked to domain: {domain_name}")
                
                if product['expected_domain'].lower() in str(domain_name).lower():
                    print(f"   ✅ Correct domain (expected: {product['expected_domain']})")
                else:
                    print(f"   ⚠️  Domain mismatch (expected: {product['expected_domain']})")
            else:
                print(f"   ❌ NOT linked to any domain")
                print(f"   📋 Expected domain: {product['expected_domain']}")
                
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    print(f"\n{'='*80}")
    print(f"   LINKED PRODUCTS: {linked_count}/4 ({linked_count*25}%)")
    print(f"{'='*80}")
    
    return linked_count

def check_old_glossary_workaround():
    """Check if old glossary term workaround is still present."""
    headers = get_headers()
    
    print("\n" + "="*80)
    print("  CHECKING FOR OLD GLOSSARY WORKAROUND")
    print("="*80)
    
    # Get glossary
    r = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=15)
    glossary_list = r.json() if r.status_code == 200 else []
    if not isinstance(glossary_list, list):
        glossary_list = [glossary_list]
    
    glossary_guid = glossary_list[0].get("guid")
    
    # Get all terms
    r2 = requests.get(
        f"{ATLAS_API}/glossary/{glossary_guid}/terms?limit=200&offset=0",
        headers=headers,
        timeout=30
    )
    
    if r2.status_code != 200:
        print(f"❌ Failed to get terms: {r2.status_code}")
        return
    
    all_terms = r2.json()
    
    # Filter for old domain terms
    domain_abbrs = ["CDM", "GPM", "CR", "MLA"]
    old_domain_terms = [
        t for t in all_terms 
        if any(t.get("name", "").endswith(f"({abbr})") for abbr in domain_abbrs)
    ]
    
    if len(old_domain_terms) == 0:
        print("\n✅ No old domain workaround terms found - cleanup complete!")
    else:
        print(f"\n⚠️  Found {len(old_domain_terms)} old domain terms still present:")
        for term in old_domain_terms:
            print(f"   - {term.get('name', '?')}")
        print("\n📋 Run cleanup script:")
        print("   python scripts/cleanup_old_domain_workaround.py")

def main():
    print("\n")
    print("="*80)
    print("  GOVERNANCE DOMAINS VERIFICATION")
    print("="*80)
    print(f"  Account: {PURVIEW_ACCOUNT}")
    print("="*80)
    print()
    
    # 1. Search for native domains
    domains = search_for_native_domains()
    
    # 2. Check product links
    linked_count = check_data_product_domains()
    
    # 3. Check old workaround
    check_old_glossary_workaround()
    
    # Final summary
    print("\n" + "="*80)
    print("  FINAL SUMMARY")
    print("="*80)
    
    if len(domains) == 4 and linked_count == 4:
        print("\n✅✅✅ GOVERNANCE DOMAINS SETUP COMPLETE! ✅✅✅\n")
        print("   ✅ 4 governance domains created")
        print("   ✅ 4 data products linked (100%)")
        print("\n🎉 Portal UI should now show:")
        print("   - Enterprise glossary → Governance domains tab: 4 domains")
        print("   - Data products → Explore by governance domain: All products")
        print("\n📋 Next steps:")
        print("   1. Verify in Portal UI: https://purview.microsoft.com")
        print("   2. Configure Fabric OneLake data source scan")
        print("   3. Run full governance audit")
        
    elif len(domains) > 0 and linked_count < 4:
        print("\n⚠️  PARTIAL COMPLETION\n")
        print(f"   ✅ {len(domains)} governance domains found")
        print(f"   ⚠️  Only {linked_count}/4 products linked")
        print("\n📋 Next steps:")
        print("   1. Open each product in Portal UI")
        print("   2. Edit → Properties → Select Governance Domain")
        print("   3. Save and re-run this script")
        
    elif len(domains) == 0:
        print("\n❌ GOVERNANCE DOMAINS NOT CREATED YET\n")
        print("   ❌ No Purview_DataDomain entities found")
        print("\n📋 Next steps:")
        print("   1. Follow guide: MANUAL_GOVERNANCE_DOMAINS_GUIDE.md")
        print("   2. Create 4 domains in Portal UI")
        print("   3. Link 4 data products")
        print("   4. Re-run this script")
        
    else:
        print("\n⚠️  UNEXPECTED STATE\n")
        print(f"   Domains found: {len(domains)}")
        print(f"   Products linked: {linked_count}/4")
        print("\n📋 Manual investigation required")
    
    print("="*80)

if __name__ == "__main__":
    main()
