#!/usr/bin/env python3
"""
Cleanup old glossary term workaround for governance domains.
Remove CDM, GPM, CR, MLA terms and their meanings links from products.
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

def get_old_domain_terms():
    """Find old domain workaround terms."""
    headers = get_headers()
    
    print("="*80)
    print("  FINDING OLD DOMAIN WORKAROUND TERMS")
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
        return []
    
    all_terms = r2.json()
    
    # Filter for domain terms (CDM, GPM, CR, MLA)
    domain_abbrs = ["CDM", "GPM", "CR", "MLA"]
    old_domain_terms = [
        t for t in all_terms 
        if any(t.get("name", "").endswith(f"({abbr})") for abbr in domain_abbrs)
        or t.get("name", "").startswith("Domain:")
    ]
    
    print(f"\nFound {len(old_domain_terms)} old domain workaround terms:")
    for term in old_domain_terms:
        name = term.get("name", "?")
        guid = term.get("guid", "?")
        print(f"   - {name} (GUID: {guid})")
    
    return old_domain_terms

def remove_meanings_from_products(domain_term_guids):
    """Remove meanings links from data products."""
    headers = get_headers()
    
    print("\n" + "="*80)
    print("  REMOVING MEANINGS FROM DATA PRODUCTS")
    print("="*80)
    
    # Our 4 data products
    products = [
        {"name": "BrainChild Barncancerforskning", "guid": "f8fe756c-6987-41ac-ab90-451237b946d5"},
        {"name": "Klinisk Patientanalys", "guid": "e7010e17-8987-4c31-af29-b06fcf4b2142"},
        {"name": "ML Feature Store", "guid": "88b9bc57-41f8-4cc7-8f73-5f5f768ee1fc"},
        {"name": "OMOP Forskningsdata", "guid": "3cc1c10e-4c78-41cf-bd47-5269f18f2e72"}
    ]
    
    for product in products:
        print(f"\n📦 {product['name']}")
        
        # Get current product
        r = requests.get(
            f"{ATLAS_API}/entity/guid/{product['guid']}",
            headers=headers,
            timeout=15
        )
        
        if r.status_code != 200:
            print(f"   ❌ Failed to get product: {r.status_code}")
            continue
        
        entity = r.json().get("entity", {})
        meanings = entity.get("relationshipAttributes", {}).get("meanings", [])
        
        if not meanings:
            print(f"   ℹ️  No meanings to remove")
            continue
        
        # Filter out old domain term meanings
        new_meanings = [
            m for m in meanings 
            if m.get("guid") not in domain_term_guids
        ]
        
        removed_count = len(meanings) - len(new_meanings)
        
        if removed_count == 0:
            print(f"   ℹ️  No domain term meanings found")
            continue
        
        print(f"   🔧 Removing {removed_count} domain term meaning(s)")
        
        # Update entity with cleaned meanings
        entity["relationshipAttributes"]["meanings"] = new_meanings
        
        update_body = {
            "entities": [entity]
        }
        
        r2 = requests.post(
            f"{ATLAS_API}/entity/bulk",
            headers=headers,
            json=update_body,
            timeout=30
        )
        
        if r2.status_code == 200:
            print(f"   ✅ Updated successfully")
        else:
            print(f"   ❌ Failed to update: {r2.status_code} - {r2.text[:200]}")

def delete_domain_terms(domain_terms):
    """Delete old domain glossary terms."""
    headers = get_headers()
    
    print("\n" + "="*80)
    print("  DELETING OLD DOMAIN TERMS")
    print("="*80)
    
    for term in domain_terms:
        name = term.get("name", "?")
        guid = term.get("guid")
        
        if not guid:
            print(f"\n❌ No GUID for term: {name}")
            continue
        
        print(f"\n🗑️  Deleting: {name}")
        
        r = requests.delete(
            f"{ATLAS_API}/glossary/term/{guid}",
            headers=headers,
            timeout=15
        )
        
        if r.status_code in [200, 204]:
            print(f"   ✅ Deleted successfully")
        elif r.status_code == 404:
            print(f"   ℹ️  Already deleted or not found")
        else:
            print(f"   ❌ Failed: {r.status_code} - {r.text[:200]}")

def check_governance_domains_category():
    """Check if Governance Domains category can be deleted."""
    headers = get_headers()
    
    print("\n" + "="*80)
    print("  CHECKING GOVERNANCE DOMAINS CATEGORY")
    print("="*80)
    
    # Get glossary
    r = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=15)
    glossary_list = r.json() if r.status_code == 200 else []
    if not isinstance(glossary_list, list):
        glossary_list = [glossary_list]
    
    glossary_guid = glossary_list[0].get("guid")
    
    # Get glossary details with categories
    r2 = requests.get(f"{ATLAS_API}/glossary/{glossary_guid}", headers=headers, timeout=15)
    
    if r2.status_code != 200:
        print(f"❌ Failed to get glossary: {r2.status_code}")
        return
    
    glossary = r2.json()
    categories = glossary.get("categories", [])
    
    gov_domain_cat = None
    for cat in categories:
        if "governance" in cat.get("displayText", "").lower() and "domain" in cat.get("displayText", "").lower():
            gov_domain_cat = cat
            break
    
    if not gov_domain_cat:
        print("ℹ️  No 'Governance Domains' category found")
        return
    
    cat_guid = gov_domain_cat.get("categoryGuid")
    cat_name = gov_domain_cat.get("displayText", "?")
    
    print(f"\nFound category: {cat_name} (GUID: {cat_guid})")
    
    # Get category details to check if empty
    r3 = requests.get(f"{ATLAS_API}/glossary/category/{cat_guid}", headers=headers, timeout=15)
    
    if r3.status_code != 200:
        print(f"❌ Failed to get category details: {r3.status_code}")
        return
    
    category = r3.json()
    terms = category.get("terms", [])
    
    if len(terms) == 0:
        print(f"✅ Category is empty ({len(terms)} terms)")
        print(f"\nℹ️  You can manually delete this category in Portal UI:")
        print(f"   Enterprise glossary → Categories → {cat_name} → Delete")
    else:
        print(f"⚠️  Category still has {len(terms)} term(s)")
        print("   Cannot delete until empty")

def main():
    print("\n")
    print("="*80)
    print("  CLEANUP OLD GOVERNANCE DOMAIN WORKAROUND")
    print("="*80)
    print("  This will remove:")
    print("  1. Old domain glossary terms (CDM, GPM, CR, MLA)")
    print("  2. Meanings relationships from data products")
    print("  3. Check if Governance Domains category can be removed")
    print("="*80)
    
    response = input("\nProceed with cleanup? (yes/no): ")
    
    if response.lower() not in ["yes", "y"]:
        print("\n❌ Cleanup cancelled")
        return
    
    print("\n")
    
    # 1. Find old terms
    old_terms = get_old_domain_terms()
    
    if not old_terms:
        print("\n✅ No old domain terms found - already cleaned!")
        return
    
    # 2. Remove meanings from products
    old_term_guids = [t.get("guid") for t in old_terms if t.get("guid")]
    remove_meanings_from_products(old_term_guids)
    
    # 3. Delete old terms
    delete_domain_terms(old_terms)
    
    # 4. Check category
    check_governance_domains_category()
    
    print("\n" + "="*80)
    print("  CLEANUP COMPLETE")
    print("="*80)
    print("✅ Old workaround removed")
    print("✅ Data products cleaned")
    print("\n📋 Next steps:")
    print("   1. Create real governance domains in Portal UI")
    print("   2. Link data products to domains")
    print("   3. Run: python scripts/verify_domains_complete.py")
    print("="*80)

if __name__ == "__main__":
    main()
