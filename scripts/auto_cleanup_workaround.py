#!/usr/bin/env python3
"""
Non-interactive cleanup of old glossary term workaround.
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
    
    r = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=15)
    glossary_list = r.json() if r.status_code == 200 else []
    if not isinstance(glossary_list, list):
        glossary_list = [glossary_list]
    
    glossary_guid = glossary_list[0].get("guid")
    
    r2 = requests.get(
        f"{ATLAS_API}/glossary/{glossary_guid}/terms?limit=200&offset=0",
        headers=headers,
        timeout=30
    )
    
    if r2.status_code != 200:
        print(f"❌ Failed to get terms: {r2.status_code}")
        return []
    
    all_terms = r2.json()
    
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
    
    products = [
        {"name": "BrainChild Barncancerforskning", "guid": "f8fe756c-6987-41ac-ab90-451237b946d5"},
        {"name": "Klinisk Patientanalys", "guid": "e7010e17-8987-4c31-af29-b06fcf4b2142"},
        {"name": "ML Feature Store", "guid": "88b9bc57-41f8-4cc7-8f73-5f5f768ee1fc"},
        {"name": "OMOP Forskningsdata", "guid": "3cc1c10e-4c78-41cf-bd47-5269f18f2e72"}
    ]
    
    for product in products:
        print(f"\n📦 {product['name']}")
        
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
        
        new_meanings = [
            m for m in meanings 
            if m.get("guid") not in domain_term_guids
        ]
        
        removed_count = len(meanings) - len(new_meanings)
        
        if removed_count == 0:
            print(f"   ℹ️  No domain term meanings found")
            continue
        
        print(f"   🔧 Removing {removed_count} domain term meaning(s)")
        
        entity["relationshipAttributes"]["meanings"] = new_meanings
        
        update_body = {"entities": [entity]}
        
        r2 = requests.post(
            f"{ATLAS_API}/entity/bulk",
            headers=headers,
            json=update_body,
            timeout=30
        )
        
        if r2.status_code == 200:
            print(f"   ✅ Updated successfully")
        else:
            print(f"   ❌ Failed to update: {r2.status_code}")

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
            print(f"   ❌ Failed: {r.status_code}")

def main():
    print("\n")
    print("="*80)
    print("  AUTO-CLEANUP: GOVERNANCE DOMAIN WORKAROUND")
    print("="*80)
    print()
    
    old_terms = get_old_domain_terms()
    
    if not old_terms:
        print("\n✅ No old domain terms found - already cleaned!")
        return
    
    old_term_guids = [t.get("guid") for t in old_terms if t.get("guid")]
    remove_meanings_from_products(old_term_guids)
    delete_domain_terms(old_terms)
    
    print("\n" + "="*80)
    print("  CLEANUP COMPLETE")
    print("="*80)
    print("✅ Old workaround removed")
    print("✅ Data products cleaned")
    print("\n📋 Next: Create governance domains in Portal UI")
    print("="*80)

if __name__ == "__main__":
    main()
