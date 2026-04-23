"""
SET GOVERNANCE DOMAIN ATTRIBUTES
=================================
Sätter governanceDomain attribute explicit på alla data products

Kör med: python scripts/set_governance_attributes.py
"""

import requests
import json
from azure.identity import AzureCliCredential
from datetime import datetime

PURVIEW_ACCOUNT = "https://prviewacc.purview.azure.com"
ATLAS_API = f"{PURVIEW_ACCOUNT}/catalog/api/atlas/v2"

# Product -> Domain mapping
PRODUCT_DOMAIN_MAP = {
    "BrainChild Barncancerforskning": "Genomics & Precision Medicine",
    "Klinisk Patientanalys": "Clinical Data Management",
    "ML Feature Store": "ML & Analytics",
    "OMOP Forskningsdata": "Clinical Data Management"
}

def get_headers():
    """Get authentication headers"""
    credential = AzureCliCredential(process_timeout=30)
    token = credential.get_token('https://purview.azure.net/.default')
    return {
        'Authorization': f'Bearer {token.token}',
        'Content-Type': 'application/json'
    }

def main():
    print("=" * 80)
    print("  SET GOVERNANCE DOMAIN ATTRIBUTES")
    print("=" * 80 + "\n")
    
    headers = get_headers()
    
    # Get all data products
    search_body = {
        "keywords": "*",
        "limit": 100,
        "filter": {"entityType": "healthcare_data_product"}
    }
    
    response = requests.post(
        f"{PURVIEW_ACCOUNT}/catalog/api/search/query?api-version=2022-08-01-preview",
        headers=headers,
        json=search_body,
        timeout=30
    )
    
    if response.status_code != 200:
        print(f"❌ Search failed: {response.status_code}")
        return 1
    
    products = response.json().get('value', [])
    print(f"✅ Found {len(products)} products\n")
    
    updated_count = 0
    
    for product in products:
        product_name = product.get('name')
        product_guid = product.get('id')
        
        if product_name not in PRODUCT_DOMAIN_MAP:
            print(f"⚠️  {product_name} - No domain mapping")
            continue
        
        domain_name = PRODUCT_DOMAIN_MAP[product_name]
        
        print(f"📦 {product_name}")
        print(f"   Setting domain: {domain_name}")
        
        try:
            # Get full entity
            get_resp = requests.get(
                f"{ATLAS_API}/entity/guid/{product_guid}",
                headers=headers,
                timeout=15
            )
            
            if get_resp.status_code != 200:
                print(f"   ❌ Failed to get entity: {get_resp.status_code}")
                continue
            
            entity_data = get_resp.json()
            entity = entity_data.get('entity', {})
            
            # Update attributes
            if 'attributes' not in entity:
                entity['attributes'] = {}
            
            entity['attributes']['governanceDomain'] = domain_name
            entity['attributes']['domainOwner'] = "Data Governance Team"
            
            # Update via bulk API
            bulk_body = {"entities": [entity]}
            
            update_resp = requests.post(
                f"{ATLAS_API}/entity/bulk",
                headers=headers,
                json=bulk_body,
                timeout=30
            )
            
            if update_resp.status_code == 200:
                result = update_resp.json()
                
                # Verify the update
                verify_resp = requests.get(
                    f"{ATLAS_API}/entity/guid/{product_guid}",
                    headers=headers,
                    timeout=15
                )
                
                if verify_resp.status_code == 200:
                    verified = verify_resp.json().get('entity', {})
                    set_domain = verified.get('attributes', {}).get('governanceDomain')
                    
                    if set_domain == domain_name:
                        print(f"   ✅ Verified: governanceDomain = {set_domain}")
                        updated_count += 1
                    else:
                        print(f"   ⚠️  Attribute set but verification shows: {set_domain}")
                else:
                    print(f"   ✅ Updated (verification skipped)")
                    updated_count += 1
            else:
                print(f"   ❌ Update failed: {update_resp.status_code}")
                print(f"      Response: {update_resp.text[:300]}")
        
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        print()
    
    print("=" * 80)
    print(f"  DONE: {updated_count}/{len(products)} updated")
    print("=" * 80)
    
    return 0

if __name__ == "__main__":
    exit(main())
