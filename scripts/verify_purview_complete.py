"""
FINAL PURVIEW VERIFICATION
===========================
Verifierar att hela Purview-strukturen är komplett

Kör med: python scripts/verify_purview_complete.py
"""

import requests
import json
from azure.identity import AzureCliCredential
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

PURVIEW_ACCOUNT = "https://prviewacc.purview.azure.com"
ATLAS_API = f"{PURVIEW_ACCOUNT}/catalog/api/atlas/v2"

def get_headers():
    """Get authentication headers"""
    credential = AzureCliCredential(process_timeout=30)
    token = credential.get_token('https://purview.azure.net/.default')
    return {
        'Authorization': f'Bearer {token.token}',
        'Content-Type': 'application/json'
    }

def print_section(title):
    """Print section header"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")

# =============================================================================
# VERIFICATION CHECKS
# =============================================================================

def check_glossary_structure():
    """Check glossary, categories and terms"""
    print_section("GLOSSARY STRUCTURE")
    
    headers = get_headers()
    checks = {}
    
    # Get glossary
    response = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=15)
    if response.status_code != 200:
        print("❌ Cannot access glossary")
        return checks
    
    data = response.json()
    glossaries = data if isinstance(data, list) else [data]
    glossary = glossaries[0]
    glossary_guid = glossary['guid']
    
    print(f"✅ Glossary: {glossary.get('name')}")
    print(f"   GUID: {glossary_guid}")
    print(f"   Terms: {len(glossary.get('terms', []))}")
    print(f"   Categories: {len(glossary.get('categories', []))}")
    
    checks['glossary_exists'] = True
    checks['glossary_name'] = glossary.get('name')
    checks['total_terms'] = len(glossary.get('terms', []))
    checks['total_categories'] = len(glossary.get('categories', []))
    
    # Check for Governance Domains category
    categories = glossary.get('categories', [])
    gov_category = None
    for cat in categories:
        if 'governance' in cat.get('displayText', '').lower() and 'domain' in cat.get('displayText', '').lower():
            gov_category = cat
            break
    
    if gov_category:
        print(f"\n✅ Governance Domains Category found")
        print(f"   Name: {gov_category.get('displayText')}")
        print(f"   GUID: {gov_category.get('categoryGuid')}")
        checks['governance_category_exists'] = True
    else:
        print(f"\n❌ Governance Domains Category not found")
        checks['governance_category_exists'] = False
    
    # Get all terms
    response = requests.get(
        f"{ATLAS_API}/glossary/{glossary_guid}/terms?limit=200&offset=0",
        headers=headers,
        timeout=30
    )
    
    if response.status_code == 200:
        terms = response.json()
        
        # Find domain terms
        domain_terms = [t for t in terms if t.get('name', '').startswith('Domain:')]
        
        print(f"\n✅ Domain Terms: {len(domain_terms)}")
        for dt in domain_terms:
            print(f"   • {dt.get('displayText', dt.get('name'))}")
        
        checks['domain_terms_count'] = len(domain_terms)
        checks['domain_terms'] = [
            {
                'name': dt.get('displayText', dt.get('name')),
                'guid': dt.get('guid')
            }
            for dt in domain_terms
        ]
    
    return checks

def check_data_products():
    """Check data products and their relationships"""
    print_section("DATA PRODUCTS")
    
    headers = get_headers()
    checks = {}
    
    search_body = {
        "keywords": "*",
        "limit": 100,
        "filter": {"entityType": "healthcare_data_product"}
    }
    
    try:
        response = requests.post(
            f"{PURVIEW_ACCOUNT}/catalog/api/search/query?api-version=2022-08-01-preview",
            headers=headers,
            json=search_body,
            timeout=30
        )
        
        if response.status_code != 200:
            print(f"❌ Search failed: {response.status_code}")
            return checks
        
        products = response.json().get('value', [])
        print(f"✅ Data Products: {len(products)}\n")
        
        checks['total_products'] = len(products)
        checks['products'] = []
        
        for product in products:
            product_name = product.get('name')
            product_guid = product.get('id')
            
            print(f"📦 {product_name}")
            print(f"   GUID: {product_guid}")
            
            product_info = {
                'name': product_name,
                'guid': product_guid,
                'has_domain': False,
                'domain_name': None,
                'has_meanings': False,
                'meaning_count': 0
            }
            
            # Get full entity details
            try:
                entity_resp = requests.get(
                    f"{ATLAS_API}/entity/guid/{product_guid}",
                    headers=headers,
                    timeout=15
                )
                
                if entity_resp.status_code == 200:
                    entity_data = entity_resp.json()
                    entity = entity_data.get('entity', {})
                    
                    # Check governance domain attribute
                    domain_attr = entity.get('attributes', {}).get('governanceDomain')
                    if domain_attr:
                        print(f"   ✅ Domain: {domain_attr}")
                        product_info['has_domain'] = True
                        product_info['domain_name'] = domain_attr
                    else:
                        print(f"   ⚠️  No domain attribute")
                    
                    # Check meanings (glossary term relationships)
                    meanings = entity.get('relationshipAttributes', {}).get('meanings', [])
                    if meanings:
                        print(f"   ✅ Meanings: {len(meanings)} terms linked")
                        for meaning in meanings:
                            term_name = meaning.get('displayText', 'Unknown')
                            print(f"      • {term_name}")
                        product_info['has_meanings'] = True
                        product_info['meaning_count'] = len(meanings)
                    else:
                        print(f"   ⚠️  No meanings linked")
            
            except Exception as e:
                print(f"   ❌ Error getting entity details: {e}")
            
            checks['products'].append(product_info)
            print()
        
        return checks
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return checks

def check_classifications():
    """Check classifications (sensitivity labels)"""
    print_section("CLASSIFICATIONS")
    
    headers = get_headers()
    checks = {}
    
    try:
        response = requests.get(
            f"{ATLAS_API}/types/typedefs?type=classification",
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            classifications = data.get('classificationDefs', [])
            
            # Filter for custom classifications
            custom_classifications = [
                c for c in classifications
                if not c.get('name', '').startswith('MICROSOFT.')
            ]
            
            print(f"✅ Total classifications: {len(classifications)}")
            print(f"✅ Custom classifications: {len(custom_classifications)}\n")
            
            if custom_classifications:
                print("Custom Classifications:")
                for c in custom_classifications[:10]:
                    print(f"   • {c.get('name')}")
            
            checks['total_classifications'] = len(classifications)
            checks['custom_classifications'] = len(custom_classifications)
        else:
            print(f"❌ Failed to get classifications: {response.status_code}")
    
    except Exception as e:
        print(f"❌ Error: {e}")
    
    return checks

def check_fabric_onelake():
    """Check Fabric OneLake connectivity"""
    print_section("FABRIC ONELAKE")
    
    checks = {}
    
    WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
    LAKEHOUSE_GOLD_ID = "2960eef0-5de6-4117-80b1-6ee783cdaeec"
    
    try:
        credential = AzureCliCredential()
        token = credential.get_token('https://storage.azure.com/.default')
        headers = {'Authorization': f'Bearer {token.token}'}
        
        url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_GOLD_ID}/Files"
        params = {'resource': 'filesystem', 'recursive': 'false'}
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        print(f"Workspace ID: {WORKSPACE_ID}")
        print(f"Lakehouse Gold ID: {LAKEHOUSE_GOLD_ID}")
        print(f"Test URL: {url}\n")
        
        if response.status_code == 200:
            paths = response.json().get('paths', [])
            print(f"✅ Connection successful!")
            print(f"   Files found: {len(paths)}")
            checks['fabric_connected'] = True
            checks['fabric_files_found'] = len(paths)
        elif response.status_code == 403:
            print(f"❌ Access Denied (403)")
            print(f"   📝 mi-purview needs Fabric workspace access")
            checks['fabric_connected'] = False
            checks['fabric_error'] = 'Access Denied - MI not configured'
        else:
            print(f"⚠️  Unexpected status: {response.status_code}")
            checks['fabric_connected'] = False
            checks['fabric_error'] = f'HTTP {response.status_code}'
    
    except Exception as e:
        print(f"❌ Error: {e}")
        checks['fabric_connected'] = False
        checks['fabric_error'] = str(e)
    
    return checks

def generate_final_report(all_checks):
    """Generate final verification report"""
    print_section("FINAL VERIFICATION REPORT")
    
    # Calculate scores
    glossary_score = 0
    if all_checks.get('glossary', {}).get('glossary_exists'):
        glossary_score += 1
    if all_checks.get('glossary', {}).get('governance_category_exists'):
        glossary_score += 1
    if all_checks.get('glossary', {}).get('domain_terms_count', 0) >= 4:
        glossary_score += 1
    
    products_score = 0
    products = all_checks.get('products', {}).get('products', [])
    if products:
        products_with_domains = len([p for p in products if p.get('has_domain')])
        products_with_meanings = len([p for p in products if p.get('has_meanings')])
        
        if products_with_domains == len(products):
            products_score += 1
        if products_with_meanings == len(products):
            products_score += 1
    
    fabric_score = 1 if all_checks.get('fabric', {}).get('fabric_connected') else 0
    
    total_score = glossary_score + products_score + fabric_score
    max_score = 6
    
    # Print summary
    print(f"📊 Glossary Structure: {glossary_score}/3")
    print(f"   ✅ Glossary exists: {all_checks.get('glossary', {}).get('glossary_exists', False)}")
    print(f"   ✅ Governance category: {all_checks.get('glossary', {}).get('governance_category_exists', False)}")
    print(f"   ✅ Domain terms: {all_checks.get('glossary', {}).get('domain_terms_count', 0)} created")
    
    print(f"\n📦 Data Products: {products_score}/2")
    if products:
        products_with_domains = len([p for p in products if p.get('has_domain')])
        products_with_meanings = len([p for p in products if p.get('has_meanings')])
        print(f"   ✅ Products with domains: {products_with_domains}/{len(products)}")
        print(f"   ✅ Products with meanings: {products_with_meanings}/{len(products)}")
    
    print(f"\n🔌 Fabric OneLake: {fabric_score}/1")
    print(f"   {'✅' if fabric_score else '❌'} Connected: {all_checks.get('fabric', {}).get('fabric_connected', False)}")
    
    print(f"\n" + "=" * 80)
    print(f"  TOTAL SCORE: {total_score}/{max_score} ({round(total_score/max_score*100, 1)}%)")
    print("=" * 80)
    
    # Status
    if total_score == max_score:
        print("\n🎉 EXCELLENT! All systems operational!")
        status = "COMPLETE"
    elif total_score >= max_score * 0.8:
        print("\n✅ GOOD! Minor items remaining")
        status = "MOSTLY_COMPLETE"
    else:
        print("\n⚠️  INCOMPLETE - Review missing items above")
        status = "INCOMPLETE"
    
    # Save report
    report = {
        "timestamp": datetime.now().isoformat(),
        "status": status,
        "score": {
            "total": total_score,
            "max": max_score,
            "percentage": round(total_score/max_score*100, 1)
        },
        "checks": all_checks
    }
    
    report_path = "scripts/purview_verification_final.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"\n📄 Full report: {report_path}")
    
    return report

# =============================================================================
# MAIN
# =============================================================================

def main():
    print_section("PURVIEW COMPLETE VERIFICATION")
    print(f"🕒 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    all_checks = {}
    
    # Run all checks
    all_checks['glossary'] = check_glossary_structure()
    all_checks['products'] = check_data_products()
    all_checks['classifications'] = check_classifications()
    all_checks['fabric'] = check_fabric_onelake()
    
    # Generate final report
    report = generate_final_report(all_checks)
    
    print(f"\n🕒 Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    return 0

if __name__ == "__main__":
    exit(main())
