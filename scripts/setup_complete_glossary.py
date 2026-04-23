"""
COMPLETE PURVIEW GLOSSARY STRUCTURE
====================================
Med Root Collection Admin-rättigheter kan vi nu:
1. Skapa Governance Domain-kategori
2. Skapa domain-terms under kategorin
3. Länka domain-terms till data products
4. Länka glossary terms till domains och products

Kör med: python scripts/setup_complete_glossary.py
"""

import requests
import json
from azure.identity import AzureCliCredential
from datetime import datetime
import time

# =============================================================================
# CONFIGURATION
# =============================================================================

PURVIEW_ACCOUNT = "https://prviewacc.purview.azure.com"
ATLAS_API = f"{PURVIEW_ACCOUNT}/catalog/api/atlas/v2"

# Governance Domains att skapa
GOVERNANCE_DOMAINS = [
    {
        "name": "Domain: Clinical Data Management",
        "abbr": "CDM",
        "description": "Governance domain för kliniska patientdata, EHR, besök och diagnoser (OMOP CDM)",
        "keywords": ["OMOP", "CDM", "Clinical", "EHR", "Patient", "Klinisk", "Patientanalys"]
    },
    {
        "name": "Domain: Genomics & Precision Medicine",
        "abbr": "GPM",
        "description": "Governance domain för genomisk data, sekvenseringsdata och precision medicine (BTB, VCF)",
        "keywords": ["Genomic", "BTB", "VCF", "DNA", "Sequencing", "BrainChild", "Barncancerforskning"]
    },
    {
        "name": "Domain: Cancer Registry",
        "abbr": "CR",
        "description": "Governance domain för cancer registry data (SBCR), behandlingar och uppföljning",
        "keywords": ["SBCR", "Cancer", "Registry", "Oncology", "Tumor", "Barncancer"]
    },
    {
        "name": "Domain: ML & Analytics",
        "abbr": "MLA",
        "description": "Governance domain för ML feature stores, analytics och datamodeller",
        "keywords": ["ML", "Feature", "Store", "Analytics", "Model", "Machine Learning"]
    }
]

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

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
# STEP 1: GET GLOSSARY
# =============================================================================

def get_glossary():
    """Get main glossary"""
    print_section("GET GLOSSARY")
    
    headers = get_headers()
    
    response = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=15)
    if response.status_code != 200:
        print(f"❌ Failed: {response.status_code}")
        return None
    
    data = response.json()
    glossaries = data if isinstance(data, list) else [data]
    
    if not glossaries:
        print("❌ No glossaries found")
        return None
    
    glossary = glossaries[0]
    print(f"✅ Glossary: {glossary.get('name')} (GUID: {glossary['guid']})")
    print(f"   Terms: {len(glossary.get('terms', []))}")
    print(f"   Categories: {len(glossary.get('categories', []))}")
    
    return glossary

# =============================================================================
# STEP 2: CREATE GOVERNANCE DOMAINS CATEGORY
# =============================================================================

def get_or_create_domains_category(glossary):
    """Get or create 'Governance Domains' category"""
    print_section("GOVERNANCE DOMAINS CATEGORY")
    
    headers = get_headers()
    glossary_guid = glossary['guid']
    
    # Check existing categories
    existing_cats = glossary.get('categories', [])
    
    for cat in existing_cats:
        cat_name = cat.get('displayText', '')
        if 'domain' in cat_name.lower() and 'governance' in cat_name.lower():
            print(f"✅ Found existing category: {cat_name}")
            print(f"   Category GUID: {cat.get('categoryGuid')}")
            return cat
    
    # Create new category
    print("📝 Creating new category: Governance Domains")
    
    category_body = {
        "anchor": {"glossaryGuid": glossary_guid},
        "name": "Governance Domains",
        "shortDescription": "Data governance domains for organizational structure",
        "longDescription": "High-level governance domains that organize data products and glossary terms"
    }
    
    try:
        response = requests.post(
            f"{ATLAS_API}/glossary/category",
            headers=headers,
            json=category_body,
            timeout=30
        )
        
        if response.status_code == 200:
            category = response.json()
            print(f"✅ Created: {category.get('displayText')}")
            print(f"   Category GUID: {category.get('guid')}")
            return category
        else:
            print(f"❌ Failed: {response.status_code}")
            print(f"   Response: {response.text[:300]}")
            return None
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

# =============================================================================
# STEP 3: CREATE DOMAIN TERMS
# =============================================================================

def create_domain_terms(glossary, category):
    """Create glossary terms for each governance domain"""
    print_section("CREATE DOMAIN TERMS")
    
    headers = get_headers()
    glossary_guid = glossary['guid']
    category_guid = category.get('guid') if category else None
    
    created_terms = []
    
    for domain in GOVERNANCE_DOMAINS:
        print(f"\n🔧 Creating: {domain['name']}")
        
        # Create term body
        term_body = {
            "name": domain['name'],
            "anchor": {"glossaryGuid": glossary_guid},
            "nickName": domain['abbr'],
            "shortDescription": domain['description'],
            "longDescription": domain['description'],
            "status": "Approved"
        }
        
        # Add category if exists
        if category_guid:
            term_body["categories"] = [{"categoryGuid": category_guid}]
        
        try:
            # Add includeTermHierarchy parameter as required by Purview
            response = requests.post(
                f"{ATLAS_API}/glossary/term?includeTermHierarchy=true",
                headers=headers,
                json=term_body,
                timeout=30
            )
            
            if response.status_code == 200:
                term = response.json()
                print(f"   ✅ Created: {term.get('displayText')}")
                print(f"      GUID: {term.get('guid')}")
                created_terms.append({**term, **domain})
            elif response.status_code == 409:
                print(f"   ℹ️  Already exists (409)")
                # Try to get existing term
                try:
                    qualified_name = f"{domain['name']}@Sjukvårdstermer"
                    get_resp = requests.get(
                        f"{ATLAS_API}/glossary/term/unique?qualifiedName={qualified_name}",
                        headers=headers,
                        timeout=15
                    )
                    if get_resp.status_code == 200:
                        term = get_resp.json()
                        print(f"   ✅ Retrieved existing term")
                        created_terms.append({**term, **domain})
                except:
                    pass
            else:
                print(f"   ❌ Failed: {response.status_code}")
                print(f"      Response: {response.text[:300]}")
        
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        time.sleep(0.5)
    
    print(f"\n📊 Domain terms ready: {len(created_terms)}")
    return created_terms

# =============================================================================
# STEP 4: GET DATA PRODUCTS
# =============================================================================

def get_data_products():
    """Get all data products"""
    print_section("GET DATA PRODUCTS")
    
    headers = get_headers()
    
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
        
        if response.status_code == 200:
            products = response.json().get('value', [])
            print(f"✅ Found {len(products)} data products:")
            for p in products:
                print(f"   • {p.get('name')}")
            return products
        else:
            print(f"❌ Failed: {response.status_code}")
            return []
    
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

# =============================================================================
# STEP 5: LINK DOMAINS TO PRODUCTS
# =============================================================================

def link_domains_to_products(domain_terms, products):
    """Link domain terms to data products via meanings relationship"""
    print_section("LINK DOMAINS TO PRODUCTS")
    
    headers = get_headers()
    linked_count = 0
    
    for product in products:
        product_name = product.get('name', '')
        product_guid = product.get('id', '')
        
        print(f"\n📦 {product_name}")
        
        # Find matching domain
        matched_domain = None
        for domain_term in domain_terms:
            keywords = domain_term.get('keywords', [])
            if any(kw.lower() in product_name.lower() for kw in keywords):
                matched_domain = domain_term
                break
        
        if not matched_domain:
            print(f"   ⚠️  No matching domain")
            continue
        
        domain_name = matched_domain.get('displayText', matched_domain.get('name'))
        print(f"   🔗 Linking to: {domain_name}")
        
        try:
            # Get product entity
            get_resp = requests.get(
                f"{ATLAS_API}/entity/guid/{product_guid}",
                headers=headers,
                timeout=15
            )
            
            if get_resp.status_code != 200:
                print(f"   ❌ Could not get entity: {get_resp.status_code}")
                continue
            
            entity_data = get_resp.json()
            entity = entity_data.get('entity', {})
            
            # Add domain term as meaning
            if 'relationshipAttributes' not in entity:
                entity['relationshipAttributes'] = {}
            
            if 'meanings' not in entity['relationshipAttributes']:
                entity['relationshipAttributes']['meanings'] = []
            
            # Create term reference
            term_ref = {
                "guid": matched_domain.get('guid'),
                "typeName": "AtlasGlossaryTerm"
            }
            
            # Check if already linked
            existing_guids = [m.get('guid') for m in entity['relationshipAttributes'].get('meanings', [])]
            if matched_domain.get('guid') in existing_guids:
                print(f"   ℹ️  Already linked")
                linked_count += 1
                continue
            
            # Add new meaning
            entity['relationshipAttributes']['meanings'].append(term_ref)
            
            # Also update attributes
            if 'attributes' not in entity:
                entity['attributes'] = {}
            entity['attributes']['governanceDomain'] = domain_name
            
            # Update via bulk API
            bulk_body = {"entities": [entity]}
            update_resp = requests.post(
                f"{ATLAS_API}/entity/bulk",
                headers=headers,
                json=bulk_body,
                timeout=30
            )
            
            if update_resp.status_code == 200:
                print(f"   ✅ Linked successfully")
                linked_count += 1
            else:
                print(f"   ❌ Update failed: {update_resp.status_code}")
                print(f"      Response: {update_resp.text[:200]}")
        
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        time.sleep(0.5)
    
    print(f"\n📊 Successfully linked: {linked_count}/{len(products)}")
    return linked_count

# =============================================================================
# STEP 6: GENERATE REPORT
# =============================================================================

def generate_report(glossary, category, domain_terms, products, linked_count):
    """Generate completion report"""
    print_section("GENERATE REPORT")
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "status": "COMPLETE",
        "glossary": {
            "name": glossary.get('name'),
            "guid": glossary.get('guid'),
            "total_terms": len(glossary.get('terms', [])),
            "total_categories": len(glossary.get('categories', []))
        },
        "governance_structure": {
            "category_created": category is not None,
            "category_name": category.get('displayText') if category else None,
            "domain_terms_created": len(domain_terms),
            "domains": [
                {
                    "name": d.get('displayText', d.get('name')),
                    "guid": d.get('guid'),
                    "abbreviation": d.get('abbr')
                }
                for d in domain_terms
            ]
        },
        "data_products": {
            "total": len(products),
            "linked_to_domains": linked_count,
            "products": [
                {
                    "name": p.get('name'),
                    "id": p.get('id')
                }
                for p in products
            ]
        },
        "summary": {
            "governance_domains": len(domain_terms),
            "data_products": len(products),
            "domain_product_links": linked_count,
            "coverage_percentage": round((linked_count / len(products) * 100) if products else 0, 1)
        }
    }
    
    # Save report
    report_path = "scripts/glossary_setup_complete.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Report saved: {report_path}")
    
    # Print summary
    print("\n" + "=" * 80)
    print("  SETUP COMPLETE - SUMMARY")
    print("=" * 80)
    print(f"\n✅ Glossary: {glossary.get('name')}")
    print(f"✅ Governance Domains Category: {'Created' if category else 'Failed'}")
    print(f"✅ Domain Terms: {len(domain_terms)} created")
    print(f"✅ Data Products: {len(products)} found")
    print(f"🔗 Links Created: {linked_count}/{len(products)} ({report['summary']['coverage_percentage']}%)")
    
    print("\n📋 Governance Domains:")
    for d in domain_terms:
        print(f"   • {d.get('displayText', d.get('name'))} ({d.get('abbr')})")
    
    return report

# =============================================================================
# MAIN
# =============================================================================

def main():
    print_section("COMPLETE PURVIEW GLOSSARY SETUP")
    print(f"🕒 Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("📝 With Root Collection Admin permissions\n")
    
    try:
        # Step 1: Get glossary
        glossary = get_glossary()
        if not glossary:
            return 1
        
        # Step 2: Get or create category
        category = get_or_create_domains_category(glossary)
        
        # Step 3: Create domain terms
        domain_terms = create_domain_terms(glossary, category)
        if not domain_terms:
            print("\n❌ No domain terms created")
            return 1
        
        # Step 4: Get data products
        products = get_data_products()
        
        # Step 5: Link domains to products
        linked_count = 0
        if products:
            linked_count = link_domains_to_products(domain_terms, products)
        
        # Step 6: Generate report
        report = generate_report(glossary, category, domain_terms, products, linked_count)
        
        print("\n" + "=" * 80)
        print("  ✅ ALL DONE")
        print("=" * 80)
        print(f"🕒 Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        return 0
    
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
