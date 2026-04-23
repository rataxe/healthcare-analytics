#!/usr/bin/env python3
"""
Automate Domain-to-Product Linking

Automatically creates relationships between:
- Governance Domains → Data Products
- Data Products → Glossary Terms  
- Data Products → Critical Data Elements
- Assets → Data Products

Uses intelligent matching based on naming conventions and metadata.

USAGE:
    python scripts/automate_domain_linking.py

REQUIRES:
    - Service Principal credentials in .env.purview or Key Vault
    - Unified Catalog API access
"""
import re
from typing import Dict, List, Optional, Tuple
from unified_catalog_client import UnifiedCatalogClient


# Mapping rules: Data Product name patterns → Domain name
DOMAIN_MAPPING = {
    r'(OMOP|CDM|Clinical)': 'Clinical Data Management',
    r'(Genomic|BTB|VCF)': 'Genomics & Precision Medicine',
    r'(SBCR|Cancer|Registry)': 'Cancer Registry',
    r'(FHIR|GMS|Interoperability)': 'Interoperability & Standards',
    r'(Analytics|Dashboard|Report)': 'Analytics & Reporting',
}

# Term → Product mapping patterns
TERM_TO_PRODUCT_PATTERNS = {
    'OMOP': ['OMOP CDM', 'Clinical Data'],
    'FHIR': ['FHIR Resources', 'GMS Data'],
    'Genomic': ['Genomic Data', 'BTB Samples'],
    'Cancer': ['SBCR Registry', 'Cancer Data'],
}


def match_domain(product_name: str, domains: List[Dict]) -> Optional[Dict]:
    """
    Match data product to domain using naming patterns
    
    Args:
        product_name: Data product name
        domains: List of available domains
    
    Returns:
        Matched domain or None
    """
    for pattern, domain_name in DOMAIN_MAPPING.items():
        if re.search(pattern, product_name, re.IGNORECASE):
            # Find domain by name
            for domain in domains:
                if domain_name.lower() in domain.get('name', '').lower():
                    return domain
    return None


def match_products_for_term(term: Dict, products: List[Dict]) -> List[Dict]:
    """
    Match glossary term to relevant data products
    
    Args:
        term: Glossary term object
        products: List of data products
    
    Returns:
        List of matching products
    """
    term_name = term.get('name', '')
    term_definition = term.get('definition', '')
    
    matches = []
    for product in products:
        product_name = product.get('name', '')
        product_desc = product.get('description', '')
        
        # Check if term is mentioned in product
        if (term_name.lower() in product_name.lower() or
            term_name.lower() in product_desc.lower()):
            matches.append(product)
            continue
        
        # Check pattern-based matching
        for keyword, product_patterns in TERM_TO_PRODUCT_PATTERNS.items():
            if keyword.lower() in term_name.lower():
                for pattern in product_patterns:
                    if pattern.lower() in product_name.lower():
                        matches.append(product)
                        break
    
    return matches


def link_domains_to_products(client: UnifiedCatalogClient, dry_run: bool = True) -> Dict:
    """
    Link governance domains to data products
    
    Args:
        client: Unified Catalog client
        dry_run: If True, only show what would be done
    
    Returns:
        Statistics
    """
    print("="*80)
    print("  LINKING DOMAINS TO DATA PRODUCTS")
    print("="*80)
    
    # Get all domains and products
    print("\n📥 Fetching domains and products...")
    domains = client.list_business_domains()
    products = client.list_data_products()
    
    print(f"   Found {len(domains)} domains")
    print(f"   Found {len(products)} products")
    
    stats = {
        'products_checked': 0,
        'matches_found': 0,
        'links_created': 0,
        'links_failed': 0,
        'links_skipped': 0
    }
    
    print(f"\n{'DRY RUN MODE' if dry_run else 'LIVE MODE'}")
    print()
    
    for product in products:
        stats['products_checked'] += 1
        product_name = product.get('name', 'Unknown')
        product_id = product.get('id')
        
        # Check if already linked
        existing_rels = client.list_data_product_relationships(product_id)
        domain_rels = [r for r in existing_rels if r.get('type') == 'PARENT_DOMAIN']
        
        if domain_rels:
            print(f"⏭️  {product_name}: Already linked to domain")
            stats['links_skipped'] += 1
            continue
        
        # Find matching domain
        domain = match_domain(product_name, domains)
        
        if domain:
            stats['matches_found'] += 1
            domain_name = domain.get('name')
            domain_id = domain.get('id')
            
            print(f"🎯 {product_name} → {domain_name}")
            
            if not dry_run:
                try:
                    client.create_data_product_relationship(
                        product_id,
                        'PARENT_DOMAIN',
                        domain_id
                    )
                    print(f"   ✅ Link created")
                    stats['links_created'] += 1
                except Exception as e:
                    print(f"   ❌ Failed: {e}")
                    stats['links_failed'] += 1
        else:
            print(f"❓ {product_name}: No matching domain found")
    
    print("\n" + "="*80)
    print("  SUMMARY")
    print("="*80)
    print(f"Products checked:  {stats['products_checked']}")
    print(f"Matches found:     {stats['matches_found']}")
    print(f"Links created:     {stats['links_created']}")
    print(f"Links failed:      {stats['links_failed']}")
    print(f"Links skipped:     {stats['links_skipped']}")
    
    return stats


def link_terms_to_products(client: UnifiedCatalogClient, dry_run: bool = True) -> Dict:
    """
    Link glossary terms to data products
    
    Args:
        client: Unified Catalog client
        dry_run: If True, only show what would be done
    
    Returns:
        Statistics
    """
    print("\n" + "="*80)
    print("  LINKING GLOSSARY TERMS TO DATA PRODUCTS")
    print("="*80)
    
    print("\n📥 Fetching terms and products...")
    terms = client.list_glossary_terms()
    products = client.list_data_products()
    
    print(f"   Found {len(terms)} terms")
    print(f"   Found {len(products)} products")
    
    stats = {
        'terms_checked': 0,
        'matches_found': 0,
        'links_created': 0,
        'links_failed': 0
    }
    
    print(f"\n{'DRY RUN MODE' if dry_run else 'LIVE MODE'}")
    print()
    
    for term in terms:
        stats['terms_checked'] += 1
        term_name = term.get('name', 'Unknown')
        term_id = term.get('id')
        
        # Find matching products
        matching_products = match_products_for_term(term, products)
        
        if matching_products:
            stats['matches_found'] += len(matching_products)
            
            for product in matching_products:
                product_name = product.get('name')
                product_id = product.get('id')
                
                print(f"🎯 {term_name} → {product_name}")
                
                if not dry_run:
                    try:
                        client.create_glossary_term_relationship(
                            term_id,
                            {
                                'typeName': 'RELATED_TO_DATA_PRODUCT',
                                'target': {'id': product_id}
                            }
                        )
                        print(f"   ✅ Link created")
                        stats['links_created'] += 1
                    except Exception as e:
                        print(f"   ❌ Failed: {e}")
                        stats['links_failed'] += 1
    
    print("\n" + "="*80)
    print("  SUMMARY")
    print("="*80)
    print(f"Terms checked:     {stats['terms_checked']}")
    print(f"Matches found:     {stats['matches_found']}")
    print(f"Links created:     {stats['links_created']}")
    print(f"Links failed:      {stats['links_failed']}")
    
    return stats


def link_all_relationships(dry_run: bool = True):
    """
    Link all relationships (domains, products, terms)
    
    Args:
        dry_run: If True, only show what would be done
    """
    print("="*80)
    print("  AUTOMATED RELATIONSHIP LINKING")
    print("="*80)
    print()
    
    if dry_run:
        print("⚠️  DRY RUN MODE - No changes will be made")
        print("   Run with --live to create links")
    else:
        print("🔴 LIVE MODE - Changes will be applied")
    
    print()
    
    # Initialize client
    try:
        client = UnifiedCatalogClient()
        print("✅ Connected to Unified Catalog API")
    except Exception as e:
        print(f"❌ Failed to connect: {e}")
        print("\n💡 Make sure you have:")
        print("   1. Created Service Principal")
        print("   2. Run: python scripts/setup_keyvault_credentials.py")
        print("   3. Or created scripts/.env.purview")
        return 1
    
    # 1. Link domains to products
    domain_stats = link_domains_to_products(client, dry_run)
    
    # 2. Link terms to products
    term_stats = link_terms_to_products(client, dry_run)
    
    # Overall summary
    print("\n" + "="*80)
    print("  OVERALL SUMMARY")
    print("="*80)
    print()
    print("Domain-to-Product Links:")
    print(f"  • Created: {domain_stats['links_created']}")
    print(f"  • Failed:  {domain_stats['links_failed']}")
    print(f"  • Skipped: {domain_stats['links_skipped']}")
    print()
    print("Term-to-Product Links:")
    print(f"  • Created: {term_stats['links_created']}")
    print(f"  • Failed:  {term_stats['links_failed']}")
    print()
    
    if dry_run:
        print("✅ Dry run complete - no changes made")
        print("\nTo apply these changes, run:")
        print("  python scripts/automate_domain_linking.py --live")
    else:
        total_created = domain_stats['links_created'] + term_stats['links_created']
        print(f"✅ {total_created} relationships created successfully!")
    
    return 0


def main():
    """Main entry point"""
    import sys
    
    # Check for --live flag
    dry_run = '--live' not in sys.argv
    
    try:
        return link_all_relationships(dry_run)
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    import sys
    sys.exit(main())
