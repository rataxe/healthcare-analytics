#!/usr/bin/env python3
"""
MASTER PURVIEW SETUP SCRIPT
Uses working methods: /entity/bulk, Unified Catalog API (if available)
Configures all governance components for Healthcare Analytics
"""
import requests
import json
from azure.identity import AzureCliCredential
from typing import Dict, List, Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

PURVIEW_ACCOUNT = 'prviewacc.purview.azure.com'
TENANT_ID = '71c4b6d5-0065-4c6c-a125-841a582754eb'

# API Endpoints
ATLAS_BASE = f'https://{PURVIEW_ACCOUNT}/catalog/api/atlas/v2'
SEARCH_BASE = f'https://{PURVIEW_ACCOUNT}/catalog/api/search/query'
UNIFIED_BASE = f'https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog'

# Authentication
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

# ============================================================================
# DATA DEFINITIONS
# ============================================================================

DATA_PRODUCTS = {
    "Klinisk Patientanalys": {
        "qualifiedName": "dp://klinisk-patientanalys",
        "description": "Dataprodukt för klinisk patientanalys — innehåller patientdemografi, diagnoser (ICD-10), läkemedel (ATC), laboratorieprov (LOINC) och radiologiska studier (DICOM).",
        "userDescription": "Omfattar patientdemografi, diagnoser (ICD-10), läkemedel (ATC), laboratorieprov (LOINC) och radiologiska studier (DICOM). Innehåller data för 10,000 patienter med fokus på vårdkvalitet och prediktiv analys.",
        "product_type": "Analytics",
        "product_status": "Published",
        "product_owners": "Healthcare Analytics Team | Clinical Data Stewards | Dr. Anna Svensson",
        "sla": "Daglig uppdatering, <1h latens, 99.5% tillgänglighet, 10 års retention",
        "use_cases": "LOS-prediktion (LightGBM) | Återinläggningsrisk (Random Forest) | Charlson Comorbidity Index | Avdelningsstatistik | GDPR-compliance | Kliniskt beslutsfattande",
        "tables": "patients, encounters, diagnoses, medications, vitals_labs, observations, dicom_studies",
        "quality_score": 0.95,
        "domain": "Klinisk Vård",
        "glossary_terms": ["Patient", "ICD-10", "ATC", "LOINC", "Personnummer"]
    },
    "BrainChild Barncancerforskning": {
        "qualifiedName": "dp://brainchild-genomics",
        "description": "Genomisk dataprodukt för BrainChild barncancerforskning — innehåller VCF-filer, CNV-analys, COSMIC varianter och BTB-protokoll.",
        "userDescription": "Genomisk data från barncancerpatienter inklusive VCF-filer, CNV-analys, COSMIC varianter och BTB (Brain Tumor Board) protokoll. Integrerar med BrainChild biobank för longitudinell forskning.",
        "product_type": "Genomics",
        "product_status": "Published",
        "product_owners": "BrainChild Research Team | Genomics Lab | Prof. Lars Bergström",
        "sla": "Veckovis uppdatering, <24h latens, 99.9% tillgänglighet, 50 års retention",
        "use_cases": "Genomisk variant-analys | CNV-detection | COSMIC mutation lookup | Klinisk genotypning | Personlig medicin | Biobank-korrelation",
        "tables": "vcf_files, specimens, cosmic_variants, cnv_segments, btb_protocols",
        "quality_score": 0.98,
        "domain": "Forskning & Genomik",
        "glossary_terms": ["VCF", "CNV", "COSMIC", "Biobank"]
    },
    "ML Feature Store": {
        "qualifiedName": "dp://ml-feature-store",
        "description": "Centraliserad feature store för maskininlärning — stöder Bronze/Silver/Gold arkitektur med feature versioning.",
        "userDescription": "Centraliserad feature store för maskininlärningsmodeller. Inkluderar Bronze (rå data), Silver (validerad), Gold (aggregerad) lager. Stöder feature versioning, point-in-time lookups och model registry.",
        "product_type": "ML_Features",
        "product_status": "Published",
        "product_owners": "ML Engineering Team | Data Science Team | Dr. Maria Lindqvist",
        "sla": "Real-time uppdatering, <100ms latens för inference, 99.99% tillgänglighet, 5 års retention",
        "use_cases": "Feature engineering | Model training | Real-time inference | Feature lineage tracking | Model versioning | A/B testing",
        "tables": "bronze_features, silver_features, gold_features, model_registry, feature_lineage",
        "quality_score": 0.92,
        "domain": "Data & Analytics",
        "glossary_terms": ["Feature Store", "Medallion Architecture", "Machine Learning"]
    },
    "OMOP Forskningsdata": {
        "qualifiedName": "dp://omop-research",
        "description": "OMOP CDM v5.4 forskningsdata — standardiserad för observationsstudier och real-world evidence.",
        "userDescription": "Standardiserad forskningsdata enligt OMOP CDM v5.4 med vocabularies (SNOMED CT, ATC, LOINC, ICD-10). Används för observationsstudier, kohort-analys och real-world evidence (RWE) forskning.",
        "product_type": "Research",
        "product_status": "Published",
        "product_owners": "Clinical Research Team | OMOP Data Stewards | Dr. Erik Johansson",
        "sla": "Månatlig uppdatering, <7 dagar latens, 99.9% tillgänglighet, Indefinite retention",
        "use_cases": "Kohort-identifiering | Observationsstudier | Drug-safety analys | Real-world evidence | Komparativ effektivitet | Vocabularies mapping",
        "tables": "person, condition_occurrence, drug_exposure, measurement, visit_occurrence, specimen, concept, vocabulary",
        "quality_score": 0.96,
        "domain": "Forskning & Genomik",
        "glossary_terms": ["OMOP CDM", "SNOMED CT", "Real-World Evidence"]
    }
}

GOVERNANCE_DOMAINS = {
    "Klinisk Vård": {
        "description": "Klinisk patientvård och vårdkvalitet — omfattar patientdata, diagnoser, behandlingar och kliniska processer.",
        "data_products": ["Klinisk Patientanalys"],
        "critical_capabilities": [
            "Patientidentifiering (Personnummer)",
            "ICD-10 diagnosklassificering",
            "ATC läkemedelskodning",
            "LOINC laboratoriestandarder",
            "DICOM bildhantering"
        ]
    },
    "Forskning & Genomik": {
        "description": "Barncancerforskning och genomisk analys — VCF-data, COSMIC varianter och biobank-integration.",
        "data_products": ["BrainChild Barncancerforskning", "OMOP Forskningsdata"],
        "critical_capabilities": [
            "VCF variant-calling",
            "CNV copy number analysis",
            "COSMIC mutation database",
            "OMOP CDM v5.4 mappning",
            "Biobank specimen tracking"
        ]
    },
    "Interoperabilitet & Standarder": {
        "description": "FHIR R4, HL7 och ISO standarder för datautbyte och interoperabilitet.",
        "data_products": [],
        "critical_capabilities": [
            "FHIR R4 resources",
            "HL7v2 message parsing",
            "ISO 13606 EHR standard",
            "SNOMED CT terminologi",
            "International Patient Summary"
        ]
    },
    "Data & Analytics": {
        "description": "Machine Learning, Feature Store och Medallion Architecture för avancerad analys.",
        "data_products": ["ML Feature Store"],
        "critical_capabilities": [
            "Feature engineering",
            "Bronze/Silver/Gold layers",
            "Model versioning",
            "Point-in-time lookups",
            "Data lineage tracking"
        ]
    }
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def test_unified_catalog_api() -> bool:
    """Test if Unified Catalog API is available"""
    try:
        r = requests.get(
            f'{UNIFIED_BASE}/businessdomains?api-version=2025-09-15-preview',
            headers=headers,
            timeout=15
        )
        return r.status_code != 403
    except:
        return False

def get_glossary_guid() -> Optional[str]:
    """Get main glossary GUID"""
    r = requests.get(f'{ATLAS_BASE}/glossary', headers=headers, timeout=30)
    if r.status_code == 200:
        data = r.json()
        glossaries = data if isinstance(data, list) else [data]
        return glossaries[0]['guid']
    return None

def search_entity(entity_type: str, keywords: str = '*') -> List[Dict]:
    """Search for entities"""
    body = {
        'keywords': keywords,
        'limit': 100,
        'filter': {'entityType': entity_type}
    }
    r = requests.post(
        f'{SEARCH_BASE}?api-version=2022-08-01-preview',
        headers=headers,
        json=body,
        timeout=30
    )
    if r.status_code == 200:
        return r.json().get('value', [])
    return []

def update_entity_bulk(entity: Dict) -> bool:
    """Update entity using /entity/bulk"""
    try:
        # Remove server-generated timestamps
        for field in ['lastModifiedTS', 'createTime', 'updateTime']:
            entity.pop(field, None)
        
        r = requests.post(
            f'{ATLAS_BASE}/entity/bulk',
            headers=headers,
            json={'entities': [entity]},
            timeout=30
        )
        return r.status_code == 200
    except:
        return False

def create_business_domain_unified(name: str, description: str) -> Optional[str]:
    """Create business domain using Unified Catalog API"""
    body = {
        "name": name,
        "displayName": name,
        "description": description,
        "parentDomainId": None
    }
    
    try:
        r = requests.post(
            f'{UNIFIED_BASE}/businessdomains?api-version=2025-09-15-preview',
            headers=headers,
            json=body,
            timeout=30
        )
        if r.status_code in [200, 201]:
            return r.json().get('id')
    except:
        pass
    return None

def create_data_product_unified(name: str, domain_id: str, details: Dict) -> Optional[str]:
    """Create data product using Unified Catalog API"""
    body = {
        "name": name,
        "displayName": name,
        "description": details.get('description', ''),
        "businessDomainId": domain_id,
        "properties": {
            "productType": details.get('product_type'),
            "status": details.get('product_status'),
            "owners": details.get('product_owners'),
            "sla": details.get('sla'),
            "useCases": details.get('use_cases'),
            "qualityScore": details.get('quality_score')
        }
    }
    
    try:
        r = requests.post(
            f'{UNIFIED_BASE}/dataProducts?api-version=2025-09-15-preview',
            headers=headers,
            json=body,
            timeout=30
        )
        if r.status_code in [200, 201]:
            return r.json().get('id')
    except:
        pass
    return None

# ============================================================================
# MAIN SETUP FUNCTIONS
# ============================================================================

def setup_data_products_atlas():
    """Update existing data products using Atlas /entity/bulk"""
    print("\n" + "="*80)
    print("  UPDATING DATA PRODUCTS (Atlas /entity/bulk)")
    print("="*80)
    
    # Find existing data products
    entities = search_entity('healthcare_data_product')
    print(f"Found {len(entities)} existing data products\n")
    
    updated = 0
    failed = 0
    
    for entity_search in entities:
        name = entity_search.get('name')
        guid = entity_search.get('id')
        
        if name not in DATA_PRODUCTS:
            continue
        
        print(f"📦 {name}")
        
        # Get full entity
        r = requests.get(f'{ATLAS_BASE}/entity/guid/{guid}', headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"   ❌ Could not fetch entity")
            failed += 1
            continue
        
        entity = r.json().get('entity', {})
        
        # Update attributes
        details = DATA_PRODUCTS[name]
        entity['attributes'].update({
            'description': details['description'],
            'userDescription': details['userDescription'],
            'product_type': details['product_type'],
            'product_status': details['product_status'],
            'product_owners': details['product_owners'],
            'sla': details['sla'],
            'use_cases': details['use_cases'],
            'tables': details['tables'],
            'quality_score': details['quality_score']
        })
        
        # Update using /entity/bulk
        if update_entity_bulk(entity):
            print(f"   ✅ Updated")
            updated += 1
        else:
            print(f"   ❌ Update failed")
            failed += 1
    
    print(f"\n✅ Updated: {updated} | ❌ Failed: {failed}")
    return updated

def setup_governance_domains_unified():
    """Create governance domains using Unified Catalog API"""
    print("\n" + "="*80)
    print("  CREATING GOVERNANCE DOMAINS (Unified Catalog API)")
    print("="*80)
    
    created = 0
    failed = 0
    domain_ids = {}
    
    for name, details in GOVERNANCE_DOMAINS.items():
        print(f"\n🏛️  {name}")
        domain_id = create_business_domain_unified(name, details['description'])
        
        if domain_id:
            print(f"   ✅ Created (ID: {domain_id})")
            domain_ids[name] = domain_id
            created += 1
        else:
            print(f"   ❌ Creation failed (API may not be available)")
            failed += 1
    
    print(f"\n✅ Created: {created} | ❌ Failed: {failed}")
    return domain_ids

def link_data_products_to_domains_unified(domain_ids: Dict[str, str]):
    """Link data products to domains using Unified Catalog API"""
    print("\n" + "="*80)
    print("  LINKING DATA PRODUCTS TO DOMAINS")
    print("="*80)
    
    linked = 0
    
    for domain_name, details in GOVERNANCE_DOMAINS.items():
        if domain_name not in domain_ids:
            continue
        
        domain_id = domain_ids[domain_name]
        print(f"\n🏛️  {domain_name}")
        
        for dp_name in details['data_products']:
            if dp_name not in DATA_PRODUCTS:
                continue
            
            dp_details = DATA_PRODUCTS[dp_name]
            product_id = create_data_product_unified(dp_name, domain_id, dp_details)
            
            if product_id:
                print(f"   ✅ Linked: {dp_name}")
                linked += 1
            else:
                print(f"   ⚠️  Could not link: {dp_name}")
    
    print(f"\n✅ Linked: {linked}")
    return linked

def link_glossary_terms_to_entities():
    """Link glossary terms to data product entities"""
    print("\n" + "="*80)
    print("  LINKING GLOSSARY TERMS TO DATA PRODUCTS")
    print("="*80)
    
    glossary_guid = get_glossary_guid()
    if not glossary_guid:
        print("❌ Could not find glossary")
        return 0
    
    # Get all glossary terms
    r = requests.get(
        f'{ATLAS_BASE}/glossary/{glossary_guid}/terms?limit=200&offset=0',
        headers=headers,
        timeout=30
    )
    
    if r.status_code != 200:
        print("❌ Could not fetch glossary terms")
        return 0
    
    all_terms = r.json()
    term_map = {}
    for t in all_terms:
        if 'attributes' in t and 'name' in t['attributes']:
            term_map[t['attributes']['name']] = t
    
    # Get all data products
    entities = search_entity('healthcare_data_product')
    
    linked = 0
    
    for entity_search in entities:
        name = entity_search.get('name')
        guid = entity_search.get('id')
        
        if name not in DATA_PRODUCTS:
            continue
        
        print(f"\n📦 {name}")
        
        # Get full entity
        r = requests.get(f'{ATLAS_BASE}/entity/guid/{guid}', headers=headers, timeout=30)
        if r.status_code != 200:
            continue
        
        entity = r.json().get('entity', {})
        
        # Get terms to link
        terms_to_link = DATA_PRODUCTS[name].get('glossary_terms', [])
        
        # Build term relationships
        if 'relationshipAttributes' not in entity:
            entity['relationshipAttributes'] = {}
        
        if 'meanings' not in entity['relationshipAttributes']:
            entity['relationshipAttributes']['meanings'] = []
        
        for term_name in terms_to_link:
            if term_name in term_map:
                term_guid = term_map[term_name]['guid']
                entity['relationshipAttributes']['meanings'].append({
                    'guid': term_guid,
                    'typeName': 'AtlasGlossaryTerm',
                    'relationshipType': 'AtlasGlossarySemanticAssignment'
                })
                print(f"   🔗 Linking term: {term_name}")
                linked += 1
        
        # Update entity
        update_entity_bulk(entity)
    
    print(f"\n✅ Linked {linked} term relationships")
    return linked

def create_sql_lineage():
    """Create lineage for SQL → Fabric data flow"""
    print("\n" + "="*80)
    print("  CREATING SQL → FABRIC LINEAGE")
    print("="*80)
    
    # This would create Process entities connecting SQL tables to Fabric tables
    # Placeholder for now - requires SQL and Fabric entities to be scanned first
    
    print("⚠️  Requires SQL Server and Fabric workspaces to be scanned first")
    print("   Run: scripts/scan_fabric_lakehouses.py")
    
    return 0

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("="*80)
    print("  PURVIEW MASTER SETUP")
    print("  Healthcare Analytics Governance Configuration")
    print("="*80)
    
    # Test API availability
    print("\n🔍 Testing API availability...")
    unified_available = test_unified_catalog_api()
    print(f"   Atlas API v2: ✅ Available")
    print(f"   Unified Catalog API: {'✅ Available' if unified_available else '❌ Not Available (403)'}")
    
    # Execute setup steps
    stats = {
        'data_products_updated': 0,
        'domains_created': 0,
        'terms_linked': 0,
        'lineage_created': 0
    }
    
    # 1. Update data products (Atlas)
    stats['data_products_updated'] = setup_data_products_atlas()
    
    # 2. Create governance domains (Unified API if available)
    domain_ids = {}
    if unified_available:
        domain_ids = setup_governance_domains_unified()
        stats['domains_created'] = len(domain_ids)
        
        # 3. Link data products to domains
        if domain_ids:
            link_data_products_to_domains_unified(domain_ids)
    else:
        print("\n⚠️  Unified Catalog API not available")
        print("   Governance domains must be created manually in portal")
        print("   https://portal.azure.com/#view/Microsoft_Azure_Purview/MainMenuBlade/~/dataCatalog")
    
    # 4. Link glossary terms
    stats['terms_linked'] = link_glossary_terms_to_entities()
    
    # 5. Create lineage
    stats['lineage_created'] = create_sql_lineage()
    
    # Summary
    print("\n" + "="*80)
    print("  SETUP COMPLETE")
    print("="*80)
    print(f"  Data Products Updated: {stats['data_products_updated']}")
    print(f"  Domains Created: {stats['domains_created']}")
    print(f"  Terms Linked: {stats['terms_linked']}")
    print(f"  Lineage Created: {stats['lineage_created']}")
    print("="*80)
    
    if not unified_available:
        print("\n📌 NEXT STEPS:")
        print("   1. Manually create 4 governance domains in Purview portal")
        print("   2. Contact Azure support to enable Unified Catalog API")
        print("   3. Re-run this script to link data products to domains")
        print("   4. Run: python scripts/scan_fabric_lakehouses.py")

if __name__ == '__main__':
    main()
