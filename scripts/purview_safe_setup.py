#!/usr/bin/env python3
"""
SAFE PURVIEW SETUP - NO DOMAIN REFERENCES
Works with current API limitations:
- Uses Atlas API v2 for entities (no domain links)
- Links glossary terms only
- Avoids DomainReference errors
"""
import requests
import json
from azure.identity import AzureCliCredential

# Configuration
PURVIEW_ACCOUNT = 'prviewacc.purview.azure.com'
ATLAS_BASE = f'https://{PURVIEW_ACCOUNT}/catalog/api/atlas/v2'
SEARCH_BASE = f'https://{PURVIEW_ACCOUNT}/catalog/api/search/query'

# Authentication
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

# Data Product Details (NO domain field)
DATA_PRODUCTS = {
    "Klinisk Patientanalys": {
        "userDescription": "Omfattar patientdemografi, diagnoser (ICD-10), läkemedel (ATC), laboratorieprov (LOINC) och radiologiska studier (DICOM). Innehåller data för 10,000 patienter med fokus på vårdkvalitet och prediktiv analys.",
        "product_type": "Analytics",
        "product_status": "Published",
        "product_owners": "Healthcare Analytics Team | Clinical Data Stewards | Dr. Anna Svensson",
        "sla": "Daglig uppdatering, <1h latens, 99.5% tillgänglighet, 10 års retention",
        "use_cases": "LOS-prediktion (LightGBM) | Återinläggningsrisk (Random Forest) | Charlson Comorbidity Index | Avdelningsstatistik | GDPR-compliance | Kliniskt beslutsfattande",
        "tables": "patients, encounters, diagnoses, medications, vitals_labs, observations, dicom_studies",
        "quality_score": 0.95,
        "glossary_terms": ["Patient", "ICD-10", "ATC", "LOINC", "Personnummer"]
    },
    "BrainChild Barncancerforskning": {
        "userDescription": "Genomisk data från barncancerpatienter inklusive VCF-filer, CNV-analys, COSMIC varianter och BTB (Brain Tumor Board) protokoll. Integrerar med BrainChild biobank för longitudinell forskning.",
        "product_type": "Genomics",
        "product_status": "Published",
        "product_owners": "BrainChild Research Team | Genomics Lab | Prof. Lars Bergström",
        "sla": "Veckovis uppdatering, <24h latens, 99.9% tillgänglighet, 50 års retention",
        "use_cases": "Genomisk variant-analys | CNV-detection | COSMIC mutation lookup | Klinisk genotypning | Personlig medicin | Biobank-korrelation",
        "tables": "vcf_files, specimens, cosmic_variants, cnv_segments, btb_protocols",
        "quality_score": 0.98,
        "glossary_terms": ["VCF", "CNV", "COSMIC", "Biobank"]
    },
    "ML Feature Store": {
        "userDescription": "Centraliserad feature store för maskininlärningsmodeller. Inkluderar Bronze (rå data), Silver (validerad), Gold (aggregerad) lager. Stöder feature versioning, point-in-time lookups och model registry.",
        "product_type": "ML_Features",
        "product_status": "Published",
        "product_owners": "ML Engineering Team | Data Science Team | Dr. Maria Lindqvist",
        "sla": "Real-time uppdatering, <100ms latens för inference, 99.99% tillgänglighet, 5 års retention",
        "use_cases": "Feature engineering | Model training | Real-time inference | Feature lineage tracking | Model versioning | A/B testing",
        "tables": "bronze_features, silver_features, gold_features, model_registry, feature_lineage",
        "quality_score": 0.92,
        "glossary_terms": ["Feature Store", "Medallion Architecture", "Machine Learning"]
    },
    "OMOP Forskningsdata": {
        "userDescription": "Standardiserad forskningsdata enligt OMOP CDM v5.4 med vocabularies (SNOMED CT, ATC, LOINC, ICD-10). Används för observationsstudier, kohort-analys och real-world evidence (RWE) forskning.",
        "product_type": "Research",
        "product_status": "Published",
        "product_owners": "Clinical Research Team | OMOP Data Stewards | Dr. Erik Johansson",
        "sla": "Månatlig uppdatering, <7 dagar latens, 99.9% tillgänglighet, Indefinite retention",
        "use_cases": "Kohort-identifiering | Observationsstudier | Drug-safety analys | Real-world evidence | Komparativ effektivitet | Vocabularies mapping",
        "tables": "person, condition_occurrence, drug_exposure, measurement, visit_occurrence, specimen, concept, vocabulary",
        "quality_score": 0.96,
        "glossary_terms": ["OMOP CDM", "SNOMED CT", "Real-World Evidence"]
    }
}

def search_entity(entity_type: str):
    """Search for entities by type"""
    body = {
        'keywords': '*',
        'limit': 50,
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

def get_glossary_guid():
    """Get glossary GUID"""
    r = requests.get(f'{ATLAS_BASE}/glossary', headers=headers, timeout=30)
    if r.status_code == 200:
        data = r.json()
        glossaries = data if isinstance(data, list) else [data]
        return glossaries[0]['guid'] if glossaries else None
    return None

def update_entity_safe(entity: dict) -> bool:
    """Update entity using /entity/bulk, removing ALL domain references"""
    # Remove server timestamps
    for field in ['lastModifiedTS', 'createTime', 'updateTime']:
        entity.pop(field, None)
    
    # CRITICAL: Remove domain references to avoid DomainReference error
    entity.pop('domainId', None)
    if 'attributes' in entity:
        entity['attributes'].pop('domain', None)
        entity['attributes'].pop('domainId', None)
    
    # Remove domain from relationshipAttributes if present
    if 'relationshipAttributes' in entity:
        entity['relationshipAttributes'].pop('domain', None)
        entity['relationshipAttributes'].pop('domains', None)
    
    try:
        r = requests.post(
            f'{ATLAS_BASE}/entity/bulk',
            headers=headers,
            json={'entities': [entity]},
            timeout=30
        )
        return r.status_code == 200
    except Exception as e:
        print(f"      Error: {e}")
        return False

def update_data_products():
    """Update all data products with detailed metadata"""
    print("\n" + "="*80)
    print("  UPDATING DATA PRODUCTS (Safe Mode - No Domain References)")
    print("="*80)
    
    entities = search_entity('healthcare_data_product')
    print(f"Found {len(entities)} data products\n")
    
    updated = 0
    failed = 0
    
    for entity_search in entities:
        name = entity_search.get('name')
        guid = entity_search.get('id')
        
        if name not in DATA_PRODUCTS:
            print(f"⚠️  {name} - not in update list")
            continue
        
        print(f"📦 {name}")
        
        # Get full entity
        r = requests.get(f'{ATLAS_BASE}/entity/guid/{guid}', headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"   ❌ Could not fetch entity")
            failed += 1
            continue
        
        entity = r.json().get('entity', {})
        
        # Update attributes (excluding domain field)
        details = DATA_PRODUCTS[name]
        entity['attributes'].update({
            'userDescription': details['userDescription'],
            'product_type': details['product_type'],
            'product_status': details['product_status'],
            'product_owners': details['product_owners'],
            'sla': details['sla'],
            'use_cases': details['use_cases'],
            'tables': details['tables'],
            'quality_score': details['quality_score']
        })
        
        # Update using safe method
        if update_entity_safe(entity):
            print(f"   ✅ Updated")
            updated += 1
        else:
            print(f"   ❌ Update failed")
            failed += 1
    
    print(f"\n✅ Updated: {updated} | ❌ Failed: {failed}")
    return updated

def link_glossary_terms():
    """Link glossary terms to data products"""
    print("\n" + "="*80)
    print("  LINKING GLOSSARY TERMS TO DATA PRODUCTS")
    print("="*80)
    
    glossary_guid = get_glossary_guid()
    if not glossary_guid:
        print("❌ Could not find glossary")
        return 0
    
    # Get all glossary terms using search (more reliable)
    body = {
        'keywords': '*',
        'limit': 200,
        'filter': {'assetType': ['Glossary Term']}
    }
    r = requests.post(
        f'{SEARCH_BASE}?api-version=2022-08-01-preview',
        headers=headers,
        json=body,
        timeout=30
    )
    
    if r.status_code != 200:
        print("❌ Could not fetch glossary terms")
        return 0
    
    all_terms = r.json().get('value', [])
    term_map = {}
    for t in all_terms:
        term_name = t.get('name')
        term_id = t.get('id')
        if term_name and term_id:
            term_map[term_name] = {'guid': term_id}
    
    print(f"Found {len(term_map)} glossary terms\n")
    
    # Get all data products
    entities = search_entity('healthcare_data_product')
    
    linked = 0
    
    for entity_search in entities:
        name = entity_search.get('name')
        guid = entity_search.get('id')
        
        if name not in DATA_PRODUCTS:
            continue
        
        print(f"📦 {name}")
        
        # Get full entity
        r = requests.get(f'{ATLAS_BASE}/entity/guid/{guid}', headers=headers, timeout=30)
        if r.status_code != 200:
            continue
        
        entity = r.json().get('entity', {})
        
        # Get term GUIDs for this product
        term_names = DATA_PRODUCTS[name].get('glossary_terms', [])
        term_guids = []
        
        for term_name in term_names:
            if term_name in term_map:
                term_guids.append(term_map[term_name])  # Already has {'guid': ...} format
        
        if not term_guids:
            print(f"   ⚠️  No terms to link")
            continue
        
        # Update meanings (glossary term relationships)
        if 'relationshipAttributes' not in entity:
            entity['relationshipAttributes'] = {}
        
        entity['relationshipAttributes']['meanings'] = term_guids
        
        # Update using safe method
        if update_entity_safe(entity):
            print(f"   ✅ Linked {len(term_guids)} terms")
            linked += len(term_guids)
        else:
            print(f"   ❌ Failed to link terms")
    
    print(f"\n✅ Total terms linked: {linked}")
    return linked

def main():
    print("="*80)
    print("  SAFE PURVIEW SETUP")
    print("  (No Domain References - Avoids DomainReference Error)")
    print("="*80)
    
    # Update data products
    updated = update_data_products()
    
    # Link glossary terms
    linked = link_glossary_terms()
    
    # Summary
    print("\n" + "="*80)
    print("  SETUP COMPLETE")
    print("="*80)
    print(f"""
✅ Data products updated: {updated}
✅ Glossary terms linked: {linked}

⚠️  GOVERNANCE DOMAINS:
Domain links are not supported via Atlas API v2.
Domains must be managed manually in the portal or via Unified Catalog API.

Current workaround:
- Domains exist in portal (manually created)
- Data products updated with metadata
- Glossary terms linked to products
- No domain references in entity structure

To enable domain links:
1. Request Unified Catalog API activation from Azure Support
2. Or manage domain associations manually in portal
""")

if __name__ == '__main__':
    main()
