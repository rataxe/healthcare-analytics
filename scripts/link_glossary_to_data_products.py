#!/usr/bin/env python3
"""
Link Glossary Terms to Data Products in Purview
Automatically assigns relevant glossary terms to each of the 4 data products
"""

import requests
import json
from azure.identity import AzureCliCredential

# Configuration
PURVIEW_ACCOUNT = "prviewacc"
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
ATLAS_API = f"{PURVIEW_ENDPOINT}/catalog/api/atlas/v2"

# Data Product → Glossary Terms mapping
DATA_PRODUCT_TERMS = {
    "Klinisk Patientanalys": [
        "Patient",
        "Swedish Personnummer",
        "Encounter",
        "Condition",
        "ICD10Code",
        "Medication",
        "ATCCode",
        "Observation",
        "LOINCCode",
        "Practitioner",
        "DICOM Study",
        "Lab Result",
        "Vital Signs",
        "Radiology Order",
        "Discharge Summary",
    ],
    "BrainChild Barncancerforskning": [
        "VCF",
        "Genomic Variant",
        "DNA Sequence",
        "Tumor Sample",
        "Specimen",
        "Biobank",
        "NGS",
        "BrainChild",
        "Copy Number Variation",
        "Structural Variant",
        "Mutation",
        "Germline Variant",
        "Somatic Variant",
    ],
    "ML Feature Store": [
        "Feature Store",
        "ML Feature",
        "Feature Engineering",
        "ML Model",
        "MLflow Model",
        "Model Registry",
        "Batch Scoring",
        "Prediction",
        "Risk Score",
        "Feature Drift",
        "Model Monitoring",
    ],
    "OMOP Forskningsdata": [
        "OMOP Concept",
        "OMOP CDM",
        "Condition Occurrence",
        "Drug Exposure",
        "Measurement",
        "Visit Occurrence",
        "Cohort",
        "De-identification",
    ],
}


def get_auth_headers():
    """Get authentication headers for Purview API"""
    cred = AzureCliCredential(process_timeout=60)
    token = cred.get_token("https://purview.azure.net/.default").token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def get_glossary_guid():
    """Get the GUID of the main glossary"""
    headers = get_auth_headers()
    response = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=30)
    response.raise_for_status()
    
    glossaries = response.json()
    if isinstance(glossaries, list):
        return glossaries[0]["guid"]
    return glossaries["guid"]


def get_all_glossary_terms(glossary_guid):
    """Fetch all glossary terms and create name→GUID mapping"""
    headers = get_auth_headers()
    all_terms = []
    offset = 0
    limit = 100
    
    print("📖 Fetching glossary terms...")
    
    while True:
        url = f"{ATLAS_API}/glossary/{glossary_guid}/terms?limit={limit}&offset={offset}"
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        terms = response.json()
        if not terms:
            break
            
        all_terms.extend(terms)
        print(f"   Fetched {len(all_terms)} terms so far...")
        
        if len(terms) < limit:
            break
        offset += limit
    
    # Create name→GUID mapping
    term_map = {term["name"]: term["guid"] for term in all_terms}
    print(f"✅ Loaded {len(term_map)} glossary terms\n")
    
    return term_map


def find_data_product_entity(product_name):
    """Find data product entity by name using Search API"""
    headers = get_auth_headers()
    search_url = f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview"
    
    body = {
        "keywords": product_name,
        "limit": 10,
        "filter": {"entityType": "healthcare_data_product"}
    }
    
    response = requests.post(search_url, headers=headers, json=body, timeout=30)
    response.raise_for_status()
    
    results = response.json().get("value", [])
    
    # Find exact match
    for result in results:
        if result.get("name", "").lower() == product_name.lower():
            return result.get("id")
    
    return None


def get_entity_details(entity_guid):
    """Get full entity details including current relationships"""
    headers = get_auth_headers()
    url = f"{ATLAS_API}/entity/guid/{entity_guid}"
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    return response.json()


def link_terms_to_entity(entity_guid, term_guids):
    """Link glossary terms to entity using relationship API"""
    headers = get_auth_headers()
    
    success_count = 0
    failed_count = 0
    
    for term_guid in term_guids:
        # Create relationship payload
        relationship = {
            "typeName": "AtlasGlossarySemanticAssignment",
            "attributes": {},
            "guid": -1,
            "end1": {
                "guid": term_guid,
                "typeName": "AtlasGlossaryTerm"
            },
            "end2": {
                "guid": entity_guid,
                "typeName": "healthcare_data_product"
            }
        }
        
        try:
            response = requests.post(
                f"{ATLAS_API}/relationship",
                headers=headers,
                json=relationship,
                timeout=30
            )
            
            if response.status_code in [200, 204]:
                success_count += 1
                print(f"      ✅ Linked term")
            else:
                failed_count += 1
                print(f"      ⚠️  Failed (status {response.status_code})")
                
        except Exception as e:
            failed_count += 1
            print(f"      ❌ Error: {e}")
    
    return success_count, failed_count


def main():
    print("=" * 70)
    print("🔗 LINK GLOSSARY TERMS TO DATA PRODUCTS")
    print("=" * 70)
    print(f"Purview Account: {PURVIEW_ACCOUNT}")
    print(f"Data Products: {len(DATA_PRODUCT_TERMS)}")
    print()
    
    try:
        # Step 1: Get glossary GUID
        print("📚 Step 1: Getting glossary GUID...")
        glossary_guid = get_glossary_guid()
        print(f"   Glossary GUID: {glossary_guid}\n")
        
        # Step 2: Load all glossary terms
        term_map = get_all_glossary_terms(glossary_guid)
        
        # Step 3: Process each data product
        total_success = 0
        total_failed = 0
        total_missing = 0
        
        for product_name, term_names in DATA_PRODUCT_TERMS.items():
            print(f"📦 Processing: {product_name}")
            print(f"   Terms to link: {len(term_names)}")
            
            # Find data product entity
            entity_guid = find_data_product_entity(product_name)
            
            if not entity_guid:
                print(f"   ⚠️  Data product not found in catalog!\n")
                continue
            
            print(f"   Entity GUID: {entity_guid}")
            
            # Map term names to GUIDs
            term_guids = []
            missing_terms = []
            
            for term_name in term_names:
                if term_name in term_map:
                    term_guids.append(term_map[term_name])
                else:
                    missing_terms.append(term_name)
            
            if missing_terms:
                print(f"   ⚠️  Missing terms in glossary: {', '.join(missing_terms)}")
                total_missing += len(missing_terms)
            
            # Link terms to entity
            if term_guids:
                print(f"   🔗 Linking {len(term_guids)} terms...")
                success, failed = link_terms_to_entity(entity_guid, term_guids)
                total_success += success
                total_failed += failed
                print(f"   ✅ Linked: {success} | ❌ Failed: {failed}")
            
            print()
        
        # Summary
        print("=" * 70)
        print("📊 SUMMARY")
        print("=" * 70)
        print(f"✅ Successfully linked: {total_success} terms")
        print(f"❌ Failed to link: {total_failed} terms")
        print(f"⚠️  Terms not found in glossary: {total_missing}")
        print()
        
        if total_success > 0:
            print("🎉 Data products updated with glossary terms!")
            print("   Verify in Purview Portal: Data Catalog → Data Products")
        
        if total_missing > 0:
            print("\n⚠️  Some terms were not found in glossary.")
            print("   Run verify_all_purview.py to see available terms.")
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
