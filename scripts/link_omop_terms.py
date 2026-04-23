"""
Link glossary terms to OMOP Forskningsdata data product
"""
import requests
from azure.identity import AzureCliCredential
import time

PURVIEW_ACCOUNT = "prviewacc"
ATLAS_API = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/catalog/api/atlas/v2"
SEARCH_API = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/catalog/api/search/query"

OMOP_TERMS = [
    "OMOP CDM",
    "OMOP Concept",
    "Condition Occurrence",
    "Drug Exposure",
    "Measurement",
    "Visit Occurrence",
    "Cohort",
    "De-identification"
]

def get_auth_headers():
    """Get authentication headers for Purview API"""
    credential = AzureCliCredential()
    token = credential.get_token("https://purview.azure.net/.default")
    return {
        "Authorization": f"Bearer {token.token}",
        "Content-Type": "application/json"
    }

def get_glossary_terms():
    """Get all glossary terms from Purview"""
    print(f"📖 Fetching glossary terms from {PURVIEW_ACCOUNT}...")
    headers = get_auth_headers()
    
    # Get glossary GUID first
    glossaries = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=30)
    glossary_guid = glossaries.json()[0]["guid"]
    
    # Get all terms
    all_terms = []
    offset = 0
    limit = 100
    
    while True:
        response = requests.get(
            f"{ATLAS_API}/glossary/{glossary_guid}/terms",
            headers=headers,
            params={"limit": limit, "offset": offset},
            timeout=30
        )
        terms = response.json()
        if not terms:
            break
        all_terms.extend(terms)
        offset += limit
        if len(terms) < limit:
            break
    
    print(f"✅ Loaded {len(all_terms)} glossary terms")
    # Use 'name' instead of 'displayText' for term names
    return {term.get("name", term.get("displayText", "")): term["guid"] for term in all_terms}

def find_omop_entity(max_retries=3):
    """Find OMOP Forskningsdata entity with retry logic"""
    headers = get_auth_headers()
    
    for attempt in range(max_retries):
        try:
            print(f"   Attempt {attempt + 1}/{max_retries} to find OMOP entity...")
            body = {
                "keywords": "OMOP Forskningsdata",
                "filter": {
                    "entityType": "healthcare_data_product"
                }
            }
            response = requests.post(
                f"{SEARCH_API}?api-version=2022-08-01-preview",
                headers=headers,
                json=body,
                timeout=30
            )
            
            if response.status_code == 200:
                results = response.json().get("value", [])
                for result in results:
                    if result.get("name") == "OMOP Forskningsdata":
                        guid = result.get("id")
                        print(f"   ✅ Found entity GUID: {guid}")
                        return guid
            
            print(f"   ⚠️  Attempt {attempt + 1} failed")
            if attempt < max_retries - 1:
                time.sleep(2)
                
        except Exception as e:
            print(f"   ❌ Error on attempt {attempt + 1}: {str(e)[:100]}")
            if attempt < max_retries - 1:
                time.sleep(2)
    
    return None

def link_terms_to_omop(entity_guid, term_guids, max_retries=3):
    """Link glossary terms to OMOP entity with retry logic"""
    headers = get_auth_headers()
    success = 0
    failed = 0
    
    print(f"🔗 Linking {len(term_guids)} terms to OMOP...")
    
    for term_name, term_guid in term_guids.items():
        for attempt in range(max_retries):
            try:
                relationship = {
                    "typeName": "AtlasGlossarySemanticAssignment",
                    "attributes": {},
                    "guid": -1,
                    "end1": {"guid": term_guid, "typeName": "AtlasGlossaryTerm"},
                    "end2": {"guid": entity_guid, "typeName": "healthcare_data_product"}
                }
                
                response = requests.post(
                    f"{ATLAS_API}/relationship",
                    headers=headers,
                    json=relationship,
                    timeout=30
                )
                
                if response.status_code in [200, 201]:
                    print(f"   ✅ {term_name}")
                    success += 1
                    break
                elif response.status_code == 409:
                    print(f"   ⚠️  {term_name} (already linked)")
                    success += 1
                    break
                else:
                    print(f"   ⚠️  {term_name} failed (status {response.status_code})")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                    else:
                        failed += 1
                        
            except Exception as e:
                print(f"   ❌ {term_name} error: {str(e)[:80]}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    failed += 1
    
    return success, failed

def main():
    print("=" * 70)
    print("🔗 LINK GLOSSARY TERMS TO OMOP FORSKNINGSDATA")
    print("=" * 70)
    print()
    
    # Get all glossary terms
    glossary_terms = get_glossary_terms()
    print()
    
    # Find terms to link
    terms_to_link = {}
    missing = []
    
    for term_name in OMOP_TERMS:
        if term_name in glossary_terms:
            terms_to_link[term_name] = glossary_terms[term_name]
        else:
            missing.append(term_name)
    
    print(f"📋 Found {len(terms_to_link)}/{len(OMOP_TERMS)} terms in glossary")
    if missing:
        print(f"   ⚠️  Missing: {', '.join(missing)}")
    print()
    
    # Find OMOP entity
    print("🔍 Finding OMOP Forskningsdata entity...")
    entity_guid = find_omop_entity()
    
    if not entity_guid:
        print("❌ Failed to find OMOP Forskningsdata entity")
        return 1
    
    print()
    
    # Link terms
    success, failed = link_terms_to_omop(entity_guid, terms_to_link)
    
    print()
    print("=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"✅ Successfully linked: {success} terms")
    print(f"❌ Failed to link: {failed} terms")
    print()
    
    if success > 0:
        print("🎉 OMOP data product updated!")
        print("   Verify in Purview Portal: Data Catalog → Data Products → OMOP Forskningsdata")
    
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    exit(main())
