#!/usr/bin/env python3
"""
Link Data Products and Glossary Terms to Governance Domains
Populates the 4 manually created governance domains with business concepts
"""
import requests
import time
from azure.identity import AzureCliCredential

# ══════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════
PURVIEW_ACCOUNT = "prviewacc"
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
ATLAS_API = f"{PURVIEW_ENDPOINT}/catalog/api/atlas/v2"

# Governance Domain Mappings (based on MANUAL_GOVERNANCE_DOMAINS.md)
DOMAIN_MAPPINGS = {
    "Klinisk Vård": {
        "data_products": ["Klinisk Patientanalys"],
        "glossary_terms": [
            # Klinisk Data (29 terms)
            "Patient", "Encounter", "Condition", "Medication", "Observation",
            "Practitioner", "DICOM Study", "Lab Result", "Vital Signs",
            "Radiology Order", "Discharge Summary", "Swedish Personnummer",
            "ICD10Code", "ATCCode", "LOINCCode", "SNOMED CT Code",
            "Sjukvårdstillfälle", "Vårdkontakt", "Primärvård", "Slutenvård",
            "Öppenvård", "Akutvård", "Diagnosregister", "Läkemedelsordinering",
            "Recept", "Dos", "Förskrivare", "Läkemedelsinteraktion", "Biverkning",
            # FHIR R4 (11 terms)
            "FHIR R4", "Bundle", "Composition", "DocumentReference", "Binary",
            "MessageHeader", "CarePlan", "ServiceRequest", "DiagnosticReport",
            "ImagingStudy", "StructuredDataCapture"
        ]
    },
    
    "Forskning & Genomik": {
        "data_products": ["BrainChild Barncancerforskning"],
        "glossary_terms": [
            # Barncancerforskning (13 terms)
            "VCF", "Genomic Variant", "DNA Sequence", "Tumor Sample", "Specimen",
            "Biobank", "NGS", "BrainChild", "Copy Number Variation",
            "Structural Variant", "Mutation", "Germline Variant", "Somatic Variant",
            # OMOP (8 terms)
            "OMOP CDM", "OMOP Concept", "Condition Occurrence", "Drug Exposure",
            "Measurement", "Visit Occurrence", "Cohort", "De-identification"
        ]
    },
    
    "Interoperabilitet & Standarder": {
        "data_products": ["OMOP Forskningsdata"],
        "glossary_terms": [
            # Interoperabilitet (12 terms)
            "HL7 FHIR", "HL7 v2", "DICOM", "PACS", "RIS", "XDS", "IHE",
            "CDA", "Meddelandestandard", "Interoperabilitet", "Structured Data",
            "Fast Healthcare Interoperability Resources",
            # Kliniska Standarder (6 terms)
            "ICD-10", "ATC", "LOINC", "SNOMED CT", "Swedish Coding System",
            "Terminologi"
        ]
    },
    
    "Data & Analytics": {
        "data_products": ["ML Feature Store"],
        "glossary_terms": [
            # ML & Prediktioner (11 terms)
            "Feature Store", "ML Feature", "Feature Engineering", "ML Model",
            "MLflow Model", "Model Registry", "Batch Scoring", "Prediction",
            "Risk Score", "Feature Drift", "Model Monitoring",
            # Dataarkitektur (5 terms)
            "Bronze Layer", "Silver Layer", "Gold Layer", "Data Lakehouse",
            "Delta Lake"
        ]
    }
}

# ══════════════════════════════════════════════════════════
# AUTHENTICATION
# ══════════════════════════════════════════════════════════
print("🔐 Authenticating with Azure...")
credential = AzureCliCredential(process_timeout=30)
token = credential.get_token("https://purview.azure.net/.default").token
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════
def sep(title=""):
    """Print section separator"""
    print(f"\n{'═'*70}")
    if title:
        print(f"  {title}")
        print('═'*70)

def get_all_glossary_terms():
    """Get all glossary terms with their GUIDs"""
    try:
        # Get glossary GUID
        r = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=30)
        if r.status_code != 200:
            print(f"❌ Could not fetch glossary: {r.status_code}")
            return {}
        
        data = r.json()
        glossaries = data if isinstance(data, list) else [data]
        glossary_guid = glossaries[0]['guid']
        
        # Get all terms
        all_terms = {}
        offset = 0
        limit = 100
        
        while True:
            r = requests.get(
                f"{ATLAS_API}/glossary/{glossary_guid}/terms?limit={limit}&offset={offset}",
                headers=headers,
                timeout=30
            )
            
            if r.status_code != 200:
                break
            
            terms = r.json()
            if not terms:
                break
            
            for term in terms:
                name = term.get("name", term.get("displayText", ""))
                if name:
                    all_terms[name] = term["guid"]
            
            if len(terms) < limit:
                break
            
            offset += limit
        
        print(f"✅ Loaded {len(all_terms)} glossary terms")
        return all_terms
        
    except Exception as e:
        print(f"❌ Error loading terms: {e}")
        return {}

def find_data_product_guid(product_name, max_retries=3):
    """Find data product entity GUID"""
    for attempt in range(1, max_retries + 1):
        try:
            body = {
                "keywords": product_name,
                "limit": 10,
                "filter": {"entityType": "healthcare_data_product"}
            }
            
            r = requests.post(
                f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
                headers=headers,
                json=body,
                timeout=30
            )
            
            if r.status_code == 200:
                results = r.json().get("value", [])
                for entity in results:
                    if entity.get("name") == product_name:
                        return entity.get("id")
            
            return None
            
        except requests.exceptions.SSLError:
            if attempt < max_retries:
                time.sleep(2)
                continue
            return None
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return None

def find_governance_domain_guid(domain_name):
    """Find governance domain entity GUID"""
    try:
        # Search for governance domains
        body = {
            "keywords": domain_name,
            "limit": 20,
            "filter": {}
        }
        
        r = requests.post(
            f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
            headers=headers,
            json=body,
            timeout=30
        )
        
        if r.status_code == 200:
            results = r.json().get("value", [])
            
            # Look for domain entity type
            for entity in results:
                entity_type = entity.get("entityType", "").lower()
                entity_name = entity.get("name", "")
                
                # Check if this is a governance domain
                if "domain" in entity_type and domain_name.lower() in entity_name.lower():
                    return entity.get("id")
        
        return None
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def link_term_to_domain(term_guid, term_name, domain_guid, domain_name):
    """Link glossary term to governance domain"""
    try:
        # Create relationship between term and domain
        relationship = {
            "typeName": "AtlasGlossaryTermAnchor",  # or appropriate domain relationship type
            "attributes": {},
            "guid": -1,
            "end1": {"guid": term_guid, "typeName": "AtlasGlossaryTerm"},
            "end2": {"guid": domain_guid, "typeName": "DataDomain"}
        }
        
        r = requests.post(
            f"{ATLAS_API}/relationship",
            headers=headers,
            json=relationship,
            timeout=30
        )
        
        if r.status_code in [200, 201]:
            return True
        elif r.status_code == 409:
            # Already linked
            return True
        else:
            return False
            
    except Exception as e:
        return False

def link_data_product_to_domain(product_guid, product_name, domain_guid, domain_name):
    """Link data product to governance domain"""
    try:
        # Try to update data product entity with domain reference
        r = requests.get(
            f"{ATLAS_API}/entity/guid/{product_guid}",
            headers=headers,
            timeout=30
        )
        
        if r.status_code != 200:
            return False
        
        entity = r.json().get("entity", {})
        attributes = entity.get("attributes", {})
        
        # Add domain reference
        attributes["domain"] = domain_name
        attributes["domainGuid"] = domain_guid
        
        entity["attributes"] = attributes
        
        r2 = requests.put(
            f"{ATLAS_API}/entity",
            headers=headers,
            json={"entity": entity},
            timeout=30
        )
        
        return r2.status_code in [200, 201]
        
    except Exception as e:
        return False

# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    sep("🔗 LINKING DATA PRODUCTS & GLOSSARY TERMS TO DOMAINS")
    
    # Load all glossary terms
    all_terms = get_all_glossary_terms()
    if not all_terms:
        print("❌ Could not load glossary terms")
        return 1
    
    total_linked = 0
    total_failed = 0
    
    for domain_name, mappings in DOMAIN_MAPPINGS.items():
        sep(f"Processing Domain: {domain_name}")
        
        # Find domain GUID
        print("🔍 Finding governance domain...")
        domain_guid = find_governance_domain_guid(domain_name)
        
        if not domain_guid:
            print(f"   ⚠️  Could not find domain '{domain_name}'")
            print(f"   💡 Make sure domain is created in Purview Portal:")
            print(f"      https://purview.microsoft.com → Unified Catalog → Governance domains")
            continue
        
        print(f"   ✅ Found domain GUID: {domain_guid}")
        
        # Link data products
        print(f"\n📦 Linking {len(mappings['data_products'])} data products...")
        for product_name in mappings["data_products"]:
            product_guid = find_data_product_guid(product_name)
            
            if product_guid:
                if link_data_product_to_domain(product_guid, product_name, domain_guid, domain_name):
                    print(f"   ✅ {product_name}")
                    total_linked += 1
                else:
                    print(f"   ⚠️  {product_name} (could not link)")
                    total_failed += 1
            else:
                print(f"   ⚠️  {product_name} (not found)")
                total_failed += 1
        
        # Link glossary terms
        print(f"\n📖 Linking {len(mappings['glossary_terms'])} glossary terms...")
        linked_count = 0
        missing_count = 0
        
        for term_name in mappings["glossary_terms"]:
            if term_name in all_terms:
                term_guid = all_terms[term_name]
                if link_term_to_domain(term_guid, term_name, domain_guid, domain_name):
                    linked_count += 1
                else:
                    missing_count += 1
            else:
                missing_count += 1
        
        print(f"   ✅ Linked: {linked_count}")
        if missing_count > 0:
            print(f"   ⚠️  Missing or failed: {missing_count}")
        
        total_linked += linked_count
        total_failed += missing_count
        
        time.sleep(1)
    
    sep("SUMMARY")
    print(f"✅ Successfully linked: {total_linked}")
    print(f"⚠️  Failed or missing: {total_failed}")
    
    print(f"\n📊 View in Purview Portal:")
    print(f"   https://purview.microsoft.com")
    print(f"   → Unified Catalog → Governance domains")
    print(f"\n💡 Each domain should now show:")
    print(f"   - Data products (1 per domain)")
    print(f"   - Glossary terms (15-40 per domain)")
    print(f"   - Critical data elements (populated by populate_data_product_details.py)")
    print(f"   - OKRs (populated by populate_data_product_details.py)")
    
    return 0 if total_failed == 0 else 1

if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
