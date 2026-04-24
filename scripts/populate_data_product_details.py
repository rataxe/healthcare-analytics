#!/usr/bin/env python3
"""
Populate Data Product Details - Critical Elements & OKRs
Adds business metadata to all 4 data products in Purview
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

# ══════════════════════════════════════════════════════════
# DATA PRODUCT METADATA
# ══════════════════════════════════════════════════════════
DATA_PRODUCT_DETAILS = {
    "Klinisk Patientanalys": {
        "description": "Omfattar patientdemografi, diagnoser (ICD-10), vårdtillfällen, läkemedel (ATC), laboratorieprov (LOINC), vitala parametrar och DICOM-studier. Används för vårdkvalitetsanalys, patientflödesoptimering och klinisk beslutsfattande.",
        "owner": "joandolf@microsoft.com",
        "steward": "Healthcare Analytics Team",
        "data_quality_contact": "joandolf@microsoft.com",
        "use_cases": [
            "Vårdkvalitetsanalys och KPI-uppföljning",
            "Patientflödesoptimering och resursplanering",
            "Kliniskt beslutstöd för läkare",
            "Rapportering till myndigheter (SKR, Socialstyrelsen)",
            "Forskning kring vårdprocesser och behandlingsresultat"
        ],
        "critical_elements": [
            {
                "name": "Swedish Personnummer",
                "type": "PII",
                "sensitivity": "High",
                "retention": "10 years (legal requirement)",
                "description": "Patient identifier - must be protected under GDPR and Patient Data Act"
            },
            {
                "name": "ICD-10 Diagnosis Codes",
                "type": "Clinical",
                "sensitivity": "High",
                "retention": "10 years",
                "description": "Diagnosis codes - sensitive health information requiring strict access control"
            },
            {
                "name": "ATC Medication Codes",
                "type": "Clinical",
                "sensitivity": "High",
                "retention": "10 years",
                "description": "Medication data - protected health information"
            },
            {
                "name": "LOINC Lab Results",
                "type": "Clinical",
                "sensitivity": "Medium",
                "retention": "10 years",
                "description": "Laboratory test results - health information"
            },
            {
                "name": "DICOM Study IDs",
                "type": "Clinical",
                "sensitivity": "High",
                "retention": "10 years",
                "description": "Links to radiology imaging - sensitive data"
            }
        ],
        "okrs": [
            {
                "objective": "Förbättra datakvalitet i kliniska system",
                "key_results": [
                    ">=95% fullständighet för obligatoriska fält (personnummer, diagnos, datum)",
                    "<=2% fel i ICD-10 koder (validering mot kodverk)",
                    "<=5% duplicerade vårdtillfällen (inom 24h)"
                ]
            },
            {
                "objective": "Öka dataaktualitet för realtidsanalys",
                "key_results": [
                    "90% av data tillgänglig inom 1 timme från registrering",
                    "99.5% uptime för data product API",
                    "<=15 minuter latens från källa till Gold layer"
                ]
            },
            {
                "objective": "Säkerställ regelefterlevnad och dataskydd",
                "key_results": [
                    "100% GDPR-compliance för PII-hantering",
                    "Audit trail för alla åtkomster till personnummer",
                    "Kryptering av alla data at-rest och in-transit"
                ]
            }
        ],
        "sla": {
            "availability": "99.5%",
            "data_freshness": "1 hour",
            "support_hours": "8-17 CET weekdays"
        }
    },
    
    "BrainChild Barncancerforskning": {
        "description": "Omfattar genomisk data från barncancerpatienter med DNA-sekvensering, VCF-filer, tumörbiopsier, kliniska samband mellan genetiska varianter och behandlingssvar. Används för precisionsmedicin och forskningsprojekt inom pediatrisk onkologi.",
        "owner": "joandolf@microsoft.com",
        "steward": "BrainChild Research Team",
        "data_quality_contact": "joandolf@microsoft.com",
        "use_cases": [
            "Identifiering av terapeutiska targets i pediatriska hjärntumörer",
            "Precision medicine - matchning av varianter mot behandlingsprotokoll",
            "Biobank management och provtagningsstrategi",
            "Forskning kring somatiska vs germline varianter",
            "Kliniska studier för nya behandlingar"
        ],
        "critical_elements": [
            {
                "name": "VCF Files",
                "type": "Genomic",
                "sensitivity": "Very High",
                "retention": "15 years (research data)",
                "description": "Variant Call Format files - contains genetic variants, highly sensitive research data"
            },
            {
                "name": "Patient-Specimen Mapping",
                "type": "PII + Clinical",
                "sensitivity": "Very High",
                "retention": "15 years",
                "description": "Links patients to biobank specimens - must be pseudonymized"
            },
            {
                "name": "Somatic Mutations",
                "type": "Genomic",
                "sensitivity": "High",
                "retention": "15 years",
                "description": "Tumor-specific mutations - critical for treatment decisions"
            },
            {
                "name": "Germline Variants",
                "type": "Genomic",
                "sensitivity": "Very High",
                "retention": "15 years",
                "description": "Inherited genetic variants - may have implications for family members"
            },
            {
                "name": "Copy Number Variations",
                "type": "Genomic",
                "sensitivity": "High",
                "retention": "15 years",
                "description": "Chromosomal alterations - important for cancer characterization"
            }
        ],
        "okrs": [
            {
                "objective": "Öka genomisk databas för bättre forskningsunderlag",
                "key_results": [
                    ">=100 nya VCF-filer sekvenserade per år",
                    ">=80% av prover har komplett klinisk metadata",
                    ">=95% av varianter annoterade med klinisk betydelse (ClinVar, COSMIC)"
                ]
            },
            {
                "objective": "Förbättra variant interpretation pipeline",
                "key_results": [
                    "<=24 timmar från sekvensering till VCF-fil tillgänglig i Gold",
                    ">=90% automatisk klassificering (pathogenic/benign/VUS)",
                    "<=5% felklassificeringar jämfört med manuell granskning"
                ]
            },
            {
                "objective": "Säkerställ etisk forskning och dataskydd",
                "key_results": [
                    "100% informed consent dokumenterad för alla prover",
                    "Pseudonymisering av alla patient-specimen länkar",
                    "Årlig etisk granskning av forskningsprojekt"
                ]
            }
        ],
        "sla": {
            "availability": "99.0%",
            "data_freshness": "24 hours",
            "support_hours": "8-17 CET weekdays"
        }
    },
    
    "ML Feature Store": {
        "description": "Machine Learning features för prediktiva modeller: Length of Stay (LOS) prediction, återinläggningsrisk, feature engineering, batch scoring och modellmonitorering. Används för operativa beslut och kapacitetsplanering.",
        "owner": "joandolf@microsoft.com",
        "steward": "ML Engineering Team",
        "data_quality_contact": "joandolf@microsoft.com",
        "use_cases": [
            "LOS prediction för kapacitetsplanering",
            "Återinläggningsrisk identifiering",
            "Feature engineering för nya ML-modeller",
            "A/B testing av modellversioner",
            "Model monitoring och drift detection"
        ],
        "critical_elements": [
            {
                "name": "ML Features",
                "type": "Derived",
                "sensitivity": "Medium",
                "retention": "2 years (versioned)",
                "description": "Engineered features from clinical data - versioned and tracked"
            },
            {
                "name": "Model Predictions",
                "type": "Operational",
                "sensitivity": "Medium",
                "retention": "1 year",
                "description": "Prediction outputs - used for operational decision-making"
            },
            {
                "name": "Model Performance Metrics",
                "type": "Metadata",
                "sensitivity": "Low",
                "retention": "5 years",
                "description": "Accuracy, precision, recall, AUC - critical for model governance"
            },
            {
                "name": "Feature Importance Scores",
                "type": "Metadata",
                "sensitivity": "Low",
                "retention": "2 years",
                "description": "SHAP values and feature importance - for explainability"
            },
            {
                "name": "Training Data Lineage",
                "type": "Metadata",
                "sensitivity": "Medium",
                "retention": "5 years",
                "description": "Tracks which data was used to train each model version"
            }
        ],
        "okrs": [
            {
                "objective": "Förbättra modellprestanda och träffsäkerhet",
                "key_results": [
                    "LOS prediction: R² >= 0.75 och MAE <= 1.5 dagar",
                    "Återinläggningsrisk: AUC >= 0.80 och precision >= 70%",
                    "<=10% performance degradation mellan produktions- och träningsmiljö"
                ]
            },
            {
                "objective": "Accelerera feature engineering och deployment",
                "key_results": [
                    "<=4 veckor från idé till feature i produktion",
                    ">=90% automatiserad feature validering (null check, distribution check)",
                    "Versioning och reproducerbarhet för 100% av features"
                ]
            },
            {
                "objective": "Säkerställ modellövervakning och governance",
                "key_results": [
                    "Daglig monitoring av alla produktionsmodeller",
                    "Automatisk alert vid >15% drift i feature distributions",
                    "Månatlig modell-audit och fairness-analys"
                ]
            }
        ],
        "sla": {
            "availability": "99.9%",
            "data_freshness": "15 minutes",
            "support_hours": "24/7 (production models)"
        }
    },
    
    "OMOP Forskningsdata": {
        "description": "OMOP CDM v5.4 forskningsdatabas med person, condition_occurrence, drug_exposure, measurement, visit_occurrence, specimen och cohort tables. De-identifierad data för observational research och real-world evidence studies.",
        "owner": "joandolf@microsoft.com",
        "steward": "Clinical Research Team",
        "data_quality_contact": "joandolf@microsoft.com",
        "use_cases": [
            "Observational studies och real-world evidence",
            "Comparative effectiveness research",
            "Cohort building för kliniska studier",
            "Population health analytics",
            "Pharmaco-epidemiological studies"
        ],
        "critical_elements": [
            {
                "name": "OMOP Concept IDs",
                "type": "Clinical",
                "sensitivity": "Low",
                "retention": "Permanent",
                "description": "Standardized vocabulary concepts - ensures interoperability"
            },
            {
                "name": "De-identified Person IDs",
                "type": "Pseudonymized",
                "sensitivity": "Medium",
                "retention": "10 years",
                "description": "De-identified patient identifiers - no direct PII but linkable within dataset"
            },
            {
                "name": "Condition Occurrence",
                "type": "Clinical",
                "sensitivity": "Medium",
                "retention": "10 years",
                "description": "Diagnosis data mapped to SNOMED CT - de-identified"
            },
            {
                "name": "Drug Exposure",
                "type": "Clinical",
                "sensitivity": "Medium",
                "retention": "10 years",
                "description": "Medication data mapped to RxNorm - de-identified"
            },
            {
                "name": "Measurement",
                "type": "Clinical",
                "sensitivity": "Medium",
                "retention": "10 years",
                "description": "Lab results mapped to LOINC - de-identified"
            }
        ],
        "okrs": [
            {
                "objective": "Öka OMOP-mappning och datakvalitet",
                "key_results": [
                    ">=90% av diagnoser mappade till SNOMED CT concepts",
                    ">=85% av läkemedel mappade till RxNorm concepts",
                    ">=95% av laboratorievärden mappade till LOINC concepts"
                ]
            },
            {
                "objective": "Stödja forskning med högkvalitativ data",
                "key_results": [
                    ">=5 publicerade forskningsstudier per år baserade på OMOP-data",
                    "<=3% missing data för obligatoriska OMOP CDM-fält",
                    "Data Quality Dashboard score >= 85%"
                ]
            },
            {
                "objective": "Säkerställ de-identifiering och etik",
                "key_results": [
                    "100% av data genomgår automatisk de-identifiering",
                    "Årlig extern audit av de-identifieringsprocess",
                    "Etiskt godkännande för alla forskningsprojekt med tillgång"
                ]
            }
        ],
        "sla": {
            "availability": "99.0%",
            "data_freshness": "Weekly (batch update)",
            "support_hours": "8-17 CET weekdays"
        }
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

def find_data_product_entity(product_name, max_retries=3):
    """Find data product entity GUID by name"""
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
                
                print(f"   ⚠️  Could not find exact match for '{product_name}'")
                return None
            else:
                print(f"   ⚠️  Search returned {r.status_code}")
                return None
                
        except requests.exceptions.SSLError as e:
            if attempt < max_retries:
                print(f"   ⚠️  SSL error, retrying ({attempt}/{max_retries})...")
                time.sleep(2)
                continue
            else:
                print(f"   ❌ SSL error after {max_retries} attempts")
                return None
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return None
    
    return None

def update_data_product_metadata(entity_guid, product_name, details):
    """Update data product entity with Critical Elements and OKRs"""
    
    # Prepare business metadata attributes
    critical_elements_json = str(details["critical_elements"])
    okrs_json = str(details["okrs"])
    use_cases_text = "\n".join([f"- {uc}" for uc in details["use_cases"]])
    
    try:
        # 1) Fetch full existing entity
        r_get = requests.get(
            f"{ATLAS_API}/entity/guid/{entity_guid}",
            headers=headers,
            timeout=30
        )

        if r_get.status_code != 200:
            print(f"   ⚠️  Could not fetch entity (HTTP {r_get.status_code})")
            print(f"      Response: {r_get.text[:200]}")
            return False

        entity = r_get.json().get("entity", {})
        attrs = entity.get("attributes", {})

        # 2) Update only business metadata attributes
        attrs.update({
            "name": product_name,
            "description": details["description"],
            "owner": details["owner"],
            "userDescription": details["description"],
            "criticalElements": critical_elements_json,
            "okrs": okrs_json,
            "useCases": use_cases_text,
            "steward": details["steward"],
            "dataQualityContact": details["data_quality_contact"],
            "slaAvailability": details["sla"]["availability"],
            "slaFreshness": details["sla"]["data_freshness"],
            "slaSupportHours": details["sla"]["support_hours"],
        })
        entity["attributes"] = attrs

        # Remove server-managed fields that can cause request validation errors
        for field in ["lastModifiedTS", "createTime", "updateTime", "status"]:
            entity.pop(field, None)

        # 3) Submit as bulk Atlas update (working pattern in this repo)
        r_update = requests.post(
            f"{ATLAS_API}/entity/bulk",
            headers=headers,
            json={"entities": [entity]},
            timeout=30
        )

        if r_update.status_code == 200:
            print(f"   ✅ Updated {product_name}")
            print(f"      - {len(details['critical_elements'])} critical elements")
            print(f"      - {len(details['okrs'])} OKRs")
            print(f"      - {len(details['use_cases'])} use cases")
            return True

        print(f"   ⚠️  Update failed (HTTP {r_update.status_code})")
        print(f"      Response: {r_update.text[:200]}")
        return False

    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def try_partial_update(entity_guid, details):
    """Try partial entity update via Atlas API"""
    try:
        # Get existing entity first
        r = requests.get(
            f"{ATLAS_API}/entity/guid/{entity_guid}",
            headers=headers,
            timeout=30
        )
        
        if r.status_code != 200:
            print(f"      Cannot fetch entity for partial update")
            return False
        
        entity = r.json().get("entity", {})
        attributes = entity.get("attributes", {})
        
        # Update only the new fields
        attributes["criticalElements"] = str(details["critical_elements"])
        attributes["okrs"] = str(details["okrs"])
        attributes["useCases"] = "\n".join([f"- {uc}" for uc in details["use_cases"]])
        attributes["steward"] = details["steward"]
        attributes["dataQualityContact"] = details["data_quality_contact"]
        
        entity["attributes"] = attributes
        
        r2 = requests.put(
            f"{ATLAS_API}/entity",
            headers=headers,
            json={"entity": entity},
            timeout=30
        )
        
        if r2.status_code in [200, 201]:
            print(f"      ✅ Partial update successful")
            return True
        else:
            print(f"      ⚠️  Partial update failed (HTTP {r2.status_code})")
            return False
            
    except Exception as e:
        print(f"      ❌ Partial update error: {e}")
        return False

# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    sep("📦 POPULATING DATA PRODUCT DETAILS")
    
    print(f"\nData Products to update: {len(DATA_PRODUCT_DETAILS)}")
    for name in DATA_PRODUCT_DETAILS.keys():
        print(f"   - {name}")
    
    success_count = 0
    fail_count = 0
    
    for product_name, details in DATA_PRODUCT_DETAILS.items():
        sep(f"Processing: {product_name}")
        
        # Step 1: Find entity GUID
        print(f"🔍 Finding entity GUID...")
        entity_guid = find_data_product_entity(product_name)
        
        if not entity_guid:
            print(f"   ❌ Could not find entity")
            fail_count += 1
            continue
        
        print(f"   ✅ Found GUID: {entity_guid}")
        
        # Step 2: Update metadata
        print(f"📝 Updating metadata...")
        if update_data_product_metadata(entity_guid, product_name, details):
            success_count += 1
        else:
            fail_count += 1
        
        time.sleep(1)  # Rate limiting
    
    sep("SUMMARY")
    print(f"✅ Successfully updated: {success_count}/{len(DATA_PRODUCT_DETAILS)}")
    print(f"❌ Failed: {fail_count}/{len(DATA_PRODUCT_DETAILS)}")
    
    if success_count > 0:
        print(f"\n📊 View in Purview Portal:")
        print(f"   https://web.purview.azure.com/resource/{PURVIEW_ACCOUNT}")
        print(f"   → Data Catalog → Data Products")
        print(f"\n✅ Each data product now has:")
        print(f"   - Use cases and business context")
        print(f"   - Critical data elements with sensitivity levels")
        print(f"   - OKRs (Objectives and Key Results)")
        print(f"   - SLA commitments (availability, freshness, support)")
    
    return 0 if fail_count == 0 else 1

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
