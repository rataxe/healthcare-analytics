#!/usr/bin/env python3
"""
Update all 4 data products with detailed metadata using /entity/bulk
"""
import requests
import json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

ATLAS = 'https://prviewacc.purview.azure.com/catalog/api/atlas/v2'
SEARCH = 'https://prviewacc.purview.azure.com/catalog/api/search/query?api-version=2022-08-01-preview'

# Find all data products
search_body = {
    'keywords': '*',
    'limit': 10,
    'filter': {'entityType': 'healthcare_data_product'}
}
r_search = requests.post(SEARCH, headers=h, json=search_body, timeout=30)
data_products = r_search.json().get('value', [])

print(f"Found {len(data_products)} data products")
print("="*80)

# Detailed metadata for each product
PRODUCT_DETAILS = {
    "Klinisk Patientanalys": {
        "userDescription": "Omfattar patientdemografi, diagnoser (ICD-10), läkemedel (ATC), laboratorieprov (LOINC) och radiologiska studier (DICOM). Innehåller data för 10,000 patienter med fokus på vårdkvalitet och prediktiv analys.",
        "use_cases": "LOS-prediktion (LightGBM) | Återinläggningsrisk (Random Forest) | Charlson Comorbidity Index | Avdelningsstatistik | GDPR-compliance | Kliniskt beslutsfattande",
        "sla": "Daglig uppdatering, <1h latens, 99.5% tillgänglighet, 10 års retention",
        "product_owners": "Healthcare Analytics Team | Clinical Data Stewards | Dr. Anna Svensson",
        "tables": "patients, encounters, diagnoses, medications, vitals_labs, observations, dicom_studies",
        "quality_score": 0.95,
        "product_status": "Published",
        "product_type": "Analytics"
    },
    "BrainChild Barncancerforskning": {
        "userDescription": "Genomisk data från barncancerpatienter inklusive VCF-filer, CNV-analys, COSMIC varianter och BTB (Brain Tumor Board) protokoll. Integrerar med BrainChild biobank för longitudinell forskning.",
        "use_cases": "Genomisk variant-analys | CNV-detection | COSMIC mutation lookup | Klinisk genotypning | Personlig medicin | Biobank-korrelation",
        "sla": "Veckovis uppdatering, <24h latens, 99.9% tillgänglighet, 50 års retention",
        "product_owners": "BrainChild Research Team | Genomics Lab | Prof. Lars Bergström",
        "tables": "vcf_files, specimens, cosmic_variants, cnv_segments, btb_protocols",
        "quality_score": 0.98,
        "product_status": "Published",
        "product_type": "Genomics"
    },
    "ML Feature Store": {
        "userDescription": "Centraliserad feature store för maskininlärningsmodeller. Inkluderar Bronze (rå data), Silver (validerad), Gold (aggregerad) lager. Stöder feature versioning, point-in-time lookups och model registry.",
        "use_cases": "Feature engineering | Model training | Real-time inference | Feature lineage tracking | Model versioning | A/B testing",
        "sla": "Real-time uppdatering, <100ms latens för inference, 99.99% tillgänglighet, 5 års retention",
        "product_owners": "ML Engineering Team | Data Science Team | Dr. Maria Lindqvist",
        "tables": "bronze_features, silver_features, gold_features, model_registry, feature_lineage",
        "quality_score": 0.92,
        "product_status": "Published",
        "product_type": "ML_Features"
    },
    "OMOP Forskningsdata": {
        "userDescription": "Standardiserad forskningsdata enligt OMOP CDM v5.4 med vocabularies (SNOMED CT, ATC, LOINC, ICD-10). Används för observationsstudier, kohort-analys och real-world evidence (RWE) forskning.",
        "use_cases": "Kohort-identifiering | Observationsstudier | Drug-safety analys | Real-world evidence | Komparativ effektivitet | Vocabularies mapping",
        "sla": "Månatlig uppdatering, <7 dagar latens, 99.9% tillgänglighet, Indefinite retention",
        "product_owners": "Clinical Research Team | OMOP Data Stewards | Dr. Erik Johansson",
        "tables": "person, condition_occurrence, drug_exposure, measurement, visit_occurrence, specimen, concept, vocabulary",
        "quality_score": 0.96,
        "product_status": "Published",
        "product_type": "Research"
    }
}

updated_count = 0
failed_count = 0

for dp in data_products:
    name = dp.get('name')
    guid = dp.get('id')
    
    if name not in PRODUCT_DETAILS:
        print(f"⚠️  Skipping {name} - no details defined")
        continue
    
    print(f"\n📦 Updating: {name}")
    print(f"   GUID: {guid}")
    
    # Get full entity
    r_get = requests.get(f'{ATLAS}/entity/guid/{guid}', headers=h, timeout=30)
    if r_get.status_code != 200:
        print(f"   ❌ Could not GET entity: {r_get.status_code}")
        failed_count += 1
        continue
    
    entity = r_get.json().get('entity', {})
    
    # Remove timestamp fields
    for field in ['lastModifiedTS', 'createTime', 'updateTime']:
        entity.pop(field, None)
    
    # Update attributes
    details = PRODUCT_DETAILS[name]
    entity['attributes'].update(details)
    
    # Update using /entity/bulk
    r_update = requests.post(
        f'{ATLAS}/entity/bulk',
        headers=h,
        json={'entities': [entity]},
        timeout=30
    )
    
    if r_update.status_code == 200:
        print(f"   ✅ Updated successfully!")
        updated_count += 1
    else:
        print(f"   ❌ Update failed: {r_update.status_code}")
        print(f"      {r_update.text[:200]}")
        failed_count += 1

print("\n" + "="*80)
print(f"Update Summary:")
print(f"  ✅ Updated: {updated_count}")
print(f"  ❌ Failed: {failed_count}")
print("="*80)
