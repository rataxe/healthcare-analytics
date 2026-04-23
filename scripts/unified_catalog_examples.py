#!/usr/bin/env python3
"""
UNIFIED CATALOG API - PRAKTISKA EXEMPEL
Användningsfall för Region Gävleborg Healthcare Analytics
"""
import json
from unified_catalog_client import UnifiedCatalogClient

def example1_bulk_import_fhir_terms():
    """
    Exempel 1: Bulk-import av FHIR-termer till Glossary
    
    Importera standardiserade FHIR-resurser och terminologier direkt
    från terminologisystem till Purview Glossary.
    """
    print("="*80)
    print("  EXEMPEL 1: BULK-IMPORT AV FHIR-TERMER")
    print("="*80)
    
    client = UnifiedCatalogClient()
    
    # FHIR-termer med definitioner från HL7 FHIR standard
    fhir_terms = [
        {
            'name': 'FHIR Patient',
            'definition': 'Demographics and other administrative information about an individual receiving care or other health-related services.',
            'domainId': '<klinisk-vård-domain-id>',
            'source': 'HL7 FHIR R4',
            'synonyms': ['Patienten', 'Subject of Care'],
            'relatedTerms': ['Person', 'Individual']
        },
        {
            'name': 'FHIR Observation',
            'definition': 'Measurements and simple assertions made about a patient, device or other subject.',
            'domainId': '<klinisk-vård-domain-id>',
            'source': 'HL7 FHIR R4',
            'synonyms': ['Mätning', 'Observation', 'Vital Signs'],
            'examples': ['Blood Pressure', 'Heart Rate', 'Lab Results']
        },
        {
            'name': 'FHIR Encounter',
            'definition': 'An interaction between a patient and healthcare provider(s) for the purpose of providing healthcare service(s) or assessing the health status of a patient.',
            'domainId': '<klinisk-vård-domain-id>',
            'source': 'HL7 FHIR R4',
            'synonyms': ['Vårdtillfälle', 'Visit', 'Kontakt']
        },
        {
            'name': 'FHIR Condition',
            'definition': 'A clinical condition, problem, diagnosis, or other event, situation, issue, or clinical concept that has risen to a level of concern.',
            'domainId': '<klinisk-vård-domain-id>',
            'source': 'HL7 FHIR R4',
            'synonyms': ['Diagnos', 'Problem', 'Clinical Finding']
        },
        {
            'name': 'FHIR Procedure',
            'definition': 'An action that is or was performed on or for a patient.',
            'domainId': '<klinisk-vård-domain-id>',
            'source': 'HL7 FHIR R4',
            'synonyms': ['Åtgärd', 'Intervention', 'Treatment']
        },
        {
            'name': 'FHIR MedicationRequest',
            'definition': 'An order or request for both supply of the medication and the instructions for administration of the medication to a patient.',
            'domainId': '<klinisk-vård-domain-id>',
            'source': 'HL7 FHIR R4',
            'synonyms': ['Läkemedelsordination', 'Prescription', 'Medicinförskrivning']
        }
    ]
    
    print("\n📋 Importerar FHIR-termer till Glossary...")
    print(f"   Totalt: {len(fhir_terms)} termer")
    
    # Bulk import
    try:
        result = client.bulk_create_glossary_terms(fhir_terms)
        print(f"\n✅ Import lyckades!")
        print(f"   Skapade: {result.get('created', 0)}")
        print(f"   Uppdaterade: {result.get('updated', 0)}")
        print(f"   Fel: {result.get('failed', 0)}")
    except Exception as e:
        print(f"\n❌ Import misslyckades: {e}")
        print("\nNOTE: Detta är ett exempel. Ersätt '<klinisk-vård-domain-id>' med")
        print("      ett verkligt domain ID från din Purview instance.")


def example2_auto_create_data_product():
    """
    Exempel 2: Skapa Data Products programmatiskt
    
    Automatiskt skapa en data product när en ny Lakehouse/Warehouse
    skapas i Fabric, med metadata från schema.
    """
    print("\n" + "="*80)
    print("  EXEMPEL 2: AUTO-SKAPA DATA PRODUCT VID FABRIC DEPLOYMENT")
    print("="*80)
    
    client = UnifiedCatalogClient()
    
    # Simulera metadata från en ny Fabric Lakehouse
    lakehouse_metadata = {
        'name': 'OMOP Clinical Data',
        'workspace': 'Healthcare Analytics',
        'tables': ['person', 'visit_occurrence', 'condition_occurrence', 
                   'procedure_occurrence', 'drug_exposure', 'measurement'],
        'schema_version': 'OMOP CDM v5.4',
        'refresh_schedule': 'Daily at 02:00 UTC'
    }
    
    print(f"\n📦 Skapar Data Product för: {lakehouse_metadata['name']}")
    
    # Skapa data product
    product_definition = {
        'name': lakehouse_metadata['name'],
        'description': f"OMOP Clinical Data Warehouse containing {len(lakehouse_metadata['tables'])} core tables for research and analytics.",
        'domainId': '<forskning-genomik-domain-id>',
        'owners': ['clinical-data-team@gavleborg.se'],
        'type': 'Lakehouse',
        'source': lakehouse_metadata['workspace'],
        'tables': lakehouse_metadata['tables'],
        'refreshSchedule': lakehouse_metadata['refresh_schedule'],
        'quality': {
            'completeness': 0.98,
            'accuracy': 0.95,
            'timeliness': 'Daily'
        },
        'sla': {
            'availability': '99.5%',
            'latency': '<1 hour',
            'support': '24/7 on-call'
        },
        'tags': ['OMOP', 'Clinical', 'Research', 'PHI']
    }
    
    try:
        product = client.create_data_product(**product_definition)
        print(f"\n✅ Data Product skapad!")
        print(f"   ID: {product.get('id')}")
        print(f"   Name: {product.get('name')}")
        print(f"   Tables: {len(product.get('tables', []))}")
    except Exception as e:
        print(f"\n❌ Kunde inte skapa product: {e}")
        print("\nNOTE: Detta är ett exempel. Ersätt '<forskning-genomik-domain-id>' med")
        print("      ett verkligt domain ID från din Purview instance.")


def example3_quality_reporting():
    """
    Exempel 3: POC-rapportering - Hämta data quality scores
    
    Hämta kvalitetsdata från Purview och presentera i Power BI dashboard.
    """
    print("\n" + "="*80)
    print("  EXEMPEL 3: DATA QUALITY REPORTING FÖR POWER BI")
    print("="*80)
    
    client = UnifiedCatalogClient()
    
    print("\n📊 Hämtar data products med quality scores...")
    
    try:
        products = client.list_data_products()
        
        # Samla quality metrics
        quality_report = []
        for product in products:
            quality = product.get('quality', {})
            quality_report.append({
                'Product': product.get('name'),
                'Domain': product.get('domainId', 'Unknown'),
                'Completeness': quality.get('completeness', 0),
                'Accuracy': quality.get('accuracy', 0),
                'Timeliness': quality.get('timeliness', 'Unknown'),
                'Overall Score': (quality.get('completeness', 0) + 
                                quality.get('accuracy', 0)) / 2
            })
        
        print(f"\n✅ Hämtade quality metrics för {len(quality_report)} products")
        
        # Spara som JSON för Power BI
        output_file = 'scripts/quality_report.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(quality_report, f, indent=2, ensure_ascii=False)
        
        print(f"\n📄 Rapport sparad: {output_file}")
        print("\nAnvänd denna JSON-fil som datakälla i Power BI:")
        print("  1. Power BI Desktop → Get Data → JSON")
        print("  2. Välj quality_report.json")
        print("  3. Skapa visualiseringar för:")
        print("     • Quality score per domain")
        print("     • Trend over time (refresh daily)")
        print("     • Alert när score < threshold")
        
        # Visa top 5 products
        sorted_report = sorted(quality_report, 
                             key=lambda x: x['Overall Score'], 
                             reverse=True)
        print("\n📈 Top 5 Data Products (by quality score):")
        for i, product in enumerate(sorted_report[:5], 1):
            score = product['Overall Score']
            print(f"   {i}. {product['Product']}: {score:.2%}")
            
    except Exception as e:
        print(f"\n❌ Kunde inte hämta data: {e}")


def example4_cicd_governance():
    """
    Exempel 4: CI/CD-pipeline för governance
    
    Sätt governance metadata som del av Fabric deployment pipeline.
    """
    print("\n" + "="*80)
    print("  EXEMPEL 4: CI/CD PIPELINE FÖR GOVERNANCE METADATA")
    print("="*80)
    
    print("""
INTEGRATION MED AZURE DEVOPS / GITHUB ACTIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Pipeline steg:
1. Deploy Fabric artifacts (notebooks, pipelines, lakehouses)
2. Extract metadata (tables, columns, relationships)
3. Update Purview via Unified Catalog API
4. Validate data quality policies
5. Run integration tests

EXEMPEL YAML (Azure DevOps):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

trigger:
  branches:
    include:
      - main
  paths:
    include:
      - fabric/**

stages:
- stage: Deploy
  jobs:
  - job: DeployFabric
    steps:
    - task: PythonScript@0
      displayName: 'Deploy Fabric Artifacts'
      inputs:
        scriptPath: 'scripts/deploy_fabric.py'
    
    - task: PythonScript@0
      displayName: 'Update Purview Governance'
      inputs:
        scriptPath: 'scripts/update_purview_governance.py'
      env:
        PURVIEW_CLIENT_ID: $(PURVIEW_CLIENT_ID)
        PURVIEW_CLIENT_SECRET: $(PURVIEW_CLIENT_SECRET)
        PURVIEW_TENANT_ID: $(PURVIEW_TENANT_ID)
    
    - task: PythonScript@0
      displayName: 'Validate Data Policies'
      inputs:
        scriptPath: 'scripts/validate_policies.py'

EXEMPEL PYTHON SCRIPT (update_purview_governance.py):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
    
    sample_script = '''
import os
from unified_catalog_client import UnifiedCatalogClient

def update_governance_from_deployment():
    """Update Purview after Fabric deployment"""
    
    # Read deployment manifest
    with open('deployment_manifest.json') as f:
        manifest = json.load(f)
    
    client = UnifiedCatalogClient()
    
    for artifact in manifest['artifacts']:
        if artifact['type'] == 'lakehouse':
            # Create/update data product
            product = client.create_data_product(
                name=artifact['name'],
                description=artifact['description'],
                domainId=artifact['domain'],
                owners=artifact['owners'],
                tables=artifact['tables']
            )
            print(f"✅ Updated: {product['name']}")
        
        elif artifact['type'] == 'glossary_terms':
            # Bulk import terms
            result = client.bulk_create_glossary_terms(artifact['terms'])
            print(f"✅ Imported {result['created']} terms")

if __name__ == '__main__':
    update_governance_from_deployment()
'''
    
    print(sample_script)
    
    print("""
FÖRDELAR MED CI/CD GOVERNANCE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✅ Version control för governance metadata
✅ Automated testing av policies och quality rules
✅ Konsistent metadata mellan dev/test/prod
✅ Audit trail för alla ändringar
✅ Rollback-möjlighet vid fel
""")


def main():
    """Run all examples"""
    print("="*80)
    print("  UNIFIED CATALOG API - PRAKTISKA EXEMPEL FÖR REGION GÄVLEBORG")
    print("="*80)
    
    print("""
Dessa exempel visar hur ni kan automatisera governance med Purview
Unified Catalog API i er Healthcare Analytics-miljö.

Välj exempel att köra:
  [1] Bulk-import av FHIR-termer
  [2] Auto-skapa Data Product vid Fabric deployment
  [3] Data Quality reporting för Power BI
  [4] CI/CD pipeline för governance
  [5] Kör alla exempel (demo mode)
""")
    
    choice = input("\nVälj exempel (1-5): ").strip()
    
    if choice == '1':
        example1_bulk_import_fhir_terms()
    elif choice == '2':
        example2_auto_create_data_product()
    elif choice == '3':
        example3_quality_reporting()
    elif choice == '4':
        example4_cicd_governance()
    elif choice == '5':
        print("\n🔄 DEMO MODE - Visar alla exempel...\n")
        example1_bulk_import_fhir_terms()
        example2_auto_create_data_product()
        example3_quality_reporting()
        example4_cicd_governance()
    else:
        print("\n❌ Ogiltigt val")
    
    print("\n" + "="*80)
    print("  Exempel klara!")
    print("  Nästa steg: Anpassa exemplen för er specifika miljö")
    print("="*80)


if __name__ == '__main__':
    main()
