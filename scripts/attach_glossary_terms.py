"""Link glossary terms to each data product via relationships (entityType=TERM)."""
import requests
from azure.identity import AzureCliCredential

UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

# Product name -> list of glossary term names
MAPPING = {
    # --- Klinisk Vård ---
    'Klinisk Patientanalys': ['Patient', 'Encounter', 'Condition', 'Medication', 'Observation', 'ICD-10', 'Master Patient Index'],
    'Akutflödesmonitorering': ['Patient', 'Encounter', 'Condition', 'ICD-10'],
    'Vårdplatskapacitet': ['Patient', 'Encounter', 'Silver Layer', 'Gold Layer'],
    'Läkemedelsuppföljning Klinik': ['Medication', 'ATC', 'Patient', 'Encounter'],
    'Kardiologisk Kvalitetsuppföljning': ['Encounter', 'Condition', 'Patient', 'ICD-10', 'DICOM'],
    'Perioperativ Produktionsstyrning': ['Encounter', 'Patient', 'Silver Layer'],

    # --- Interoperabilitet & Standarder ---
    'FHIR Interoperabilitetslager': ['FHIR R4', 'HL7 FHIR', 'Patient', 'Observation'],
    'FHIR Interoperabilitetsnav': ['FHIR R4', 'HL7 FHIR', 'Patient', 'Observation'],
    'Masterdata Vårdhändelser': ['Master Patient Index', 'Patient', 'Encounter', 'FHIR R4'],
    'Standardiserad Vårdepisodmodell': ['OMOP CDM', 'Encounter', 'Condition', 'ICD-10'],
    'Terminologitjänst & Kodverk': ['ICD-10', 'SNOMED CT', 'LOINC', 'ATC'],
    'Terminologitjänst Kliniska Kodverk': ['ICD-10', 'SNOMED CT', 'LOINC', 'ATC'],

    # --- Forskning & Genomik ---
    'Precisionsonkologi Variantlager': ['VCF', 'NGS', 'Specimen', 'Biobank'],
    'Pediatrisk Imaging Research Hub': ['DICOM', 'Cohort', 'Real-World Evidence'],
    'Biobank & Provspårbarhet': ['Biobank', 'Specimen', 'GMS', 'Patient'],
    'Nationell Biobank Sammanställning': ['Biobank', 'Specimen', 'GMS', 'Cohort'],
    'Pediatrisk Precision Onkologi': ['NGS', 'VCF', 'DICOM', 'Cohort', 'Gold Layer'],
    'Radiogenomik Barnonkologi': ['DICOM', 'VCF', 'NGS', 'Cohort'],
    'Genomisk Forskningskohort': ['VCF', 'NGS', 'Cohort', 'Specimen', 'GMS'],
    'OMOP Forskningsplattform': ['OMOP CDM', 'Cohort', 'Real-World Evidence', 'Patient'],

    # --- Data & Analytics ---
    'Population Health Dashboard': ['Gold Layer', 'OMOP CDM', 'Real-World Evidence', 'Cohort'],
    'Operations Intelligence Mart': ['Gold Layer', 'Delta Lake', 'Data Lakehouse', 'Encounter'],
    'MLOps Modellregister': ['Gold Layer', 'Medallion Architecture', 'Data Lakehouse'],
    'MLOps Modelltelemetri': ['Gold Layer', 'Medallion Architecture', 'Data Governance'],
    'Prediktiv Vårdplatskapacitet': ['Gold Layer', 'Silver Layer', 'Encounter', 'Patient'],
    'Population Health Segmentering': ['Cohort', 'OMOP CDM', 'Gold Layer', 'Real-World Evidence'],
    'Medallion Data Platform': ['Bronze Layer', 'Silver Layer', 'Gold Layer', 'Medallion Architecture', 'Delta Lake', 'Data Lakehouse'],
    'Feature Store Klinisk ML': ['Silver Layer', 'Gold Layer', 'Delta Lake', 'Patient', 'Encounter'],

    # --- Hälsosjukvård / Governance ---
    'Compliance Kontrollbibliotek': ['Data Governance', 'De-identification', 'Personnummer'],
    'Informationsklassning Vårddata': ['Data Governance', 'Personnummer', 'De-identification', 'Patient'],
    'Audit Lineage Vårdplattform': ['Data Governance', 'Medallion Architecture'],
    'Informationsklassning & Policyefterlevnad': ['Data Governance', 'Personnummer', 'De-identification'],
    'Åtkomstgranskning & Behörighetskontroll': ['Data Governance', 'Personnummer'],
    'Lineage & Data Risk Office': ['Data Governance', 'Medallion Architecture', 'Delta Lake'],
}


def main():
    t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
    h = {'Authorization': f'Bearer {t}', 'Content-Type': 'application/json'}

    terms = requests.get(f'{UNIFIED}/terms?api-version={VER}', headers=h, timeout=30).json().get('value', [])
    term_by_name = {}
    for term in terms:
        if term.get('status') == 'Published':
            term_by_name.setdefault(term['name'], term['id'])
    print(f'Glossary terms available: {len(term_by_name)}')

    products = requests.get(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, timeout=30).json().get('value', [])
    prod_by_name = {p['name']: p['id'] for p in products}

    linked = skipped = failed = missing_term = missing_prod = 0

    for prod_name, term_names in MAPPING.items():
        pid = prod_by_name.get(prod_name)
        if not pid:
            print(f'[SKIP product] {prod_name}')
            missing_prod += 1
            continue
        rr = requests.get(
            f'{UNIFIED}/dataProducts/{pid}/relationships?api-version={VER}&entityType=TERM',
            headers=h, timeout=20,
        )
        existing = {x['entityId'] for x in rr.json().get('value', [])} if rr.status_code == 200 else set()
        print(f'\n=== {prod_name} (existing TERM links: {len(existing)}) ===')
        for tn in term_names:
            tid = term_by_name.get(tn)
            if not tid:
                print(f'  [!] term not found: {tn}')
                missing_term += 1
                continue
            if tid in existing:
                print(f'  = already linked: {tn}')
                skipped += 1
                continue
            resp = requests.post(
                f'{UNIFIED}/dataProducts/{pid}/relationships?api-version={VER}&entityType=TERM',
                headers=h, json={'entityId': tid, 'relationshipType': 'Related'}, timeout=20,
            )
            if resp.status_code in (200, 201):
                print(f'  + linked {tn}')
                linked += 1
            else:
                print(f'  FAIL {tn}: {resp.status_code} {resp.text[:180]}')
                failed += 1

    print('\n=== DONE ===')
    print(f'Linked:        {linked}')
    print(f'Skipped:       {skipped}')
    print(f'Failed:        {failed}')
    print(f'Missing term:  {missing_term}')
    print(f'Missing prod:  {missing_prod}')


if __name__ == '__main__':
    main()
