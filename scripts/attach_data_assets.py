"""Attach appropriate data assets to each of the 29 data products missing assets.

Uses the discovered asset pool (30 unique assets already registered in Purview)
and maps each product to semantically relevant assets based on its purpose.
"""
import json
import requests
from azure.identity import AzureCliCredential

UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

# Product -> list of asset names to attach. Names must exist in asset_pool.json.
# Assets are picked based on product purpose.
MAPPING = {
    # === Klinisk Vård ===
    'Akutflödesmonitorering': ['patients', 'encounters', 'diagnoses', 'HealthcareAnalyticsDB'],
    'Vårdplatskapacitet': ['encounters', 'patients', 'silver_patient', 'ml_predictions'],
    'Läkemedelsuppföljning Klinik': ['medications', 'patients', 'encounters', 'FHIR Patient'],
    'Kardiologisk Kvalitetsuppföljning': ['encounters', 'diagnoses', 'patients', 'DICOM MRI_Brain'],
    'Perioperativ Produktionsstyrning': ['encounters', 'patients', 'silver_patient', 'HealthcareAnalyticsDB'],

    # === Interoperabilitet & Standarder ===
    'FHIR Interoperabilitetslager': ['FHIR Patient', 'FHIR Observation', 'BrainChild FHIR Server (R4)'],
    'FHIR Interoperabilitetsnav': ['FHIR Patient', 'FHIR Observation', 'BrainChild FHIR Server (R4)'],
    'Masterdata Vårdhändelser': ['patients', 'silver_patient', 'FHIR Patient', 'encounters'],
    'Standardiserad Vårdepisodmodell': ['encounters', 'diagnoses', 'gold_omop', 'OMOP Forskningsdata'],
    'Terminologitjänst & Kodverk': ['diagnoses', 'medications', 'gold_omop'],
    'Terminologitjänst Kliniska Kodverk': ['diagnoses', 'medications', 'gold_omop'],

    # === Forskning & Genomik ===
    'Precisionsonkologi Variantlager': ['BrainChild DICOM Server', 'ml_features', 'silver_patient'],
    'Pediatrisk Imaging Research Hub': ['BrainChild DICOM Server', 'DICOM MRI_Brain',
                                        'brainchild_silver_dicom_studies', 'brainchild_silver_dicom_pathology'],
    'Biobank & Provspårbarhet': ['patients', 'silver_patient', 'diagnoses'],
    'Nationell Biobank Sammanställning': ['patients', 'silver_patient', 'diagnoses'],
    'Pediatrisk Precision Onkologi': ['brainchild_silver_dicom_pathology', 'brainchild_silver_dicom_studies',
                                      'silver_patient', 'ml_predictions'],
    'Radiogenomik Barnonkologi': ['DICOM MRI_Brain', 'brainchild_silver_dicom_studies', 'ml_features'],

    # === Data & Analytics ===
    'Population Health Dashboard': ['healthcare1_msft_gold_omop', 'OneLake Catalog - Governance for Admins',
                                    'gold_omop', 'silver_patient'],
    'Operations Intelligence Mart': ['DW', 'encounters', 'ml_predictions', 'HealthcareAnalyticsDB'],
    'MLOps Modellregister': ['los-predictor-lgbm', 'healthcare-analytics-v1', 'ml_predictions'],
    'MLOps Modelltelemetri': ['healthcare-analytics-v1', 'los-predictor-lgbm', 'ml_predictions'],
    'Prediktiv Vårdplatskapacitet': ['ml_predictions', 'ml_features', 'los-predictor-lgbm'],
    'Population Health Segmentering': ['ml_features', 'silver_patient', 'gold_omop'],

    # === Hälsosjukvård (governance) ===
    'Compliance Kontrollbibliotek': ['OneLake Catalog - Governance for Admins', 'HealthcareAnalyticsDB'],
    'Informationsklassning Vårddata': ['OneLake Catalog - Governance for Admins', 'patients', 'silver_patient'],
    'Audit Lineage Vårdplattform': ['OneLake Catalog - Governance for Admins', 'HealthcareAnalyticsDB', 'DW'],
    'Informationsklassning & Policyefterlevnad': ['OneLake Catalog - Governance for Admins', 'patients'],
    'Åtkomstgranskning & Behörighetskontroll': ['OneLake Catalog - Governance for Admins', 'HealthcareAnalyticsDB'],
    'Lineage & Data Risk Office': ['OneLake Catalog - Governance for Admins', 'HealthcareAnalyticsDB', 'DW'],
}


def main():
    t = AzureCliCredential().get_token('https://purview.azure.net/.default').token
    h = {'Authorization': f'Bearer {t}', 'Content-Type': 'application/json'}

    # Load asset pool
    with open('scripts/asset_pool.json', 'r', encoding='utf-8') as f:
        pool = json.load(f)

    # Index: name -> list of (assetId, record). Pick first unique by name.
    by_name = {}
    for aid, rec in pool.items():
        by_name.setdefault(rec['name'], []).append((aid, rec))

    # Fetch all data products
    r = requests.get(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, timeout=60)
    products = r.json().get('value', [])
    name_to_pid = {p['name']: p['id'] for p in products}
    zero = {p['name'] for p in products if p.get('additionalProperties', {}).get('assetCount', 0) == 0}

    total_linked = 0
    total_failed = 0
    for prod_name, asset_names in MAPPING.items():
        if prod_name not in name_to_pid:
            print(f'SKIP (not found): {prod_name}')
            continue
        if prod_name not in zero:
            print(f'SKIP (already has assets): {prod_name}')
            continue
        pid = name_to_pid[prod_name]
        print(f'\n=== {prod_name} ===')

        for an in asset_names:
            if an not in by_name:
                print(f'  [!] asset not in pool: {an}')
                continue
            aid, rec = by_name[an][0]  # first match
            payload = {
                'type': 'General',
                'name': rec['name'],
                'description': rec.get('description', ''),
                'source': rec['source'],
                'dataProductId': pid,
            }
            resp = requests.post(f'{UNIFIED}/dataAssets?api-version={VER}',
                                 headers=h, json=payload, timeout=60)
            if resp.status_code in (200, 201):
                print(f'  + linked {an}  ({rec["source"].get("assetType")})')
                total_linked += 1
            else:
                print(f'  FAIL {an}: {resp.status_code} {resp.text[:200]}')
                total_failed += 1

    print(f'\n=== DONE ===')
    print(f'Linked: {total_linked}')
    print(f'Failed: {total_failed}')


if __name__ == '__main__':
    main()
