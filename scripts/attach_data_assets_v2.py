"""Attach data assets to zero-asset products via relationships endpoint (correct API)."""
import requests
from azure.identity import AzureCliCredential

UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

MAPPING = {
    # Klinisk Vård
    'Akutflödesmonitorering': ['patients', 'encounters', 'diagnoses', 'HealthcareAnalyticsDB'],
    'Vårdplatskapacitet': ['encounters', 'patients', 'silver_patient', 'ml_predictions'],
    'Läkemedelsuppföljning Klinik': ['medications', 'patients', 'encounters', 'FHIR Patient'],
    'Kardiologisk Kvalitetsuppföljning': ['encounters', 'diagnoses', 'patients', 'DICOM MRI_Brain'],
    'Perioperativ Produktionsstyrning': ['encounters', 'patients', 'silver_patient', 'HealthcareAnalyticsDB'],
    # Interoperabilitet
    'FHIR Interoperabilitetslager': ['FHIR Patient', 'FHIR Observation', 'BrainChild FHIR Server (R4)'],
    'FHIR Interoperabilitetsnav': ['FHIR Patient', 'FHIR Observation', 'BrainChild FHIR Server (R4)'],
    'Masterdata Vårdhändelser': ['patients', 'silver_patient', 'FHIR Patient', 'encounters'],
    'Standardiserad Vårdepisodmodell': ['encounters', 'diagnoses', 'gold_omop', 'OMOP Forskningsdata'],
    'Terminologitjänst & Kodverk': ['diagnoses', 'medications', 'gold_omop'],
    'Terminologitjänst Kliniska Kodverk': ['diagnoses', 'medications', 'gold_omop'],
    # Forskning
    'Precisionsonkologi Variantlager': ['BrainChild DICOM Server', 'ml_features', 'silver_patient'],
    'Pediatrisk Imaging Research Hub': ['BrainChild DICOM Server', 'DICOM MRI_Brain',
                                        'brainchild_silver_dicom_studies', 'brainchild_silver_dicom_pathology'],
    'Biobank & Provspårbarhet': ['patients', 'silver_patient', 'diagnoses'],
    'Nationell Biobank Sammanställning': ['patients', 'silver_patient', 'diagnoses'],
    'Pediatrisk Precision Onkologi': ['brainchild_silver_dicom_pathology', 'brainchild_silver_dicom_studies',
                                      'silver_patient', 'ml_predictions'],
    'Radiogenomik Barnonkologi': ['DICOM MRI_Brain', 'brainchild_silver_dicom_studies', 'ml_features'],
    # Data & Analytics
    'Population Health Dashboard': ['healthcare1_msft_gold_omop', 'OneLake Catalog - Governance for Admins',
                                    'gold_omop', 'silver_patient'],
    'Operations Intelligence Mart': ['DW', 'encounters', 'ml_predictions', 'HealthcareAnalyticsDB'],
    'MLOps Modellregister': ['los-predictor-lgbm', 'healthcare-analytics-v1', 'ml_predictions'],
    'MLOps Modelltelemetri': ['healthcare-analytics-v1', 'los-predictor-lgbm', 'ml_predictions'],
    'Prediktiv Vårdplatskapacitet': ['ml_predictions', 'ml_features', 'los-predictor-lgbm'],
    'Population Health Segmentering': ['ml_features', 'silver_patient', 'gold_omop'],
    # Hälsosjukvård (governance)
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

    # Fetch all UC data assets (uc_id <-> name)
    r = requests.get(f'{UNIFIED}/dataAssets?api-version={VER}', headers=h, timeout=60)
    assets = r.json().get('value', [])
    name_to_ucid = {}
    for a in assets:
        if a.get('systemData', {}).get('provisioningState') == 'Succeeded':
            name_to_ucid.setdefault(a['name'], a['id'])
    print(f'Asset pool (UC ids): {len(name_to_ucid)} unique names')

    # Fetch all data products
    r = requests.get(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, timeout=60)
    products = r.json().get('value', [])
    name_to_pid = {p['name']: p['id'] for p in products}

    total_linked = 0
    total_skipped = 0
    total_failed = 0

    for prod_name, asset_names in MAPPING.items():
        if prod_name not in name_to_pid:
            print(f'SKIP (product not found): {prod_name}')
            continue
        pid = name_to_pid[prod_name]

        # Existing DATAASSET relationships
        rr = requests.get(f'{UNIFIED}/dataProducts/{pid}/relationships?api-version={VER}&entityType=DATAASSET',
                          headers=h, timeout=30)
        existing = {x['entityId'] for x in rr.json().get('value', [])} if rr.status_code == 200 else set()

        print(f'\n=== {prod_name} (existing DATAASSET links: {len(existing)}) ===')
        for an in asset_names:
            ucid = name_to_ucid.get(an)
            if not ucid:
                print(f'  [!] asset not in pool: {an}')
                continue
            if ucid in existing:
                print(f'  = already linked: {an}')
                total_skipped += 1
                continue
            body = {'entityId': ucid, 'relationshipType': 'Related'}
            resp = requests.post(
                f'{UNIFIED}/dataProducts/{pid}/relationships?api-version={VER}&entityType=DATAASSET',
                headers=h, json=body, timeout=30,
            )
            if resp.status_code in (200, 201):
                print(f'  + linked {an}')
                total_linked += 1
            else:
                print(f'  FAIL {an}: {resp.status_code} {resp.text[:200]}')
                total_failed += 1

    print('\n=== DONE ===')
    print(f'Linked:  {total_linked}')
    print(f'Skipped: {total_skipped}')
    print(f'Failed:  {total_failed}')


if __name__ == '__main__':
    main()
