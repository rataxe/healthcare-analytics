import requests, json
from azure.identity import AzureCliCredential as C

t = C().get_token('https://purview.azure.net/.default').token
h = {'Authorization': f'Bearer {t}', 'Content-Type': 'application/json'}
UNIFIED = 'https://prviewacc.purview.azure.com/datagovernance/catalog'
VER = '2025-09-15-preview'

# Get domains
domains_r = requests.get(f'{UNIFIED}/businessDomains?api-version={VER}', headers=h, timeout=30)
domains = {d['name']: d['id'] for d in domains_r.json().get('value', [])}

# Common audience values to try
test_audiences = [
    ['DataEngineer'],
    ['DataScientist'],
    ['DataAnalyst'],
    ['BIEngineer'],
    ['DataSteward'],
    ['SecurityOfficer'],
    ['OperationsManager'],  # This failed before
    ['Pharmacist'],  # This failed before
    ['DataEngineer', 'DataScientist'],
    [],
]

for aud in test_audiences:
    payload = {
        'name': f'Test Audience {aud}',
        'description': 'Testing audience enum',
        'status': 'Published',
        'type': 'Operational',
        'domain': list(domains.values())[0],
        'businessUse': 'Test',
        'contacts': {'owner': [{'id': '9350a243-7bcf-4053-8f7e-996364f4de24', 'description': 'Creator'}]},
        'termsOfUse': [],
        'documentation': [],
        'endorsed': True,
        'audience': aud,
    }
    
    r = requests.post(f'{UNIFIED}/dataProducts?api-version={VER}', headers=h, json=payload, timeout=30)
    if r.status_code == 201:
        print(f'✅ {aud}')
    else:
        err = r.json()
        if 'errors' in err:
            # Extract just the key error message
            for key, msgs in err['errors'].items():
                if 'audience' in key:
                    print(f'❌ {aud} — {msgs[0][:80]}')
                    break
        else:
            print(f'❌ {aud} — {r.status_code}')
