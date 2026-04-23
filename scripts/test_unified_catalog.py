#!/usr/bin/env python3
"""
Test Purview Unified Catalog API (2025-09-15-preview)
Business Domains and Data Products
"""
import requests
import json
from azure.identity import AzureCliCredential

# Auth
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token('https://purview.azure.net/.default').token
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

# Test multiple endpoint formats
ENDPOINTS_TO_TEST = [
    'https://api.purview-service.microsoft.com',
    'https://prviewacc-api.purview-service.microsoft.com',
    'https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com',
    'https://prviewacc.purview.azure.com'
]

API_VERSION = '2025-09-15-preview'

print("=" * 80)
print("Testing Purview Unified Catalog API Endpoints")
print("=" * 80)

for endpoint in ENDPOINTS_TO_TEST:
    print(f"\n📍 Testing endpoint: {endpoint}")
    print("-" * 80)
    
    # Test 1: List Business Domains
    url_domains = f'{endpoint}/datagovernance/catalog/businessdomains?api-version={API_VERSION}'
    try:
        r1 = requests.get(url_domains, headers=headers, timeout=30)
        print(f'  GET /businessdomains: {r1.status_code}')
        if r1.status_code == 200:
            data = r1.json()
            print(f'    ✅ Success! Found {len(data.get("value", []))} business domains')
            if data.get('value'):
                for domain in data['value'][:3]:
                    print(f'      - {domain.get("name", "?")} (ID: {domain.get("id", "?")})')
        elif r1.status_code not in [404, 403]:
            print(f'    Response: {r1.text[:200]}')
    except Exception as e:
        print(f'    ❌ Error: {e}')
    
    # Test 2: List Data Products
    url_products = f'{endpoint}/datagovernance/catalog/dataProducts?api-version={API_VERSION}'
    try:
        r2 = requests.get(url_products, headers=headers, timeout=30)
        print(f'  GET /dataProducts: {r2.status_code}')
        if r2.status_code == 200:
            data = r2.json()
            print(f'    ✅ Success! Found {len(data.get("value", []))} data products')
            if data.get('value'):
                for dp in data['value'][:3]:
                    print(f'      - {dp.get("name", "?")} (ID: {dp.get("id", "?")})')
        elif r2.status_code not in [404, 403]:
            print(f'    Response: {r2.text[:200]}')
    except Exception as e:
        print(f'    ❌ Error: {e}')

print("\n" + "=" * 80)
print("Test complete")
print("=" * 80)
