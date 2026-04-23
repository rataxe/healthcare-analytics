#!/usr/bin/env python3
"""
Check and configure Purview permissions for scanning
"""
import requests
import json
from azure.identity import AzureCliCredential

# Configuration
PURVIEW_ACCOUNT = 'prviewacc'
SUBSCRIPTION_ID = '5b44c9f3-bbe7-464c-aa3e-562726a12004'
RESOURCE_GROUP = 'purview'

cred = AzureCliCredential(process_timeout=30)
token_purview = cred.get_token('https://purview.azure.net/.default').token
token_mgmt = cred.get_token('https://management.azure.com/.default').token

h_purview = {'Authorization': f'Bearer {token_purview}', 'Content-Type': 'application/json'}
h_mgmt = {'Authorization': f'Bearer {token_mgmt}', 'Content-Type': 'application/json'}

print("="*80)
print("  PURVIEW PERMISSIONS CHECK")
print("="*80)

# 1. Get current user
print("\n1. Getting current user...")
r = requests.get(
    'https://graph.microsoft.com/v1.0/me',
    headers={'Authorization': f'Bearer {cred.get_token("https://graph.microsoft.com/.default").token}'},
    timeout=30
)
if r.status_code == 200:
    user_data = r.json()
    user_id = user_data.get('id')
    user_email = user_data.get('userPrincipalName')
    print(f"   User: {user_email}")
    print(f"   Object ID: {user_id}")
else:
    print(f"   ❌ Could not get user info: {r.status_code}")

# 2. Check Purview collections
print("\n2. Checking Purview collections...")
r = requests.get(
    f'https://{PURVIEW_ACCOUNT}.purview.azure.com/account/collections?api-version=2019-11-01-preview',
    headers=h_purview,
    timeout=30
)
if r.status_code == 200:
    collections = r.json().get('value', [])
    print(f"   Found {len(collections)} collections")
    
    root_collection = None
    for col in collections:
        if col.get('parentCollection') is None:
            root_collection = col
            print(f"   Root collection: {col.get('friendlyName')} ({col.get('name')})")
            break
else:
    print(f"   ❌ Could not list collections: {r.status_code}")

# 3. Check role assignments
print("\n3. Checking your Purview roles...")
r = requests.get(
    f'https://{PURVIEW_ACCOUNT}.purview.azure.com/policystore/collections/{PURVIEW_ACCOUNT}/metadataRoles?api-version=2021-07-01',
    headers=h_purview,
    timeout=30
)
if r.status_code == 200:
    roles = r.json().get('value', [])
    print(f"   Current roles: {len(roles)}")
    for role in roles:
        print(f"     - {role.get('name', '?')}: {role.get('properties', {}).get('members', [])}")
else:
    print(f"   Status: {r.status_code}")

# 4. Check Azure RBAC on Purview account
print("\n4. Checking Azure RBAC on Purview account...")
r = requests.get(
    f'https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.Purview/accounts/{PURVIEW_ACCOUNT}/providers/Microsoft.Authorization/roleAssignments?api-version=2022-04-01',
    headers=h_mgmt,
    timeout=30
)
if r.status_code == 200:
    assignments = r.json().get('value', [])
    print(f"   Found {len(assignments)} role assignments")
    for assignment in assignments[:5]:
        role_id = assignment.get('properties', {}).get('roleDefinitionId', '').split('/')[-1]
        principal_id = assignment.get('properties', {}).get('principalId')
        print(f"     - Principal: {principal_id[:8]}... Role: {role_id[:8]}...")
else:
    print(f"   ❌ Could not list role assignments: {r.status_code}")

# 5. Recommendations
print("\n" + "="*80)
print("  RECOMMENDATIONS")
print("="*80)
print("""
To enable data source scanning, you need one of these roles:

Option 1 - Purview Collection Role (Recommended):
  Go to Purview Portal → Data Map → Collections → Root Collection
  Click "Role assignments" → Add "Data Source Administrator"
  Add your user account

Option 2 - Azure RBAC Role:
  Run this Azure CLI command:
  
  az role assignment create \\
    --role "Purview Data Source Administrator" \\
    --assignee <your-email> \\
    --scope "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Purview/accounts/{}"

Option 3 - Portal Manual Scanning:
  1. Go to: https://portal.azure.com/#view/Microsoft_Azure_Purview
  2. Open prviewacc → Data Map → Sources
  3. Click "+ Register" to add data sources
  4. Configure and run scans manually

After adding permissions, re-run:
  python scripts/scan_all_data_sources.py
""".format(SUBSCRIPTION_ID, RESOURCE_GROUP, PURVIEW_ACCOUNT))

print("="*80)
