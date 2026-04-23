"""
FIX DOMAIN TERM NAMES
======================
Uppdaterar domain terms att ha fullständiga namn

Kör med: python scripts/fix_domain_term_names.py
"""

import requests
import json
from azure.identity import AzureCliCredential
from datetime import datetime

PURVIEW_ACCOUNT = "https://prviewacc.purview.azure.com"
ATLAS_API = f"{PURVIEW_ACCOUNT}/catalog/api/atlas/v2"

# Domain terms med deras GUIDs (från tidigare körning)
DOMAIN_TERMS = [
    {
        "guid": "0b2b84bf-bd52-48a3-b493-acb3cfa4e780",
        "name": "Domain: Clinical Data Management",
        "nickname": "CDM",
        "description": "Governance domain för kliniska patientdata, EHR, besök och diagnoser (OMOP CDM)"
    },
    {
        "guid": "d8836abb-0c7b-403a-990b-baed055e9c3c",
        "name": "Domain: Genomics & Precision Medicine",
        "nickname": "GPM",
        "description": "Governance domain för genomisk data, sekvenseringsdata och precision medicine (BTB, VCF)"
    },
    {
        "guid": "19001e56-0cbe-49ce-b1ff-9241b2aa6713",
        "name": "Domain: Cancer Registry",
        "nickname": "CR",
        "description": "Governance domain för cancer registry data (SBCR), behandlingar och uppföljning"
    },
    {
        "guid": "31971a13-da55-4def-b0ce-02769aeb00e8",
        "name": "Domain: ML & Analytics",
        "nickname": "MLA",
        "description": "Governance domain för ML feature stores, analytics och datamodeller"
    }
]

def get_headers():
    """Get authentication headers"""
    credential = AzureCliCredential(process_timeout=30)
    token = credential.get_token('https://purview.azure.net/.default')
    return {
        'Authorization': f'Bearer {token.token}',
        'Content-Type': 'application/json'
    }

def main():
    print("=" * 80)
    print("  FIX DOMAIN TERM NAMES")
    print("=" * 80 + "\n")
    
    headers = get_headers()
    
    for domain in DOMAIN_TERMS:
        guid = domain['guid']
        new_name = domain['name']
        nickname = domain['nickname']
        description = domain['description']
        
        print(f"🔧 Updating: {nickname} → {new_name}")
        
        try:
            # Get current term
            response = requests.get(
                f"{ATLAS_API}/glossary/term/{guid}",
                headers=headers,
                timeout=15
            )
            
            if response.status_code != 200:
                print(f"   ❌ Failed to get term: {response.status_code}")
                continue
            
            term = response.json()
            
            # Update name
            term['name'] = new_name
            term['nickName'] = nickname
            term['shortDescription'] = description
            term['longDescription'] = description
            
            # Update term
            update_response = requests.put(
                f"{ATLAS_API}/glossary/term/{guid}?includeTermHierarchy=true",
                headers=headers,
                json=term,
                timeout=30
            )
            
            if update_response.status_code == 200:
                updated = update_response.json()
                print(f"   ✅ Updated: {updated.get('name')}")
            else:
                print(f"   ❌ Update failed: {update_response.status_code}")
                print(f"      Response: {update_response.text[:300]}")
        
        except Exception as e:
            print(f"   ❌ Error: {e}")
        
        print()
    
    print("=" * 80)
    print("  DONE")
    print("=" * 80)

if __name__ == "__main__":
    main()
