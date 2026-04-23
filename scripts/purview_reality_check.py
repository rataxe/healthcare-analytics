#!/usr/bin/env python3
"""
PURVIEW REALITY CHECK
Komplett diagnostik av vad som FAKTISKT finns i Purview
Jämför API-state mot Portal UI-state
"""

import requests
import json
from azure.identity import AzureCliCredential
from typing import Dict, List

PURVIEW_ACCOUNT = "prviewacc.purview.azure.com"
BASE_URL = f"https://{PURVIEW_ACCOUNT}"
ATLAS_API = f"{BASE_URL}/catalog/api/atlas/v2"

def get_auth_header():
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token("https://purview.azure.net/.default").token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

print("="*80)
print("  PURVIEW REALITY CHECK - Vad finns VERKLIGEN i Purview?")
print("="*80)
print()

headers = get_auth_header()

# ============================================================================
# 1. GLOSSARY - Finns glossaryn?
# ============================================================================
print("📚 1. GLOSSARY")
print("-" * 80)

try:
    r = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=15)
    if r.status_code == 200:
        data = r.json()
        glossaries = data if isinstance(data, list) else [data]
        
        if glossaries:
            g = glossaries[0]
            print(f"✅ Glossary finns: {g.get('name', '?')}")
            print(f"   GUID: {g.get('guid', '?')}")
            
            # Hämta terms
            g_guid = g['guid']
            r_terms = requests.get(
                f"{ATLAS_API}/glossary/{g_guid}/terms?limit=200&offset=0",
                headers=headers, timeout=15
            )
            
            if r_terms.status_code == 200:
                all_terms = r_terms.json()
                print(f"   Totalt antal termer: {len(all_terms)}")
                
                # Kategorisera termer
                dp_terms = [t for t in all_terms if t.get('name', '').startswith('DP:')]
                domain_terms = [t for t in all_terms if 'Domain:' in t.get('name', '')]
                regular_terms = [t for t in all_terms if not t.get('name', '').startswith('DP:') and 'Domain:' not in t.get('name', '')]
                
                print(f"   - Data Product termer (DP:): {len(dp_terms)}")
                print(f"   - Domain workaround termer: {len(domain_terms)}")
                print(f"   - Vanliga termer: {len(regular_terms)}")
                
                if dp_terms:
                    print(f"\n   Data Product termer:")
                    for t in dp_terms:
                        print(f"      • {t.get('name', '?')}")
                
                if domain_terms:
                    print(f"\n   Domain workaround termer:")
                    for t in domain_terms:
                        print(f"      • {t.get('name', '?')}")
            
            # Hämta categories
            r_cat = requests.get(f"{ATLAS_API}/glossary/{g_guid}", headers=headers, timeout=15)
            if r_cat.status_code == 200:
                cats = r_cat.json().get('categories', [])
                print(f"\n   Kategorier: {len(cats)}")
                for c in cats:
                    print(f"      • {c.get('displayText', '?')}")
        else:
            print("❌ Ingen glossary hittades")
    else:
        print(f"❌ Kunde inte hämta glossary: {r.status_code}")
except Exception as e:
    print(f"❌ EXCEPTION: {e}")

print()

# ============================================================================
# 2. DATA PRODUCTS - Finns de som entities?
# ============================================================================
print("📦 2. DATA PRODUCTS (Entities)")
print("-" * 80)

try:
    # Sök efter healthcare_data_product entities
    search_body = {
        "keywords": "*",
        "limit": 50,
        "filter": {
            "entityType": "healthcare_data_product"
        }
    }
    
    r = requests.post(
        f"{BASE_URL}/catalog/api/search/query?api-version=2022-08-01-preview",
        headers=headers, json=search_body, timeout=15
    )
    
    if r.status_code == 200:
        results = r.json().get('value', [])
        print(f"✅ Hittade {len(results)} data product entities")
        
        for dp in results:
            name = dp.get('name', '?')
            guid = dp.get('id', '?')
            coll = dp.get('collectionId', '?')
            print(f"\n   • {name}")
            print(f"     GUID: {guid}")
            print(f"     Collection: {coll}")
    else:
        print(f"❌ Search misslyckades: {r.status_code}")
        print(f"   Response: {r.text[:200]}")
except Exception as e:
    print(f"❌ EXCEPTION: {e}")

print()

# ============================================================================
# 3. GOVERNANCE DOMAINS - Finns native domains?
# ============================================================================
print("🏛️ 3. GOVERNANCE DOMAINS (Native)")
print("-" * 80)

try:
    # Sök efter Purview_DataDomain entities
    search_body = {
        "keywords": "*",
        "limit": 50,
        "filter": {
            "entityType": "Purview_DataDomain"
        }
    }
    
    r = requests.post(
        f"{BASE_URL}/catalog/api/search/query?api-version=2022-08-01-preview",
        headers=headers, json=search_body, timeout=15
    )
    
    if r.status_code == 200:
        results = r.json().get('value', [])
        if results:
            print(f"✅ Hittade {len(results)} governance domains")
            for dom in results:
                print(f"   • {dom.get('name', '?')}")
        else:
            print(f"❌ Inga governance domains finns (måste skapas manuellt i Portal UI)")
    else:
        print(f"❌ Search misslyckades: {r.status_code}")
except Exception as e:
    print(f"❌ EXCEPTION: {e}")

print()

# ============================================================================
# 4. CLASSIFICATIONS - Finns custom classifications?
# ============================================================================
print("🏷️ 4. CLASSIFICATIONS")
print("-" * 80)

try:
    r = requests.get(f"{ATLAS_API}/types/typedefs?type=classification", headers=headers, timeout=15)
    
    if r.status_code == 200:
        data = r.json()
        classifications = data.get('classificationDefs', [])
        
        # Filtrera ut system classifications
        custom = [c for c in classifications if not c.get('name', '').startswith('MICROSOFT.')]
        system = [c for c in classifications if c.get('name', '').startswith('MICROSOFT.')]
        
        print(f"✅ Totalt: {len(classifications)} classifications")
        print(f"   - Custom: {len(custom)}")
        print(f"   - System: {len(system)}")
        
        if custom:
            print(f"\n   Custom classifications:")
            for c in custom[:10]:  # Visa första 10
                print(f"      • {c.get('name', '?')}")
            if len(custom) > 10:
                print(f"      ... och {len(custom) - 10} till")
    else:
        print(f"❌ Kunde inte hämta classifications: {r.status_code}")
except Exception as e:
    print(f"❌ EXCEPTION: {e}")

print()

# ============================================================================
# 5. COLLECTIONS - Vilka collections finns?
# ============================================================================
print("📁 5. COLLECTIONS")
print("-" * 80)

try:
    r = requests.get(f"{BASE_URL}/account/collections?api-version=2019-11-01-preview", headers=headers, timeout=15)
    
    if r.status_code == 200:
        colls = r.json().get('value', [])
        print(f"✅ Hittade {len(colls)} collections")
        for c in colls:
            name = c.get('friendlyName', c.get('name', '?'))
            coll_id = c.get('name', '?')
            print(f"   • {name} ({coll_id})")
    else:
        print(f"❌ Kunde inte hämta collections: {r.status_code}")
except Exception as e:
    print(f"❌ EXCEPTION: {e}")

print()

# ============================================================================
# 6. DATA SOURCES - Finns några data sources registrerade?
# ============================================================================
print("🔌 6. DATA SOURCES")
print("-" * 80)

try:
    r = requests.get(
        f"{BASE_URL}/scan/datasources?api-version=2022-07-01-preview",
        headers=headers, timeout=15
    )
    
    if r.status_code == 200:
        sources = r.json().get('value', [])
        print(f"✅ Hittade {len(sources)} data sources")
        for s in sources:
            name = s.get('name', '?')
            kind = s.get('kind', '?')
            print(f"   • {name} (Type: {kind})")
    else:
        print(f"❌ Kunde inte hämta data sources: {r.status_code}")
        print(f"   Response: {r.text[:200]}")
except Exception as e:
    print(f"❌ EXCEPTION: {e}")

print()

# ============================================================================
# SAMMANFATTNING
# ============================================================================
print("="*80)
print("  SAMMANFATTNING - Vad fungerar och vad fungerar INTE")
print("="*80)
print()

print("✅ FUNGERAR (API-nivå):")
print("   • Autentisering och API-access")
print("   • Atlas API v2 endpoints")
print()

print("⚠️ OKÄND STATUS (måste verifieras i Portal UI):")
print("   • Glossary termer - API säger X antal, men syns de i portalen?")
print("   • Data product entities - finns de verkligen?")
print("   • Classifications - är de applicerade på assets?")
print("   • Collections - används de?")
print()

print("❌ FUNGERAR INTE:")
print("   • Governance Domains - MÅSTE skapas manuellt i Portal UI")
print("   • Governance Domains REST API - finns inte (404/403)")
print("   • SQL Lineage configuration - Azure CLI integration misslyckades")
print()

print("📋 NÄSTA STEG:")
print("   1. GÅ TILL PURVIEW PORTAL UI: https://purview.microsoft.com")
print("   2. Verifiera att glossary termer SYNS i UI")
print("   3. Verifiera att data products SYNS i Data Catalog")
print("   4. Skapa governance domains MANUELLT (API fungerar inte)")
print("   5. Kör SQL-kommandon MANUELLT för lineage (se SQL_LINEAGE_SETUP.md)")
print()

print("="*80)
