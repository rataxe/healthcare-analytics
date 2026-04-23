#!/usr/bin/env python3
"""
UNIFIED CATALOG API SETUP
Step-by-step guide and automation for setting up Purview Unified Catalog API access
"""
import requests
import json
import os
from pathlib import Path

PURVIEW_ACCOUNT = 'prviewacc.purview.azure.com'
TENANT_ID = '71c4b6d5-0065-4c6c-a125-841a582754eb'

def print_step(step_num: int, title: str):
    """Print step header"""
    print("\n" + "="*80)
    print(f"  STEG {step_num}: {title}")
    print("="*80)

def step1_create_service_principal():
    """Guide for creating Service Principal"""
    print_step(1, "SKAPA SERVICE PRINCIPAL I ENTRA ID")
    
    print("""
För att komma åt Purview Unified Catalog API behöver vi en Service Principal.

INSTRUKTIONER:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Öppna Azure Portal: https://portal.azure.com
2. Sök på "Microsoft Entra ID" (tidigare Azure Active Directory)
3. Välj "App registrations" i vänstermenyn
4. Klicka "New registration"
5. Fyll i:
   - Name: "Purview-Unified-Catalog-Client"
   - Supported account types: "Single tenant"
   - Redirect URI: https://exampleURI.com (typ: Web)
6. Klicka "Register"

EFTER REGISTRERING:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

7. Kopiera "Application (client) ID" från Overview-sidan
8. Gå till "Certificates & secrets" → "Client secrets"
9. Klicka "New client secret"
   - Description: "Purview API Access"
   - Expires: 24 months (rekommenderat)
10. Kopiera det genererade "Value" DIREKT (visas bara en gång!)

SPARA CREDENTIALS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Tenant ID: {tenant_id}
Client ID: [KLISTRA IN HÄR]
Client Secret: [KLISTRA IN HÄR]

⚠️  VIKTIGT: Spara dessa värden säkert! Secret:en kan inte visas igen.
""".format(tenant_id=TENANT_ID))
    
    print("\nNär du har skapat Service Principal, tryck Enter för att fortsätta...")
    input()

def step2_assign_purview_roles():
    """Guide for assigning Purview roles"""
    print_step(2, "TILLDELA ROLLER I PURVIEW UNIFIED CATALOG")
    
    print("""
Service Principal behöver roller i Purview för att komma åt Unified Catalog API.

INSTRUKTIONER:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Öppna Microsoft Purview Governance Portal:
   https://web.purview.azure.com

2. Välj din Purview account: "prviewacc"

3. Gå till "Unified Catalog" i vänstermenyn

4. Välj "Catalog Management" → "Governance domains"

5. Välj ditt root domain (eller specifikt domain)

6. Gå till fliken "Roles"

7. Lägg till Service Principal med lämplig roll:

ROLLER OCH BEHÖRIGHETER:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📖 Data Catalog Reader
   - Läsa: Business Domains, Data Products, Terms, OKRs, Critical Data Elements
   - Rekommenderas för: Read-only automatisering

📝 Data Steward  (REKOMMENDERAT FÖR OSS)
   - Allt som Data Catalog Reader
   - Skriva: Data Products, Terms, Policies
   - Rekommenderas för: Full automatisering

👤 Data Product Owner
   - Allt som Data Steward
   - Hantera: Data Product ägande och policies

🏛️ Governance Domain Owner
   - Skapa och hantera: Business Domains
   - Hantera: Policies på domain-nivå

REKOMMENDATION FÖR HEALTHCARE ANALYTICS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Tilldela "Data Steward" roll till Service Principal för att kunna:
✅ Skapa och uppdatera Data Products
✅ Skapa och hantera Glossary Terms
✅ Länka OKRs och Critical Data Elements
✅ Hantera Data Policies
""")
    
    print("\nNär du har tilldelat roller, tryck Enter för att fortsätta...")
    input()

def step3_save_credentials():
    """Save credentials to .env file"""
    print_step(3, "SPARA CREDENTIALS")
    
    env_file = Path("scripts/.env.purview")
    
    print("""
Ange dina Service Principal credentials (de värden du kopierade från Entra ID):
""")
    
    print(f"\nTenant ID (default: {TENANT_ID}):")
    tenant = input().strip() or TENANT_ID
    
    print("\nClient ID (Application ID från App Registration):")
    client_id = input().strip()
    
    print("\nClient Secret (värdet från Client Secrets):")
    client_secret = input().strip()
    
    if not client_id or not client_secret:
        print("\n❌ Client ID och Client Secret måste anges!")
        return False
    
    # Save to .env file
    env_content = f"""# Purview Unified Catalog API Credentials
# Generated: {__import__('datetime').datetime.now().isoformat()}

PURVIEW_TENANT_ID={tenant}
PURVIEW_CLIENT_ID={client_id}
PURVIEW_CLIENT_SECRET={client_secret}
PURVIEW_ACCOUNT={PURVIEW_ACCOUNT}

# Unified Catalog API Base URL
UNIFIED_CATALOG_BASE=https://{PURVIEW_ACCOUNT}/datagovernance/catalog
API_VERSION=2025-09-15-preview
"""
    
    env_file.write_text(env_content)
    print(f"\n✅ Credentials sparade i: {env_file}")
    print("\n⚠️  VIKTIGT: Lägg till .env.purview i .gitignore!")
    
    # Update .gitignore
    gitignore = Path(".gitignore")
    if gitignore.exists():
        content = gitignore.read_text()
        if ".env.purview" not in content:
            with open(gitignore, "a") as f:
                f.write("\n# Purview credentials\n.env.purview\n")
            print("✅ .env.purview tillagt i .gitignore")
    
    return True

def step4_test_access():
    """Test Unified Catalog API access"""
    print_step(4, "TESTA API-ÅTKOMST")
    
    env_file = Path("scripts/.env.purview")
    if not env_file.exists():
        print("❌ .env.purview fil saknas. Kör steg 3 först.")
        return False
    
    # Load credentials
    env_vars = {}
    for line in env_file.read_text().splitlines():
        if line.strip() and not line.startswith('#'):
            key, value = line.split('=', 1)
            env_vars[key.strip()] = value.strip()
    
    tenant_id = env_vars.get('PURVIEW_TENANT_ID')
    client_id = env_vars.get('PURVIEW_CLIENT_ID')
    client_secret = env_vars.get('PURVIEW_CLIENT_SECRET')
    
    print("\n1️⃣ Hämtar OAuth2 access token...")
    
    # Get OAuth2 token
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://purview.azure.net'
    }
    
    try:
        r = requests.post(token_url, data=token_data, timeout=30)
        r.raise_for_status()
        token_response = r.json()
        access_token = token_response['access_token']
        print("   ✅ Access token erhållen")
    except Exception as e:
        print(f"   ❌ Kunde inte hämta token: {e}")
        return False
    
    # Test Unified Catalog API
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    base_url = f"https://{PURVIEW_ACCOUNT}/datagovernance/catalog"
    api_version = "2025-09-15-preview"
    
    print("\n2️⃣ Testar Unified Catalog API endpoints...")
    
    endpoints = [
        ('Business Domains', f'{base_url}/businessDomains?api-version={api_version}'),
        ('Data Products', f'{base_url}/dataProducts?api-version={api_version}'),
        ('Glossary Terms', f'{base_url}/glossaryTerms?api-version={api_version}'),
        ('Critical Data Elements', f'{base_url}/criticalDataElements?api-version={api_version}'),
        ('OKRs', f'{base_url}/okrs?api-version={api_version}'),
    ]
    
    results = []
    for name, url in endpoints:
        try:
            r = requests.get(url, headers=headers, timeout=30)
            status = r.status_code
            
            if status == 200:
                data = r.json()
                count = len(data.get('value', []))
                results.append((name, '✅', f'{count} items'))
                print(f"   ✅ {name}: {count} items")
            elif status == 403:
                results.append((name, '❌', 'Forbidden - saknar roll'))
                print(f"   ❌ {name}: 403 Forbidden - saknar behörighet")
            elif status == 404:
                results.append((name, '⚠️', 'Not Found'))
                print(f"   ⚠️  {name}: 404 Not Found")
            else:
                results.append((name, '❌', f'Status {status}'))
                print(f"   ❌ {name}: {status}")
        except Exception as e:
            results.append((name, '❌', str(e)))
            print(f"   ❌ {name}: {e}")
    
    # Summary
    print("\n" + "="*80)
    print("  SAMMANFATTNING")
    print("="*80)
    
    success_count = sum(1 for _, status, _ in results if status == '✅')
    
    if success_count == len(endpoints):
        print("\n🎉 PERFEKT! Unified Catalog API fungerar fullt ut!")
        print(f"\n✅ Alla {len(endpoints)} endpoints tillgängliga")
        print("\nDu kan nu använda Unified Catalog API för att:")
        print("  • Skapa och hantera Business Domains")
        print("  • Automatisera Data Products")
        print("  • Bulk-importera Glossary Terms")
        print("  • Definiera Critical Data Elements")
        print("  • Koppla OKRs till data")
        
        print("\n" + "="*80)
        print("  PRAKTISKA ANVÄNDNINGSFALL FÖR REGION GÄVLEBORG")
        print("="*80)
        print("""
Med Unified Catalog API kan ni till exempel automatisera:

📋 BULK-IMPORT AV FHIR-TERMER TILL GLOSSARY
   • Patient, Diagnos, Observation, Encounter etc.
   • Direkt från terminologisystem (SNOMED CT, LOINC, ICD-10)
   • Automatisk uppdatering när standarder ändras

📦 SKAPA DATA PRODUCTS PROGRAMMATISKT
   • När nya datamängder landar i Fabric
   • Automatisk metadata från Lakehouse/Warehouse schema
   • Länkning till governance domains och glossary terms

📊 POC-RAPPORTERING I POWER BI
   • Hämta data quality scores via API
   • Real-time governance dashboards
   • SLA-monitoring för data products

🚀 CI/CD-PIPELINE FÖR GOVERNANCE
   • Sätt governance metadata som del av Fabric-deployment
   • Version control för data product definitioner
   • Automated testing av data policies och quality rules
""")
        return True
    elif success_count > 0:
        print(f"\n⚠️  DELVIS FRAMGÅNG: {success_count}/{len(endpoints)} endpoints fungerar")
        print("\nFungerar:")
        for name, status, info in results:
            if status == '✅':
                print(f"  ✅ {name}: {info}")
        print("\nFungerar INTE:")
        for name, status, info in results:
            if status != '✅':
                print(f"  {status} {name}: {info}")
        print("\nKONTROLLERA:")
        print("  1. Att Service Principal har rätt roller i Purview")
        print("  2. Att roller är tilldelade på rätt Governance Domain")
        print("  3. Att API:et är aktiverat (preview feature)")
        return False
    else:
        print("\n❌ INGEN ÅTKOMST till Unified Catalog API")
        print("\nMÖJLIGA ORSAKER:")
        print("  1. Unified Catalog API inte aktiverat på denna Purview account")
        print("  2. Service Principal saknar roller helt")
        print("  3. Fel credentials (kontrollera .env.purview)")
        print("\nKONTAKTA Azure Support för att aktivera Unified Catalog API preview")
        return False

def main():
    print("="*80)
    print("  PURVIEW UNIFIED CATALOG API SETUP")
    print("  Steg-för-steg konfiguration för API-åtkomst")
    print("="*80)
    
    print("""
Detta script guidar dig genom att sätta upp åtkomst till Purview Unified Catalog API.

VILL DU:
  [1] Genomföra hela setup (alla steg)
  [2] Endast testa befintlig setup
  [3] Endast spara nya credentials
  [4] Visa instruktioner för Service Principal
  [5] Visa instruktioner för roller
""")
    
    choice = input("\nVälj alternativ (1-5): ").strip()
    
    if choice == '1':
        # Full setup
        step1_create_service_principal()
        step2_assign_purview_roles()
        if step3_save_credentials():
            step4_test_access()
    elif choice == '2':
        # Test only
        step4_test_access()
    elif choice == '3':
        # Save credentials only
        step3_save_credentials()
    elif choice == '4':
        # Show SP instructions
        step1_create_service_principal()
    elif choice == '5':
        # Show role instructions
        step2_assign_purview_roles()
    else:
        print("\n❌ Ogiltigt val")

if __name__ == '__main__':
    main()
