#!/usr/bin/env python3
"""
VISA ALLA GLOSSARY TERMER
Listar alla 188 termer med detaljer
"""

import requests
import json
from azure.identity import AzureCliCredential
from collections import defaultdict

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
print("  PURVIEW GLOSSARY TERMER - Fullständig lista")
print("="*80)
print()

headers = get_auth_header()

# Hämta glossary
r = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=15)
if r.status_code != 200:
    print(f"❌ Kunde inte hämta glossary: {r.status_code}")
    exit(1)

data = r.json()
glossaries = data if isinstance(data, list) else [data]
g = glossaries[0]

print(f"📚 GLOSSARY: {g.get('name', '?')}")
print(f"   GUID: {g.get('guid', '?')}")
print()
print(f"   Portal UI: https://purview.microsoft.com/glossary/{g.get('guid', '?')}")
print()

# Hämta alla termer
g_guid = g['guid']
r_terms = requests.get(
    f"{ATLAS_API}/glossary/{g_guid}/terms?limit=500&offset=0",
    headers=headers, timeout=30
)

if r_terms.status_code != 200:
    print(f"❌ Kunde inte hämta termer: {r_terms.status_code}")
    exit(1)

all_terms = r_terms.json()

print(f"📊 TOTALT: {len(all_terms)} termer")
print()

# Gruppera termer per kategori
terms_by_category = defaultdict(list)

for term in all_terms:
    cats = term.get('categories', [])
    if cats:
        cat_name = cats[0].get('displayText', 'Okategoriserad')
    else:
        cat_name = 'Ingen kategori'
    
    terms_by_category[cat_name].append(term)

# Skriv ut per kategori
print("="*80)
print("  TERMER PER KATEGORI")
print("="*80)
print()

for cat_name in sorted(terms_by_category.keys()):
    terms = terms_by_category[cat_name]
    print(f"📁 {cat_name} ({len(terms)} termer)")
    print("-" * 80)
    
    for term in sorted(terms, key=lambda t: t.get('name', '')):
        name = term.get('name', '?')
        guid = term.get('guid', '?')
        short_desc = term.get('shortDescription', '')
        long_desc = term.get('longDescription', '')
        
        # Ta första meningen av beskrivningen
        desc = short_desc or long_desc or ''
        if desc:
            desc = desc.split('.')[0][:80] + '...' if len(desc) > 80 else desc
        
        print(f"   • {name}")
        if desc:
            print(f"     {desc}")
        print(f"     Portal: https://purview.microsoft.com/catalog/glossary/{guid}")
    
    print()

# Statistik
print("="*80)
print("  STATISTIK")
print("="*80)
print()

# Termer med beskrivningar
with_desc = sum(1 for t in all_terms if t.get('shortDescription') or t.get('longDescription'))
print(f"Termer med beskrivning: {with_desc}/{len(all_terms)} ({with_desc*100//len(all_terms)}%)")

# Termer med kategorier
with_cat = sum(1 for t in all_terms if t.get('categories'))
print(f"Termer med kategori: {with_cat}/{len(all_terms)} ({with_cat*100//len(all_terms)}%)")

# Termer med related terms
with_related = sum(1 for t in all_terms if t.get('seeAlso'))
print(f"Termer med relationer: {with_related}/{len(all_terms)}")

print()
print("="*80)
print("  HUR MAN SER TERMERNA I PURVIEW PORTAL UI")
print("="*80)
print()
print("1. Gå till: https://purview.microsoft.com")
print()
print("2. Klicka på 'Glossary' i vänstermenyn")
print()
print("3. Du ser nu alla glossary termer i en lista")
print()
print("4. Du kan:")
print("   • Söka efter termer i sökrutan")
print("   • Filtrera per kategori (dropdown 'All categories')")
print("   • Sortera på namn/status/ägare")
print("   • Klicka på en term för att se detaljer")
print()
print("5. För att se en specifik term, klicka på länken ovan")
print()
print("="*80)
print()

# Spara till fil
output = {
    "glossary_name": g.get('name'),
    "glossary_guid": g.get('guid'),
    "total_terms": len(all_terms),
    "terms_by_category": {
        cat: [
            {
                "name": t.get('name'),
                "guid": t.get('guid'),
                "description": t.get('shortDescription', t.get('longDescription', '')),
                "url": f"https://purview.microsoft.com/catalog/glossary/{t.get('guid')}"
            }
            for t in terms
        ]
        for cat, terms in terms_by_category.items()
    }
}

with open('glossary_terms_export.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print("💾 Exporterad till: glossary_terms_export.json")
print()
