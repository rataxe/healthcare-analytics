"""
Fix Purview Glossary Issues
============================
Fokuserar på glossary-relaterade fixes utan SQL/Key Vault dependencies.
"""
import sys
import requests
from azure.identity import AzureCliCredential

PURVIEW_ACCOUNT = "prviewacc"
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
ATLAS_API = f"{PURVIEW_ENDPOINT}/catalog/api/atlas/v2"

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def section(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print("=" * 70)

def get_glossary():
    """Get main glossary."""
    r = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=30)
    if r.status_code != 200:
        print(f"ERROR: Cannot get glossary ({r.status_code})")
        sys.exit(1)
    
    data = r.json()
    glist = data if isinstance(data, list) else [data]
    return glist[0]

def get_all_categories(glossary_guid):
    """Get all categories in glossary."""
    r = requests.get(f"{ATLAS_API}/glossary/{glossary_guid}", headers=headers, timeout=30)
    if r.status_code == 200:
        return r.json().get("categories", [])
    return []

def get_all_terms(glossary_guid):
    """Get all terms in glossary."""
    all_terms = []
    offset = 0
    limit = 100
    
    while True:
        r = requests.get(f"{ATLAS_API}/glossary/{glossary_guid}/terms?limit={limit}&offset={offset}",
                        headers=headers, timeout=30)
        if r.status_code != 200:
            break
        
        terms = r.json()
        if not terms:
            break
        
        all_terms.extend(terms)
        if len(terms) < limit:
            break
        offset += limit
    
    return all_terms

def get_category_terms(category_guid):
    """Get terms in a specific category."""
    r = requests.get(f"{ATLAS_API}/glossary/category/{category_guid}/terms",
                    headers=headers, timeout=30)
    if r.status_code == 200:
        return r.json()
    return []

def assign_term_to_category(term_guid, category_guid):
    """Assign a term to a category."""
    # Get full term first
    r = requests.get(f"{ATLAS_API}/glossary/term/{term_guid}", headers=headers, timeout=30)
    if r.status_code != 200:
        return False
    
    term = r.json()
    
    # Add category reference
    if "categories" not in term:
        term["categories"] = []
    
    # Check if already assigned
    if any(c.get("categoryGuid") == category_guid for c in term["categories"]):
        return True  # Already assigned
    
    term["categories"].append({
        "categoryGuid": category_guid,
        "relationGuid": None
    })
    
    # Update term
    r = requests.put(f"{ATLAS_API}/glossary/term/{term_guid}", 
                    headers=headers, json=term, timeout=30)
    return r.status_code == 200

# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

section("1. GET GLOSSARY INFO")
glossary = get_glossary()
g_guid = glossary["guid"]
g_name = glossary.get("name", "?")
print(f"  Glossary: {g_name}")
print(f"  GUID: {g_guid}")

section("2. GET ALL CATEGORIES")
categories = get_all_categories(g_guid)
print(f"  Total categories: {len(categories)}")

cat_map = {}
for cat in categories:
    cat_name = cat.get("displayText", "?")
    cat_guid = cat.get("categoryGuid", "?")
    cat_map[cat_name] = cat_guid
    print(f"  - {cat_name} ({cat_guid})")

section("3. GET ALL TERMS")
all_terms = get_all_terms(g_guid)
print(f"  Total terms: {len(all_terms)}")

# Group by category
terms_by_category = {}
terms_no_category = []

for term in all_terms:
    t_name = term.get("name", "?")
    t_guid = term.get("guid", "?")
    t_cats = term.get("categories", [])
    
    if not t_cats:
        terms_no_category.append((t_name, t_guid))
    else:
        for cat_ref in t_cats:
            cat_guid = cat_ref.get("categoryGuid")
            if cat_guid:
                if cat_guid not in terms_by_category:
                    terms_by_category[cat_guid] = []
                terms_by_category[cat_guid].append((t_name, t_guid))

section("4. TERMS PER CATEGORY")
for cat_name, cat_guid in cat_map.items():
    term_count = len(terms_by_category.get(cat_guid, []))
    print(f"  {cat_name}: {term_count} terms")
    
    if term_count == 0:
        print(f"    WARNING: Category '{cat_name}' is EMPTY!")

section("5. TERMS WITHOUT CATEGORY")
print(f"  Terms without category: {len(terms_no_category)}")
if terms_no_category:
    print("  First 10:")
    for name, guid in terms_no_category[:10]:
        print(f"    - {name}")

# ══════════════════════════════════════════════════════════
# CATEGORY ASSIGNMENT RULES
# ══════════════════════════════════════════════════════════

section("6. FIX CATEGORY ASSIGNMENTS")

# Define term -> category mapping based on term names
category_rules = {
    "Kliniska Standarder": ["SNOMED", "ICD", "LOINC", "ATC", "HL7", "FHIR"],
    "Interoperabilitet": ["HL7", "FHIR", "DICOM", "REST", "API", "Integration"],
    "Dataarkitektur": ["Lakehouse", "Delta", "Bronze", "Silver", "Gold", "ETL", "Pipeline"],
    "Kliniska Data": ["Patient", "Encounter", "Observation", "Medication", "Condition", "Vital"],
    "Barncancerforskning": ["Cancer", "Oncology", "Tumor", "Genomics", "Mutation", "Clinical Trial"],
    "Sjukvårdstermer": []  # Will be filled with Swedish healthcare terms
}

# Additional Swedish terms for Sjukvårdstermer
swedish_keywords = ["Vård", "Läkare", "Sjukhus", "Behandling", "Diagnos", "Receptbelagd"]

fixes_applied = 0
fixes_needed = []

for term_name, term_guid in terms_no_category:
    assigned = False
    
    # Try to match with category rules
    for cat_name, keywords in category_rules.items():
        if cat_name not in cat_map:
            continue
        
        cat_guid = cat_map[cat_name]
        
        # Check if term name contains any keyword
        for keyword in keywords:
            if keyword.lower() in term_name.lower():
                fixes_needed.append((term_name, term_guid, cat_name, cat_guid))
                assigned = True
                break
        
        if assigned:
            break
    
    # Check Swedish keywords for Sjukvårdstermer
    if not assigned and "Sjukvårdstermer" in cat_map:
        for keyword in swedish_keywords:
            if keyword.lower() in term_name.lower():
                cat_guid = cat_map["Sjukvårdstermer"]
                fixes_needed.append((term_name, term_guid, "Sjukvårdstermer", cat_guid))
                break

print(f"\n  Terms that need category assignment: {len(fixes_needed)}")

if fixes_needed:
    print("\n  Applying fixes...")
    for term_name, term_guid, cat_name, cat_guid in fixes_needed:
        print(f"    Assigning '{term_name}' -> '{cat_name}'...", end=" ")
        if assign_term_to_category(term_guid, cat_guid):
            print("OK")
            fixes_applied += 1
        else:
            print("FAILED")

section("7. SUMMARY")
print(f"  Total terms: {len(all_terms)}")
print(f"  Terms without category: {len(terms_no_category)}")
print(f"  Fixes applied: {fixes_applied}")
print(f"\n  DONE!")
