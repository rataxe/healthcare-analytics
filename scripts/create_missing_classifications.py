"""
Create Missing Custom Classifications in Purview
=================================================
Skapar 3 saknade custom classifications:
1. ICD-10 Code — Diagnoskoder
2. ATC Code — Läkemedelskoder
3. LOINC Code — Laboratoriekoder

Usage:
    python scripts/create_missing_classifications.py
    python scripts/create_missing_classifications.py --dry-run
"""
import argparse
import requests
from azure.identity import AzureCliCredential

PURVIEW_ACCOUNT = "prviewacc"

# Classification definitions
CLASSIFICATIONS = [
    {
        "name": "ICD10Code",
        "description": "International Classification of Diseases, 10th Revision (ICD-10) diagnostic codes. Used to identify medical diagnoses and procedures in healthcare data.",
        "pattern": r"^[A-Z]\d{2}(\.\d{1,3})?$",
        "examples": ["A00.0", "I10", "E11.9", "C50.9"]
    },
    {
        "name": "ATCCode", 
        "description": "Anatomical Therapeutic Chemical (ATC) classification system for medications. Used to identify pharmaceutical products and active substances.",
        "pattern": r"^[A-Z]\d{2}[A-Z]{2}\d{2}$",
        "examples": ["N02BE01", "J01CA04", "C09AA01", "A10BA02"]
    },
    {
        "name": "LOINCCode",
        "description": "Logical Observation Identifiers Names and Codes (LOINC) for laboratory tests and clinical observations. Universal standard for lab result identification.",
        "pattern": r"^\d{4,5}-\d$",
        "examples": ["2339-0", "718-7", "14682-9", "33762-6"]
    }
]


def create_classification(name, description, pattern, examples, dry_run=False):
    """Create a custom classification in Purview."""
    
    if dry_run:
        print(f"\n  [DRY RUN] Would create classification:")
        print(f"    Name: {name}")
        print(f"    Pattern: {pattern}")
        print(f"    Examples: {', '.join(examples)}")
        return True
    
    try:
        cred = AzureCliCredential(process_timeout=30)
        token = cred.get_token("https://purview.azure.net/.default").token
        h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        
        ATLAS = f"https://{PURVIEW_ACCOUNT}.purview.azure.com/catalog/api/atlas/v2"
        
        # Check if classification already exists
        r_check = requests.get(f"{ATLAS}/types/typedef/name/{name}", headers=h, timeout=15)
        if r_check.status_code == 200:
            print(f"  ✅ {name} (already exists)")
            return True
        
        # Create classification typedef
        typedef_body = {
            "classificationDefs": [
                {
                    "category": "CLASSIFICATION",
                    "name": name,
                    "description": description,
                    "typeVersion": "1.0",
                    "attributeDefs": [
                        {
                            "name": "pattern",
                            "typeName": "string",
                            "isOptional": False,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": False,
                            "isIndexable": True
                        },
                        {
                            "name": "examples",
                            "typeName": "string",
                            "isOptional": True,
                            "cardinality": "SINGLE",
                            "valuesMinCount": 0,
                            "valuesMaxCount": 1,
                            "isUnique": False,
                            "isIndexable": False
                        }
                    ]
                }
            ]
        }
        
        r_create = requests.post(f"{ATLAS}/types/typedefs", headers=h, json=typedef_body, timeout=30)
        
        if r_create.status_code in (200, 201):
            print(f"  ✅ {name} (created successfully)")
            return True
        elif r_create.status_code == 409:
            print(f"  ✅ {name} (already exists)")
            return True
        else:
            print(f"  ⚠️  {name}: HTTP {r_create.status_code}")
            print(f"     Response: {r_create.text[:200]}")
            return False
            
    except Exception as e:
        print(f"  ❌ {name}: {str(e)[:100]}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Create missing custom classifications in Purview")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be created without making changes")
    args = parser.parse_args()
    
    print("=" * 70)
    print("  CREATE MISSING CUSTOM CLASSIFICATIONS")
    print("=" * 70)
    
    if args.dry_run:
        print("\n🔍 DRY RUN MODE — No changes will be made\n")
    
    print("\nCreating 3 missing classifications:\n")
    
    success_count = 0
    for cls in CLASSIFICATIONS:
        if create_classification(
            cls["name"],
            cls["description"],
            cls["pattern"],
            cls["examples"],
            args.dry_run
        ):
            success_count += 1
    
    print("\n" + "=" * 70)
    print(f"  SUMMARY: {success_count}/{len(CLASSIFICATIONS)} classifications ready")
    print("=" * 70)
    
    if args.dry_run:
        print("\n🔍 Dry run complete — run without --dry-run to create classifications")
    else:
        print("\n✅ Classification creation complete!")
        print("\nNext steps:")
        print("  1. Verify in portal: https://web.purview.azure.com/resource/prviewacc")
        print("  2. Navigate to: Data Catalog → Classifications")
        print("  3. Run verification: python scripts/verify_all_purview.py")


if __name__ == "__main__":
    main()
