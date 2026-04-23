# Microsoft Purview API Samples - Analys

## Repository Information
**URL**: https://github.com/microsoft/purview-api-samples  
**Owner**: Microsoft  
**Purpose**: Officiella exempel för Purview REST APIs

---

## Repository Struktur (Förväntad)

Baserat på Microsofts standard repository-struktur för API samples förväntas följande:

### 1. **Atlas API v2 Samples**
```
/atlas-api/
  - entity-crud-operations.py
  - glossary-operations.py
  - typedef-operations.py
  - relationship-operations.py
  - search-operations.py
```

**Användbart för**:
- Legacy entity operations
- Glossary term management (Atlas API metod)
- Custom entity types
- Relationshipshantering

### 2. **Scanning API Samples**
```
/scanning-api/
  - register-data-source.py
  - create-scan.py
  - run-scan.py
  - scan-rules.py
```

**Användbart för**:
- Data source registration
- Automated scanning
- Scan rule konfiguration

### 3. **Data Map API Samples**
```
/data-map-api/
  - collections.py
  - metadata-policies.py
  - lineage.py
```

**Användbart för**:
- Collection management
- Metadata policies
- Lineage tracking

### 4. **Unified Catalog API Samples** (OM FINNS)
```
/unified-catalog-api/
  - business-domains.py
  - data-products.py
  - glossary-terms-unified.py
  - critical-data-elements.py
  - okrs.py
  - data-access-policies.py
```

**Status**: OSÄKERT - måste verifieras om denna mapp finns

---

## Värde för Vårt Projekt

### ✅ Direkt Användbart

1. **Authentication Patterns**
   - Microsofts rekommenderade auth-metoder
   - Service Principal setup examples
   - Token management best practices

2. **Error Handling**
   - Officiella error handling patterns
   - Retry logic examples
   - Rate limiting hantering

3. **Request Formatting**
   - Exakt JSON struktur för requests
   - Required vs optional fields
   - Proper header formatting

4. **Pagination**
   - Hur man hanterar stora resultat
   - Skip/limit patterns
   - Continuation tokens

### ⚠️ Potentiella Begränsningar

1. **API Version**
   - Samples kanske använder äldre API versions
   - Vår client använder 2025-09-15-preview (nyaste)

2. **Language**
   - Samples kan vara i olika språk (Python, C#, Java)
   - Behöver översätta patterns till Python om annat språk

3. **Unified Catalog Coverage**
   - Osäkert om Unified Catalog API samples finns
   - Repository kanske inte är uppdaterat med senaste API

---

## Jämförelse: Microsoft Samples vs Vår Implementation

| Aspekt | Microsoft Samples | Vår unified_catalog_client.py |
|--------|------------------|------------------------------|
| **API Coverage** | Troligen Atlas API v2 focus | Unified Catalog API (2025-09-15-preview) |
| **Language** | Multi-language | Python |
| **Authentication** | Flera metoder | OAuth2 Service Principal |
| **Completeness** | Snippets/examples | Full client library (55+ methods) |
| **Maintenance** | Microsoft | Oss |
| **Documentation** | Officiell | Vår egen |
| **Production Ready** | Examples only | Ja, full implementation |

---

## Rekommenderad Användning

### 🎯 Steg 1: Clone Repository
```bash
cd c:\code
git clone https://github.com/microsoft/purview-api-samples
cd purview-api-samples
```

### 🎯 Steg 2: Utforska Struktur
```bash
# Lista alla filer
dir /s /b *.py

# Eller i PowerShell
Get-ChildItem -Recurse -Filter *.py | Select-Object FullName
```

### 🎯 Steg 3: Analysera Relevanta Samples

**Prioritet 1: Authentication**
- Hur Microsoft rekommenderar Service Principal setup
- Token management patterns
- Error handling för auth failures

**Prioritet 2: Unified Catalog API** (om finns)
- Business Domains examples
- Data Products examples
- Jämför med vår implementation

**Prioritet 3: Error Handling**
- Retry logic
- Rate limiting
- HTTP error responses

**Prioritet 4: Request Patterns**
- JSON struktur
- Header formatting
- Query parameters

### 🎯 Steg 4: Integrera Learnings

Om vi hittar värdefulla patterns:
1. Uppdatera `unified_catalog_client.py` med best practices
2. Förbättra error handling baserat på Microsoft patterns
3. Lägg till retry logic om Microsoft rekommenderar det
4. Uppdatera dokumentation med referenser till officiella exempel

---

## Automated Analysis Script

```python
#!/usr/bin/env python3
"""
Analyze Microsoft Purview API Samples Repository

Clone and analyze the official samples to extract relevant patterns.

USAGE:
    python scripts/analyze_microsoft_samples.py
"""
import os
import subprocess
from pathlib import Path
import json

def clone_repo():
    """Clone Microsoft samples repository"""
    repo_url = "https://github.com/microsoft/purview-api-samples"
    target_dir = Path("c:/code/purview-api-samples")
    
    if target_dir.exists():
        print(f"✅ Repository already exists at: {target_dir}")
        return target_dir
    
    print(f"📥 Cloning repository...")
    result = subprocess.run(
        ["git", "clone", repo_url, str(target_dir)],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(f"✅ Cloned successfully to: {target_dir}")
        return target_dir
    else:
        print(f"❌ Clone failed: {result.stderr}")
        return None

def analyze_structure(repo_dir):
    """Analyze repository structure"""
    print("\n" + "="*80)
    print("  REPOSITORY STRUCTURE")
    print("="*80)
    
    # Find all Python files
    py_files = list(repo_dir.rglob("*.py"))
    print(f"\n📁 Found {len(py_files)} Python files")
    
    # Group by directory
    by_dir = {}
    for f in py_files:
        rel_path = f.relative_to(repo_dir)
        dir_name = rel_path.parts[0] if len(rel_path.parts) > 1 else "root"
        if dir_name not in by_dir:
            by_dir[dir_name] = []
        by_dir[dir_name].append(rel_path)
    
    for dir_name, files in sorted(by_dir.items()):
        print(f"\n📂 {dir_name}/ ({len(files)} files)")
        for f in sorted(files)[:5]:  # Show first 5
            print(f"   • {f}")
        if len(files) > 5:
            print(f"   ... and {len(files) - 5} more")
    
    return by_dir

def find_unified_catalog_samples(repo_dir):
    """Look for Unified Catalog API samples"""
    print("\n" + "="*80)
    print("  UNIFIED CATALOG API SAMPLES")
    print("="*80)
    
    keywords = [
        "unified", "catalog", "business.*domain", "data.*product",
        "okr", "critical.*data", "data.*access.*polic"
    ]
    
    found = []
    for py_file in repo_dir.rglob("*.py"):
        content = py_file.read_text(errors='ignore').lower()
        for keyword in keywords:
            if keyword in content:
                found.append((py_file, keyword))
                break
    
    if found:
        print(f"✅ Found {len(found)} relevant files:")
        for f, keyword in found:
            rel = f.relative_to(repo_dir)
            print(f"   • {rel} (matched: {keyword})")
    else:
        print("❌ No Unified Catalog API samples found")
        print("   Repository may not include preview API examples yet")
    
    return found

def find_auth_patterns(repo_dir):
    """Extract authentication patterns"""
    print("\n" + "="*80)
    print("  AUTHENTICATION PATTERNS")
    print("="*80)
    
    auth_keywords = [
        "ClientSecretCredential",
        "DefaultAzureCredential",
        "AzureCliCredential",
        "oauth2",
        "access_token"
    ]
    
    auth_files = []
    for py_file in repo_dir.rglob("*.py"):
        content = py_file.read_text(errors='ignore')
        for keyword in auth_keywords:
            if keyword in content:
                auth_files.append((py_file, keyword))
                break
    
    if auth_files:
        print(f"✅ Found {len(auth_files)} files with auth patterns:")
        for f, keyword in auth_files[:10]:
            rel = f.relative_to(repo_dir)
            print(f"   • {rel} (uses: {keyword})")
    
    return auth_files

def compare_with_our_implementation(repo_dir):
    """Compare Microsoft samples with our implementation"""
    print("\n" + "="*80)
    print("  COMPARISON WITH OUR IMPLEMENTATION")
    print("="*80)
    
    our_client = Path("scripts/unified_catalog_client.py")
    if not our_client.exists():
        print("❌ Our client not found")
        return
    
    our_content = our_client.read_text()
    our_methods = [
        line.strip().split('(')[0].replace('def ', '')
        for line in our_content.split('\n')
        if line.strip().startswith('def ') and not line.strip().startswith('def _')
    ]
    
    print(f"\n📊 Our Implementation:")
    print(f"   • Total methods: {len(our_methods)}")
    print(f"   • API Version: 2025-09-15-preview")
    print(f"   • Authentication: OAuth2 Service Principal")
    
    # Check if Microsoft has similar methods
    ms_methods = []
    for py_file in repo_dir.rglob("*.py"):
        content = py_file.read_text(errors='ignore')
        for method in our_methods[:10]:  # Check first 10
            if method in content:
                ms_methods.append((method, py_file))
    
    if ms_methods:
        print(f"\n✅ Found {len(ms_methods)} matching method names in Microsoft samples:")
        for method, f in ms_methods[:5]:
            print(f"   • {method} in {f.name}")
    else:
        print("\n⚠️  No matching method names found")
        print("   Microsoft samples may use different naming conventions")

def main():
    """Main analysis"""
    print("="*80)
    print("  MICROSOFT PURVIEW API SAMPLES - AUTOMATED ANALYSIS")
    print("="*80)
    
    # Clone repository
    repo_dir = clone_repo()
    if not repo_dir:
        return
    
    # Analyze structure
    analyze_structure(repo_dir)
    
    # Find Unified Catalog samples
    find_unified_catalog_samples(repo_dir)
    
    # Find auth patterns
    find_auth_patterns(repo_dir)
    
    # Compare with our implementation
    compare_with_our_implementation(repo_dir)
    
    print("\n" + "="*80)
    print("  ANALYSIS COMPLETE")
    print("="*80)
    print("\nNext Steps:")
    print("1. Review found files manually")
    print("2. Extract useful patterns")
    print("3. Update unified_catalog_client.py if needed")
    print("4. Add references to Microsoft samples in documentation")

if __name__ == '__main__':
    main()
```

---

## Förväntade Resultat

### Scenario A: Unified Catalog Samples Finns ✅
- **Action**: Jämför med vår implementation
- **Update**: Lägg till Microsoft references i dokumentation
- **Benefit**: Validering att vår approach är korrekt

### Scenario B: Bara Atlas API Samples ❌
- **Action**: Extrahera auth och error handling patterns
- **Update**: Förbättra vår client med best practices
- **Benefit**: Bättre production quality

### Scenario C: Repository Verkar Tom/Gammal ⚠️
- **Action**: Notera att Microsoft samples inte täcker nya APIs
- **Update**: Vår implementation är ahead of official samples
- **Benefit**: Bekräftar att vi är på rätt väg

---

## Slutsats

Microsoft's samples repository är värdefull för:
1. ✅ **Authentication patterns** - hur Microsoft rekommenderar auth
2. ✅ **Error handling** - officiella patterns för error cases
3. ✅ **Request formatting** - exakt JSON struktur
4. ⚠️ **Unified Catalog API** - osäkert om samples finns än

**REKOMMENDATION**:
1. Clone repository och kör analysis script
2. Extrahera användbara patterns
3. Uppdatera vår client om vi hittar förbättringar
4. **BEHÅLL vår implementation** - den täcker Unified Catalog API som Microsoft samples troligen inte gör än

**Vi är fortfarande ahead of the curve! 🚀**
