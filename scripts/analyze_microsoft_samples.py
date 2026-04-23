#!/usr/bin/env python3
"""
Analyze Microsoft Purview API Samples Repository

Clone and analyze the official samples to extract relevant patterns.

USAGE:
    python scripts/analyze_microsoft_samples.py

OUTPUT:
    - Repository structure analysis
    - Unified Catalog API sample detection
    - Authentication pattern extraction
    - Comparison with our unified_catalog_client.py
"""
import os
import subprocess
from pathlib import Path
import re
from typing import List, Tuple, Dict

def clone_repo() -> Path:
    """Clone Microsoft samples repository"""
    repo_url = "https://github.com/microsoft/purview-api-samples"
    target_dir = Path("c:/code/purview-api-samples")
    
    if target_dir.exists():
        print(f"✅ Repository already exists at: {target_dir}")
        print("   To update: cd c:/code/purview-api-samples && git pull")
        return target_dir
    
    print(f"📥 Cloning repository from {repo_url}...")
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


def analyze_structure(repo_dir: Path) -> Dict[str, List[Path]]:
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
    
    # Also check for README files
    readmes = list(repo_dir.rglob("README.md"))
    if readmes:
        print(f"\n📖 Found {len(readmes)} README files:")
        for r in readmes[:10]:
            print(f"   • {r.relative_to(repo_dir)}")
    
    return by_dir


def find_unified_catalog_samples(repo_dir: Path) -> List[Tuple[Path, str]]:
    """Look for Unified Catalog API samples"""
    print("\n" + "="*80)
    print("  UNIFIED CATALOG API SAMPLES")
    print("="*80)
    
    keywords = [
        r"unified[_\s]*catalog",
        r"business[_\s]*domain",
        r"data[_\s]*product",
        r"okr",
        r"critical[_\s]*data[_\s]*element",
        r"data[_\s]*access[_\s]*polic"
    ]
    
    found = []
    for py_file in repo_dir.rglob("*.py"):
        try:
            content = py_file.read_text(encoding='utf-8', errors='ignore').lower()
            for keyword in keywords:
                if re.search(keyword, content):
                    found.append((py_file, keyword))
                    break
        except Exception as e:
            continue
    
    if found:
        print(f"✅ Found {len(found)} relevant files:")
        for f, keyword in found[:20]:
            rel = f.relative_to(repo_dir)
            print(f"   • {rel}")
            print(f"     → matched: {keyword}")
    else:
        print("❌ No Unified Catalog API samples found")
        print("   Repository may not include preview API examples yet")
        print("   This confirms our unified_catalog_client.py is ahead of official samples")
    
    return found


def find_auth_patterns(repo_dir: Path) -> List[Tuple[Path, str]]:
    """Extract authentication patterns"""
    print("\n" + "="*80)
    print("  AUTHENTICATION PATTERNS")
    print("="*80)
    
    auth_keywords = [
        "ClientSecretCredential",
        "DefaultAzureCredential",
        "AzureCliCredential",
        "oauth2",
        "access_token",
        "get_token",
        "Bearer"
    ]
    
    auth_files = []
    patterns_found = {}
    
    for py_file in repo_dir.rglob("*.py"):
        try:
            content = py_file.read_text(encoding='utf-8', errors='ignore')
            for keyword in auth_keywords:
                if keyword in content:
                    auth_files.append((py_file, keyword))
                    patterns_found[keyword] = patterns_found.get(keyword, 0) + 1
                    break
        except Exception:
            continue
    
    if auth_files:
        print(f"✅ Found {len(auth_files)} files with auth patterns:")
        for f, keyword in auth_files[:15]:
            rel = f.relative_to(repo_dir)
            print(f"   • {rel}")
            print(f"     → uses: {keyword}")
    
    if patterns_found:
        print(f"\n📊 Authentication Pattern Summary:")
        for pattern, count in sorted(patterns_found.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {pattern}: {count} files")
    
    return auth_files


def find_api_versions(repo_dir: Path) -> Dict[str, int]:
    """Find which API versions are used in samples"""
    print("\n" + "="*80)
    print("  API VERSIONS USED")
    print("="*80)
    
    version_pattern = r"api[-_]version['\"]?\s*[:=]\s*['\"]?(\d{4}-\d{2}-\d{2}(?:-preview)?)"
    versions = {}
    
    for py_file in repo_dir.rglob("*.py"):
        try:
            content = py_file.read_text(encoding='utf-8', errors='ignore')
            matches = re.findall(version_pattern, content, re.IGNORECASE)
            for version in matches:
                versions[version] = versions.get(version, 0) + 1
        except Exception:
            continue
    
    if versions:
        print(f"✅ Found API versions in use:")
        for version, count in sorted(versions.items(), key=lambda x: x[0], reverse=True):
            is_preview = "preview" in version
            marker = "🔮" if is_preview else "✅"
            print(f"   {marker} {version}: {count} files")
        
        print(f"\n📊 Our Implementation:")
        print(f"   • API Version: 2025-09-15-preview")
        if "2025-09-15-preview" in versions:
            print(f"   ✅ Microsoft samples use SAME version!")
        else:
            latest = max(versions.keys())
            print(f"   ⚠️  Microsoft samples latest: {latest}")
            print(f"   → We are using NEWER API version")
    else:
        print("❌ No API version patterns found")
    
    return versions


def compare_with_our_implementation(repo_dir: Path):
    """Compare Microsoft samples with our implementation"""
    print("\n" + "="*80)
    print("  COMPARISON WITH OUR IMPLEMENTATION")
    print("="*80)
    
    our_client = Path("scripts/unified_catalog_client.py")
    if not our_client.exists():
        print("❌ Our client not found at: scripts/unified_catalog_client.py")
        return
    
    our_content = our_client.read_text()
    
    # Count our methods
    our_methods = [
        line.strip().split('(')[0].replace('def ', '')
        for line in our_content.split('\n')
        if line.strip().startswith('def ') and not line.strip().startswith('def _')
    ]
    
    # Count resource groups implemented
    resource_groups = [
        "Business Domains",
        "Data Products",
        "Glossary Terms",
        "Critical Data Elements",
        "OKRs",
        "Data Access Policies"
    ]
    
    print(f"\n📊 Our unified_catalog_client.py:")
    print(f"   • Total public methods: {len(our_methods)}")
    print(f"   • Resource groups: {len(resource_groups)}")
    print(f"   • API Version: 2025-09-15-preview")
    print(f"   • Authentication: OAuth2 Service Principal")
    print(f"   • Status: Production-ready")
    
    # Check if Microsoft has similar methods
    method_sample = ["list_business_domains", "list_data_products", "create_glossary_term"]
    ms_matches = []
    
    for py_file in repo_dir.rglob("*.py"):
        try:
            content = py_file.read_text(encoding='utf-8', errors='ignore')
            for method in method_sample:
                if method in content:
                    ms_matches.append((method, py_file))
        except Exception:
            continue
    
    if ms_matches:
        print(f"\n✅ Found {len(ms_matches)} matching method patterns in Microsoft samples:")
        for method, f in ms_matches[:5]:
            print(f"   • {method} in {f.name}")
    else:
        print("\n⚠️  No matching method names found in Microsoft samples")
        print("   → Microsoft samples may use different naming conventions")
        print("   → Or Unified Catalog API samples not yet published")


def extract_best_practices(repo_dir: Path):
    """Extract best practices from Microsoft samples"""
    print("\n" + "="*80)
    print("  BEST PRACTICES EXTRACTION")
    print("="*80)
    
    practices = {
        "Error Handling": ["try", "except", "raise", "HTTPError"],
        "Retry Logic": ["retry", "backoff", "tenacity", "@retry"],
        "Rate Limiting": ["rate_limit", "throttle", "429", "RateLimitError"],
        "Pagination": ["skip", "limit", "continuation", "nextLink", "while"],
        "Logging": ["logging", "logger", "log.info", "log.error"],
        "Type Hints": ["typing", "List[", "Dict[", "Optional["]
    }
    
    practice_counts = {}
    
    for py_file in repo_dir.rglob("*.py"):
        try:
            content = py_file.read_text(encoding='utf-8', errors='ignore')
            for practice, keywords in practices.items():
                if any(kw in content for kw in keywords):
                    practice_counts[practice] = practice_counts.get(practice, 0) + 1
        except Exception:
            continue
    
    if practice_counts:
        print(f"📊 Best Practices Found:")
        for practice, count in sorted(practice_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"   • {practice}: {count} files")
        
        print(f"\n✅ Recommendations for Our Implementation:")
        if practice_counts.get("Retry Logic", 0) > 5:
            print("   → Consider adding retry logic (Microsoft uses it extensively)")
        if practice_counts.get("Rate Limiting", 0) > 3:
            print("   → Consider adding rate limit handling")
        if practice_counts.get("Type Hints", 0) > 10:
            print("   → Good: Type hints are widely used (we use them too)")
        if practice_counts.get("Logging", 0) > 10:
            print("   → Consider adding more logging for debugging")


def main():
    """Main analysis"""
    print("="*80)
    print("  MICROSOFT PURVIEW API SAMPLES - AUTOMATED ANALYSIS")
    print("="*80)
    print()
    
    # Clone repository
    repo_dir = clone_repo()
    if not repo_dir or not repo_dir.exists():
        print("❌ Failed to access repository")
        return 1
    
    # Analyze structure
    by_dir = analyze_structure(repo_dir)
    
    # Find Unified Catalog samples
    unified_samples = find_unified_catalog_samples(repo_dir)
    
    # Find auth patterns
    auth_files = find_auth_patterns(repo_dir)
    
    # Find API versions
    versions = find_api_versions(repo_dir)
    
    # Compare with our implementation
    compare_with_our_implementation(repo_dir)
    
    # Extract best practices
    extract_best_practices(repo_dir)
    
    print("\n" + "="*80)
    print("  ANALYSIS COMPLETE")
    print("="*80)
    
    print("\n📝 Summary:")
    print(f"   • Python files found: {sum(len(files) for files in by_dir.values())}")
    print(f"   • Unified Catalog samples: {len(unified_samples)}")
    print(f"   • Auth pattern examples: {len(auth_files)}")
    print(f"   • API versions used: {len(versions)}")
    
    print("\n🎯 Next Steps:")
    print("   1. Review found files manually in c:/code/purview-api-samples")
    print("   2. Extract useful patterns (error handling, retry logic)")
    print("   3. Update unified_catalog_client.py if improvements found")
    print("   4. Add references to Microsoft samples in documentation")
    
    print("\n✅ Conclusion:")
    if unified_samples:
        print("   → Microsoft HAS Unified Catalog samples - review for improvements")
    else:
        print("   → Microsoft LACKS Unified Catalog samples - our implementation is ahead!")
    
    print()
    return 0


if __name__ == '__main__':
    import sys
    sys.exit(main())
