#!/usr/bin/env python3
"""
Test OneLake Connection

Tests connectivity to Fabric OneLake using Azure CLI credentials.
Helps diagnose "Connection failed" errors in Purview.

USAGE:
    python scripts/test_onelake_connection.py
"""
import requests
import sys
from azure.identity import AzureCliCredential


def test_onelake_connection():
    """Test OneLake connectivity"""
    
    print("="*80)
    print("  ONELAKE CONNECTION TEST")
    print("="*80)
    print()
    
    # Configuration
    workspace_id = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
    lakehouse_gold_id = "2960eef0-5de6-4117-80b1-6ee783cdaeec"
    
    print("🔧 Configuration:")
    print(f"   Workspace ID: {workspace_id}")
    print(f"   Lakehouse ID: {lakehouse_gold_id}")
    print()
    
    # Get credentials
    print("🔐 Getting Azure credentials...")
    try:
        credential = AzureCliCredential()
        token = credential.get_token('https://storage.azure.com/.default')
        print("   ✅ Credentials obtained")
    except Exception as e:
        print(f"   ❌ Failed to get credentials: {e}")
        print("\n📝 Solution: az login")
        return 1
    
    # Test 1: List Files root
    print("\n🧪 Test 1: List Files/ root...")
    try:
        headers = {'Authorization': f'Bearer {token.token}'}
        url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_gold_id}/Files"
        params = {'resource': 'filesystem', 'recursive': 'false'}
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            paths = response.json().get('paths', [])
            print(f"   ✅ Success - Found {len(paths)} items")
            
            if paths:
                print("\n   📁 Contents:")
                for p in paths[:10]:  # Show first 10
                    name = p.get('name', '?')
                    is_dir = p.get('isDirectory', False)
                    size = p.get('contentLength', 0)
                    icon = '📁' if is_dir else '📄'
                    print(f"      {icon} {name} ({size:,} bytes)")
                
                if len(paths) > 10:
                    print(f"      ... and {len(paths) - 10} more")
            else:
                print("   ⚠️  Directory is empty")
        
        elif response.status_code == 403:
            print(f"   ❌ Access denied (403)")
            print(f"\n   📝 Solutions:")
            print(f"      1. Check Fabric workspace permissions")
            print(f"      2. Add your account to workspace with Contributor role")
            print(f"      3. Verify workspace ID is correct")
            return 1
        
        elif response.status_code == 404:
            print(f"   ❌ Not found (404)")
            print(f"\n   📝 Solutions:")
            print(f"      1. Verify workspace ID: {workspace_id}")
            print(f"      2. Verify lakehouse ID: {lakehouse_gold_id}")
            print(f"      3. Check lakehouse exists in Fabric Portal")
            return 1
        
        else:
            print(f"   ❌ Failed: {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return 1
    
    except Exception as e:
        print(f"   ❌ Exception: {e}")
        return 1
    
    # Test 2: Check specific folder
    print("\n🧪 Test 2: List Files/DEH...")
    try:
        url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_gold_id}/Files/DEH"
        params = {'resource': 'directory', 'recursive': 'false'}
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            paths = response.json().get('paths', [])
            print(f"   ✅ DEH folder exists - {len(paths)} items")
            
            if paths:
                for p in paths[:5]:
                    name = p.get('name', '?').split('/')[-1]
                    is_dir = p.get('isDirectory', False)
                    icon = '📁' if is_dir else '📄'
                    print(f"      {icon} {name}")
        
        elif response.status_code == 404:
            print(f"   ⚠️  DEH folder not found")
            print(f"   This may be OK if using different path")
        
        else:
            print(f"   ⚠️  Status: {response.status_code}")
    
    except Exception as e:
        print(f"   ⚠️  Exception: {e}")
    
    # Test 3: List Tables
    print("\n🧪 Test 3: List Tables/...")
    try:
        url = f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_gold_id}/Tables"
        params = {'resource': 'filesystem', 'recursive': 'false'}
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            paths = response.json().get('paths', [])
            print(f"   ✅ Found {len(paths)} tables")
            
            if paths:
                print("\n   📊 Tables:")
                for p in paths[:10]:
                    name = p.get('name', '?')
                    print(f"      • {name}")
        
        else:
            print(f"   ⚠️  Status: {response.status_code}")
    
    except Exception as e:
        print(f"   ⚠️  Exception: {e}")
    
    # Summary
    print("\n" + "="*80)
    print("  TEST SUMMARY")
    print("="*80)
    print()
    print("✅ OneLake connection working!")
    print()
    print("🔗 Correct URLs for Purview:")
    print()
    print("Option 1 (GUID-based - RECOMMENDED):")
    print(f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_gold_id}/Files/DEH")
    print()
    print("Option 2 (Name-based):")
    print("https://onelake.dfs.fabric.microsoft.com/DataGovernance/DEH.Lakehouse/Files/DEH")
    print("   ⚠️  Replace 'DataGovernance' with actual workspace name")
    print()
    print("Next steps:")
    print("  1. Copy correct URL above")
    print("  2. Update in Purview Portal:")
    print("     Unified Catalog → Solution integrations → Self-serve analytics")
    print("  3. Click 'Test connection'")
    print()
    
    return 0


if __name__ == '__main__':
    try:
        sys.exit(test_onelake_connection())
    except KeyboardInterrupt:
        print("\n\n❌ Test cancelled")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
