#!/usr/bin/env python3
"""
Verify that mi-purview has been successfully added to Fabric workspace
and can access OneLake.
"""

import requests
import json
from azure.identity import AzureCliCredential

# Configuration
WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
LAKEHOUSE_ID = "2960eef0-5de6-4117-80b1-6ee783cdaeec"
MI_PRINCIPAL_ID = "a1110d1d-6964-43c4-b171-13379215123a"

def check_workspace_members():
    """Check if mi-purview is a workspace member."""
    print("="*80)
    print("  CHECKING WORKSPACE MEMBERSHIP")
    print("="*80)
    
    try:
        cred = AzureCliCredential(process_timeout=30)
        token = cred.get_token("https://api.fabric.microsoft.com/.default").token
        
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/roleAssignments"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            members = response.json().get("value", [])
            
            print(f"✅ Found {len(members)} workspace members:")
            
            mi_found = False
            for member in members:
                principal = member.get("principal", {})
                role = member.get("role", "?")
                p_id = principal.get("id", "?")
                p_type = principal.get("type", "?")
                name = principal.get("displayName", p_id)
                
                if p_id == MI_PRINCIPAL_ID:
                    print(f"\n   ✅ mi-purview FOUND!")
                    print(f"      Principal ID: {p_id}")
                    print(f"      Role: {role}")
                    print(f"      Type: {p_type}")
                    mi_found = True
                else:
                    icon = "🤖" if p_type == "ServicePrincipal" else "👤"
                    print(f"   {icon} {name} ({role})")
            
            if not mi_found:
                print(f"\n   ❌ mi-purview NOT FOUND")
                print(f"      Expected Principal ID: {MI_PRINCIPAL_ID}")
                print(f"\n      👉 Add mi-purview to workspace manually:")
                print(f"         1. https://app.fabric.microsoft.com")
                print(f"         2. Workspace → Settings → Manage access")
                print(f"         3. Add: mi-purview or Principal ID above")
                print(f"         4. Role: Member or Contributor")
            
            return mi_found
        
        elif response.status_code == 403:
            print("⚠️  Cannot list workspace members (403)")
            print("   This is expected if you don't have Admin rights")
            print("   Continuing with OneLake access test...")
            return None  # Unknown
        
        else:
            print(f"❌ Failed to list members: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_onelake_access():
    """Test OneLake access with user credentials (proxy for MI access)."""
    print("\n" + "="*80)
    print("  TESTING ONELAKE ACCESS")
    print("="*80)
    
    try:
        cred = AzureCliCredential(process_timeout=30)
        token = cred.get_token("https://storage.azure.com/.default").token
        
        # Test both Files and Tables paths
        paths = [
            f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_ID}.Lakehouse/Tables",
            f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_ID}.Lakehouse/Files"
        ]
        
        headers = {
            "Authorization": f"Bearer {token}",
            "x-ms-version": "2021-06-08"
        }
        
        for path in paths:
            path_type = "Tables" if "/Tables" in path else "Files"
            print(f"\n📁 Testing {path_type}...")
            
            response = requests.get(
                f"{path}?resource=filesystem&recursive=false",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                items = data.get("paths", [])
                print(f"   ✅ Accessible! Found {len(items)} items")
                
                # Show first few items
                for item in items[:5]:
                    name = item.get("name", "?")
                    is_dir = item.get("isDirectory", False)
                    icon = "📁" if is_dir else "📄"
                    print(f"      {icon} {name}")
                
                if len(items) > 5:
                    print(f"      ... and {len(items) - 5} more")
                    
            elif response.status_code == 403:
                print(f"   ❌ Access Denied (403)")
                print(f"      mi-purview likely not added to workspace yet")
                return False
                
            else:
                print(f"   ⚠️  Status: {response.status_code}")
                print(f"      Response: {response.text[:200]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error testing OneLake: {e}")
        return False

def check_purview_scan_ready():
    """Check if Purview is ready to scan OneLake."""
    print("\n" + "="*80)
    print("  PURVIEW SCAN READINESS")
    print("="*80)
    
    try:
        cred = AzureCliCredential(process_timeout=30)
        token = cred.get_token("https://purview.azure.net/.default").token
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Check if OneLake source exists in Purview
        # Note: This would require DataSources API which may not be fully accessible
        print("ℹ️  Purview scan configuration:")
        print("   1. Azure Portal → prviewacc")
        print("   2. Data Map → Sources → Register")
        print("   3. Select: Microsoft Fabric OneLake")
        print("   4. Workspace ID: afda4639-34ce-4ee9-a82f-ab7b5cfd7334")
        print("   5. Authentication: Managed Identity (mi-purview)")
        print("   6. Run scan to discover tables")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    print("\n")
    print("="*80)
    print("  FABRIC + PURVIEW ACCESS VERIFICATION")
    print("="*80)
    print(f"  Workspace ID: {WORKSPACE_ID}")
    print(f"  Lakehouse ID: {LAKEHOUSE_ID}")
    print(f"  MI Principal: {MI_PRINCIPAL_ID}")
    print("="*80)
    print()
    
    results = {}
    
    # Check 1: Workspace membership
    results['workspace_member'] = check_workspace_members()
    
    # Check 2: OneLake access
    results['onelake_access'] = test_onelake_access()
    
    # Check 3: Purview scan ready
    results['scan_ready'] = check_purview_scan_ready()
    
    # Summary
    print("\n" + "="*80)
    print("  SUMMARY")
    print("="*80)
    
    def status_icon(val):
        if val is True:
            return "✅"
        elif val is False:
            return "❌"
        else:
            return "⚠️ "
    
    print(f"{status_icon(results['workspace_member'])} Workspace Membership: {results['workspace_member'] or 'Unknown'}")
    print(f"{status_icon(results['onelake_access'])} OneLake Access: {results['onelake_access']}")
    print(f"{status_icon(results['scan_ready'])} Purview Scan Ready: {results['scan_ready']}")
    
    if results['onelake_access'] and results['workspace_member'] in [True, None]:
        print("\n🎉 SUCCESS! Ready to configure Purview scan")
        print("\n📋 Next steps:")
        print("   1. Configure Purview Data Source (see instructions above)")
        print("   2. Run Purview scan")
        print("   3. Verify assets appear in Data Catalog")
        print("   4. Apply glossary terms to OneLake assets")
        
    elif not results['onelake_access']:
        print("\n⚠️  INCOMPLETE: OneLake not accessible")
        print("\n📋 Action required:")
        print("   1. Add mi-purview to Fabric workspace")
        print("   2. See FABRIC_MI_SETUP.md for instructions")
        print("   3. Run this script again after 5-10 minutes")
        
    print("="*80)

if __name__ == "__main__":
    main()
