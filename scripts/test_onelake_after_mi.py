#!/usr/bin/env python3
"""
Test OneLake access after adding mi-purview to workspaces.
"""

import requests
from azure.identity import AzureCliCredential

def test_workspace(name, workspace_id, lakehouse_id):
    """Test access to a specific workspace."""
    print(f"\n{'='*80}")
    print(f"  {name}")
    print(f"{'='*80}")
    print(f"Workspace ID: {workspace_id}")
    print(f"Lakehouse ID: {lakehouse_id}")
    
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token("https://storage.azure.com/.default").token
    
    headers = {
        "Authorization": f"Bearer {token}",
        "x-ms-version": "2021-06-08"
    }
    
    # Try multiple path formats
    paths_to_test = [
        f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}.Lakehouse/Tables",
        f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/Tables",
    ]
    
    for base_url in paths_to_test:
        url = f"{base_url}?resource=filesystem&recursive=false"
        print(f"\n📍 Testing: {base_url.split('/')[-1]}")
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                paths = data.get("paths", [])
                print(f"   ✅ SUCCESS! Found {len(paths)} items")
                
                for item in paths[:10]:
                    name_val = item.get("name", "?")
                    is_dir = item.get("isDirectory", False)
                    icon = "📁" if is_dir else "📄"
                    print(f"      {icon} {name_val}")
                
                if len(paths) > 10:
                    print(f"      ... and {len(paths) - 10} more")
                
                return True
                
            elif response.status_code == 403:
                print(f"   ❌ Access Denied (403)")
                print(f"      mi-purview may not have workspace access yet")
                print(f"      Or permissions haven't propagated (wait 5-10 min)")
                
            elif response.status_code == 404:
                print(f"   ⚠️  Not Found (404) - Path may be incorrect")
                
            else:
                print(f"   ⚠️  Error: {response.status_code}")
                error_text = response.text[:300]
                print(f"      {error_text}")
                
        except Exception as e:
            print(f"   ❌ Exception: {e}")
    
    return False

def main():
    print("\n")
    print("="*80)
    print("  ONELAKE ACCESS TEST - AFTER ADDING MI-PURVIEW")
    print("="*80)
    print()
    
    workspaces = [
        {
            "name": "Healthcare Analytics",
            "workspace_id": "afda4639-34ce-4ee9-a82f-ab7b5cfd7334",
            "lakehouse_id": "2960eef0-5de6-4117-80b1-6ee783cdaeec"
        },
        # Add BrainChild workspace if you have the IDs
    ]
    
    results = {}
    
    for ws in workspaces:
        success = test_workspace(
            ws["name"],
            ws["workspace_id"],
            ws["lakehouse_id"]
        )
        results[ws["name"]] = success
    
    # Summary
    print("\n" + "="*80)
    print("  SUMMARY")
    print("="*80)
    
    for name, success in results.items():
        icon = "✅" if success else "❌"
        print(f"{icon} {name}: {'Connected' if success else 'Failed'}")
    
    if all(results.values()):
        print("\n🎉 SUCCESS! All workspaces accessible")
        print("\n📋 Next steps:")
        print("   1. Azure Portal → prviewacc → Data Map → Sources")
        print("   2. Register → Microsoft Fabric OneLake")
        print("   3. Configure with mi-purview managed identity")
        print("   4. Run scan to discover tables")
    else:
        print("\n⚠️  Some workspaces not accessible")
        print("\n📋 Troubleshooting:")
        print("   1. Verify mi-purview added to workspace (Fabric Portal)")
        print("   2. Wait 5-10 minutes for permission propagation")
        print("   3. Check mi-purview role is Member or Contributor (not Viewer)")
        print("   4. Verify workspace and lakehouse IDs are correct")
    
    print("="*80)

if __name__ == "__main__":
    main()
