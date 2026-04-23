#!/usr/bin/env python3
"""
Configure Fabric workspace to grant mi-purview access.
Adds the managed identity as a workspace member so Purview can scan OneLake.
"""

import requests
import json
from azure.identity import AzureCliCredential

# Configuration
WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
MI_PRINCIPAL_ID = "a1110d1d-6964-43c4-b171-13379215123a"  # mi-purview
MI_NAME = "mi-purview"

def get_fabric_token():
    """Get token for Fabric API."""
    cred = AzureCliCredential(process_timeout=30)
    token = cred.get_token("https://api.fabric.microsoft.com/.default")
    return token.token

def add_workspace_member(token):
    """Add mi-purview as workspace member."""
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/roleAssignments"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Add as Member (Contributor would also work)
    body = {
        "principal": {
            "id": MI_PRINCIPAL_ID,
            "type": "ServicePrincipal"  # Managed Identity uses same type as SP
        },
        "role": "Member"  # Options: Admin, Member, Contributor, Viewer
    }
    
    print(f"🔧 Adding {MI_NAME} to workspace...")
    print(f"   Principal ID: {MI_PRINCIPAL_ID}")
    print(f"   Workspace ID: {WORKSPACE_ID}")
    print(f"   Role: Member")
    
    response = requests.post(url, headers=headers, json=body, timeout=30)
    
    if response.status_code in [200, 201]:
        print(f"✅ Successfully added {MI_NAME} to workspace")
        return True
    elif response.status_code == 409:
        print(f"ℹ️  {MI_NAME} already has access to workspace")
        return True
    else:
        print(f"❌ Failed to add workspace member: {response.status_code}")
        print(f"   Response: {response.text}")
        return False

def list_workspace_members(token):
    """List all workspace members to verify."""
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/roleAssignments"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    print(f"\n📋 Current workspace members:")
    response = requests.get(url, headers=headers, timeout=30)
    
    if response.status_code == 200:
        members = response.json().get("value", [])
        for member in members:
            principal = member.get("principal", {})
            role = member.get("role", "?")
            name = principal.get("displayName", principal.get("id", "?"))
            p_type = principal.get("type", "?")
            p_id = principal.get("id", "?")
            
            icon = "🤖" if p_type == "ServicePrincipal" else "👤"
            check = "✓" if p_id == MI_PRINCIPAL_ID else " "
            
            print(f"   {icon} [{check}] {name} ({role}) - {p_type}")
        
        return True
    else:
        print(f"   ❌ Failed to list members: {response.status_code}")
        return False

def test_onelake_access(token):
    """Test if we can now access OneLake with user token (as proxy check)."""
    # Note: This uses user token, not MI token
    # Real Purview scan would use MI token
    
    GOLD_ID = "2960eef0-5de6-4117-80b1-6ee783cdaeec"
    base_url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{GOLD_ID}.Lakehouse/Tables"
    
    # Get storage token
    cred = AzureCliCredential(process_timeout=30)
    storage_token = cred.get_token("https://storage.azure.com/.default").token
    
    headers = {
        "Authorization": f"Bearer {storage_token}",
        "x-ms-version": "2021-06-08"
    }
    
    print(f"\n🧪 Testing OneLake access...")
    response = requests.get(
        f"{base_url}?resource=filesystem&recursive=false",
        headers=headers,
        timeout=30
    )
    
    if response.status_code == 200:
        paths = response.json().get("paths", [])
        print(f"✅ OneLake accessible! Found {len(paths)} tables:")
        for p in paths[:5]:  # Show first 5
            print(f"   - {p.get('name', '?')}")
        return True
    else:
        print(f"⚠️  OneLake access: {response.status_code}")
        print(f"   (MI needs time to propagate permissions)")
        return False

def main():
    print("="*80)
    print("  FABRIC WORKSPACE CONFIGURATION")
    print("="*80)
    
    try:
        # Get Fabric token
        token = get_fabric_token()
        print("✅ Authenticated to Fabric API\n")
        
        # Add MI to workspace
        if not add_workspace_member(token):
            print("\n⚠️  Could not add MI to workspace")
            print("    Manual steps required:")
            print(f"    1. Open https://app.fabric.microsoft.com")
            print(f"    2. Navigate to workspace: {WORKSPACE_ID}")
            print(f"    3. Settings → Manage access")
            print(f"    4. Add: {MI_NAME} (Principal ID: {MI_PRINCIPAL_ID})")
            print(f"    5. Role: Member or Contributor")
        
        # List members to verify
        list_workspace_members(token)
        
        # Test OneLake access
        test_onelake_access(token)
        
        print("\n" + "="*80)
        print("  NEXT STEPS FOR PURVIEW")
        print("="*80)
        print("1. Azure Portal → Purview Account (prviewacc)")
        print("2. Data Map → Sources")
        print("3. Register → OneLake")
        print(f"4. Use managed identity: {MI_NAME}")
        print(f"5. Workspace ID: {WORKSPACE_ID}")
        print("6. Run scan to discover tables")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
