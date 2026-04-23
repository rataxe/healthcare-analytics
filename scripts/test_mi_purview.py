"""
Test Purview User-Assigned MI (mi-purview) connectivity to Fabric OneLake
"""
import requests
from azure.identity import AzureCliCredential

print("=" * 80)
print("  TESTING USER-ASSIGNED MI: mi-purview")
print("=" * 80)

# Configuration
WORKSPACE_ID = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
LAKEHOUSE_GOLD_ID = "2960eef0-5de6-4117-80b1-6ee783cdaeec"

print("\n🔍 Step 1: Get Principal ID from Azure Portal")
print("-" * 80)
print("📋 Instructions:")
print("   1. In Azure Portal, click on 'mi-purview' in the table")
print("   2. Copy the 'Principal ID' (looks like: 12345678-abcd-...)")
print("   3. Also copy the 'Client ID'")
print()

principal_id = input("   Enter Principal ID: ").strip()
if not principal_id:
    print("   ⚠️  Skipping - no Principal ID provided")
    principal_id = "UNKNOWN"
else:
    print(f"   ✅ Principal ID: {principal_id}")

print("\n🔍 Step 2: Check Fabric Workspace Permissions")
print("-" * 80)
print("📋 This MI needs to be added to Fabric workspace:")
print(f"   Workspace ID: {WORKSPACE_ID}")
print("   Search for: mi-purview")
print(f"   Or paste: {principal_id}")
print("   Role: Contributor")
print()

print("\n🔍 Step 3: Test OneLake Connection (with YOUR credentials)")
print("-" * 80)
print("⚠️  Note: This tests with YOUR credentials, not the MI")
print("   For true MI test, must be done in Purview Portal\n")

try:
    credential = AzureCliCredential()
    token = credential.get_token('https://storage.azure.com/.default')
    headers = {'Authorization': f'Bearer {token.token}'}
    
    # Test Files/ directory
    url = f"https://onelake.dfs.fabric.microsoft.com/{WORKSPACE_ID}/{LAKEHOUSE_GOLD_ID}/Files"
    params = {'resource': 'filesystem', 'recursive': 'false'}
    
    print(f"   Testing: {url}")
    response = requests.get(url, headers=headers, params=params, timeout=30)
    
    if response.status_code == 200:
        print("   ✅ SUCCESS - Your credentials work!")
        paths = response.json().get('paths', [])
        print(f"   📁 Found {len(paths)} items in Files/")
    elif response.status_code == 403:
        print("   ❌ FAILED - 403 Access Denied")
        print("   📝 Your account needs Fabric workspace access")
    else:
        print(f"   ❌ FAILED - Status: {response.status_code}")
        print(f"   Response: {response.text[:200]}")

except Exception as e:
    print(f"   ❌ ERROR: {e}")

print("\n" + "=" * 80)
print("  NEXT STEPS")
print("=" * 80)

print("""
1. ✅ MI EXISTS: 'mi-purview' is already created

2. 📋 ADD MI TO FABRIC WORKSPACE:
   a) Open: https://app.fabric.microsoft.com
   b) Navigate to workspace (ID: afda4639-34ce-4ee9-a82f-ab7b5cfd7334)
   c) Workspace Settings → Manage access
   d) + Add people or groups
   e) Search: mi-purview (or paste Principal ID)
   f) Role: Contributor
   g) Add

3. 🔧 UPDATE PURVIEW CONFIGURATION:
   a) Open: https://web.purview.azure.com
   b) Select: prviewacc
   c) Unified Catalog → Solution integrations → Self-serve analytics
   d) Edit storage:
      - Authentication: User-assigned managed identity
      - Managed identity: mi-purview
      - Location URL: https://onelake.dfs.fabric.microsoft.com/{0}/{1}/Files/DEH
   e) Save → Test connection

4. ✅ VERIFY:
   Run test in Purview Portal - should show 'Connection successful'
""".format(WORKSPACE_ID, LAKEHOUSE_GOLD_ID))

print("\n📚 Documentation updated:")
print("   - PURVIEW_MI_STATUS.md (updated for User-Assigned MI)")
print("   - README_FABRIC_FIX.md (step-by-step guide)")
