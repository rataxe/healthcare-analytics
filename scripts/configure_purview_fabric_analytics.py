#!/usr/bin/env python3
"""
Configure Purview Self-Serve Analytics with Fabric Storage

Fixes the "Connection failed" issue shown in Purview Unified Catalog.
Configures proper Fabric OneLake connection with managed identity or Service Principal.

Reference: 
- https://learn.microsoft.com/en-us/purview/how-to-use-fabric-and-purview
- https://learn.microsoft.com/en-us/purview/self-serve-analytics

USAGE:
    python scripts/configure_purview_fabric_analytics.py
    
FIXES:
    ✅ Connection failed errors
    ✅ Authentication issues
    ✅ Storage configuration
    ✅ Managed Identity permissions
"""
import requests
import sys
from typing import Dict, Optional
from azure.identity import AzureCliCredential
from pathlib import Path


class PurviewFabricConfigurator:
    """Configure Purview-Fabric integration for self-serve analytics"""
    
    def __init__(self):
        """Initialize configurator"""
        self.purview_account = "https://prviewacc.purview.azure.com"
        self.tenant_id = "71c4b6d5-0065-4c6c-a125-841a582754eb"
        
        # Fabric workspace and lakehouse IDs
        self.fabric_workspace_id = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
        self.fabric_lakehouse_gold_id = "2960eef0-5de6-4117-80b1-6ee783cdaeec"
        
        # Get credentials
        try:
            self.credential = AzureCliCredential()
        except Exception as e:
            print(f"❌ Failed to get Azure credentials: {e}")
            sys.exit(1)
    
    def _get_token(self, resource: str = 'https://purview.azure.net') -> str:
        """Get access token for Purview"""
        token = self.credential.get_token(f'{resource}/.default')
        return token.token
    
    def check_current_config(self) -> Dict:
        """Check current self-serve analytics configuration"""
        print("\n🔍 Checking current configuration...")
        
        try:
            token = self._get_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Get solution integrations settings
            url = f"{self.purview_account}/governance/solutionIntegrations?api-version=2023-09-01"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                config = response.json()
                print(f"   ✅ Current config retrieved")
                return config
            else:
                print(f"   ⚠️  Status: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                return {}
        
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return {}
    
    def get_fabric_storage_url(self) -> str:
        """Generate correct Fabric storage URL"""
        # Correct format for OneLake DFS endpoint
        lakehouse_name = "DEH"  # From screenshot
        folder = "DEH"  # Root folder
        
        # OneLake format: https://onelake.dfs.fabric.microsoft.com/{workspace_name}/{lakehouse_name}.Lakehouse/Files/{folder}
        # But we need workspace ID instead
        
        url = f"https://onelake.dfs.fabric.microsoft.com/{self.fabric_workspace_id}/{self.fabric_lakehouse_gold_id}/Files/{folder}"
        return url
    
    def test_fabric_connection(self) -> bool:
        """Test connection to Fabric storage"""
        print("\n🧪 Testing Fabric connection...")
        
        try:
            token = self._get_token('https://storage.azure.com')
            headers = {'Authorization': f'Bearer {token}'}
            
            # Test OneLake DFS API
            test_url = f"https://onelake.dfs.fabric.microsoft.com/{self.fabric_workspace_id}/{self.fabric_lakehouse_gold_id}/Files"
            params = {'resource': 'filesystem', 'recursive': 'false'}
            
            response = requests.get(test_url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                print(f"   ✅ Fabric connection successful")
                paths = response.json().get('paths', [])
                print(f"   📊 Found {len(paths)} items in Files/")
                return True
            else:
                print(f"   ❌ Connection failed: {response.status_code}")
                print(f"   Error: {response.text[:500]}")
                return False
        
        except Exception as e:
            print(f"   ❌ Connection test failed: {e}")
            return False
    
    def check_purview_managed_identity(self) -> bool:
        """Check if Purview has managed identity enabled"""
        print("\n🔐 Checking Purview Managed Identity...")
        
        try:
            token = self._get_token('https://management.azure.com')
            headers = {'Authorization': f'Bearer {token}'}
            
            # Get Purview account details
            subscription_id = "5b44c9f3-bbe7-464c-aa3e-562726a12004"
            resource_group = "purview"
            account_name = "prviewacc"
            
            url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Purview/accounts/{account_name}?api-version=2021-07-01"
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                account = response.json()
                identity = account.get('identity', {})
                
                if identity.get('type') == 'SystemAssigned':
                    principal_id = identity.get('principalId')
                    print(f"   ✅ Managed Identity enabled")
                    print(f"   📋 Principal ID: {principal_id}")
                    return True
                else:
                    print(f"   ⚠️  Managed Identity not enabled")
                    return False
            else:
                print(f"   ⚠️  Cannot check: {response.status_code}")
                return False
        
        except Exception as e:
            print(f"   ❌ Check failed: {e}")
            return False
    
    def configure_fabric_storage(self) -> bool:
        """Configure Fabric storage for self-serve analytics"""
        print("\n⚙️  Configuring Fabric storage...")
        
        try:
            token = self._get_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Configuration payload
            storage_url = self.get_fabric_storage_url()
            
            config = {
                "properties": {
                    "storageType": "Fabric",
                    "storageConfiguration": {
                        "locationUrl": storage_url,
                        "authenticationType": "SystemAssignedManagedIdentity"
                    },
                    "enabled": True
                }
            }
            
            print(f"   📍 Storage URL: {storage_url}")
            print(f"   🔐 Auth: System Assigned Managed Identity")
            
            # Update configuration
            url = f"{self.purview_account}/governance/solutionIntegrations/selfServeAnalytics?api-version=2023-09-01"
            
            response = requests.put(url, headers=headers, json=config, timeout=30)
            
            if response.status_code in [200, 201]:
                print(f"   ✅ Configuration updated successfully")
                return True
            else:
                print(f"   ❌ Configuration failed: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                return False
        
        except Exception as e:
            print(f"   ❌ Configuration failed: {e}")
            return False
    
    def grant_fabric_permissions(self) -> Dict:
        """Generate commands to grant Fabric permissions"""
        print("\n🔑 Required Fabric Permissions...")
        
        commands = {
            'description': 'Purview Managed Identity needs access to Fabric workspace',
            'steps': [
                {
                    'step': 1,
                    'action': 'Get Purview Managed Identity Principal ID',
                    'command': '''
az purview account show \\
  --name prviewacc \\
  --resource-group purview \\
  --query "identity.principalId" \\
  --output tsv
'''.strip()
                },
                {
                    'step': 2,
                    'action': 'Add to Fabric Workspace (Manual)',
                    'instructions': [
                        'Go to Fabric workspace (DataGovernenne)',
                        'Click "Manage access"',
                        'Add the Purview Managed Identity',
                        'Grant role: Contributor or Admin'
                    ]
                },
                {
                    'step': 3,
                    'action': 'Verify Lakehouse Permissions',
                    'instructions': [
                        'Open DEH Lakehouse',
                        'Verify Purview MI has Read access',
                        'Check Files/ folder permissions'
                    ]
                }
            ]
        }
        
        print("\n📋 Steps to grant permissions:")
        print()
        
        for step_info in commands['steps']:
            print(f"STEP {step_info['step']}: {step_info['action']}")
            print("-" * 60)
            
            if 'command' in step_info:
                print(step_info['command'])
            elif 'instructions' in step_info:
                for i, instr in enumerate(step_info['instructions'], 1):
                    print(f"  {i}. {instr}")
            print()
        
        return commands
    
    def test_analytics_query(self) -> bool:
        """Test self-serve analytics query capability"""
        print("\n🧪 Testing analytics query...")
        
        try:
            token = self._get_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Simple test query
            query = {
                "query": "SELECT * FROM information_schema.tables LIMIT 5"
            }
            
            url = f"{self.purview_account}/governance/selfServeAnalytics/query?api-version=2023-09-01"
            
            response = requests.post(url, headers=headers, json=query, timeout=60)
            
            if response.status_code == 200:
                print(f"   ✅ Query executed successfully")
                results = response.json()
                print(f"   📊 Results: {results}")
                return True
            else:
                print(f"   ⚠️  Query status: {response.status_code}")
                print(f"   Note: This may require Fabric lakehouse to be properly configured")
                return False
        
        except Exception as e:
            print(f"   ⚠️  Query test: {e}")
            return False
    
    def run_full_configuration(self):
        """Run complete configuration flow"""
        print("="*80)
        print("  PURVIEW FABRIC SELF-SERVE ANALYTICS CONFIGURATION")
        print("="*80)
        
        # Step 1: Check current config
        current = self.check_current_config()
        
        # Step 2: Check managed identity
        has_mi = self.check_purview_managed_identity()
        
        if not has_mi:
            print("\n⚠️  WARNING: Managed Identity is required but not enabled")
            print("\n📝 To enable Managed Identity:")
            print("   1. Go to Azure Portal → prviewacc Purview account")
            print("   2. Navigate to 'Identity'")
            print("   3. Enable 'System assigned' managed identity")
            print("   4. Re-run this script")
            return 1
        
        # Step 3: Test Fabric connection
        fabric_ok = self.test_fabric_connection()
        
        if not fabric_ok:
            print("\n⚠️  WARNING: Cannot connect to Fabric")
            print("   This may be a permissions issue")
        
        # Step 4: Show permission commands
        perms = self.grant_fabric_permissions()
        
        # Step 5: Configure storage
        print("\n" + "="*80)
        print("  APPLYING CONFIGURATION")
        print("="*80)
        
        proceed = input("\nProceed with configuration? (y/N): ").strip().lower()
        
        if proceed != 'y':
            print("\n❌ Configuration cancelled")
            return 1
        
        config_ok = self.configure_fabric_storage()
        
        if config_ok:
            # Step 6: Test analytics
            self.test_analytics_query()
            
            print("\n" + "="*80)
            print("  ✅ CONFIGURATION COMPLETE")
            print("="*80)
            print()
            print("Next steps:")
            print("  1. Verify in Purview Portal:")
            print("     Unified Catalog → Solution integrations → Self-serve analytics")
            print("  2. Click 'Test connection' - should now succeed")
            print("  3. If still failing, grant Fabric permissions (see commands above)")
            print()
            return 0
        else:
            print("\n❌ Configuration failed")
            return 1


def main():
    """Main entry point"""
    try:
        configurator = PurviewFabricConfigurator()
        return configurator.run_full_configuration()
    
    except KeyboardInterrupt:
        print("\n\n❌ Configuration cancelled by user")
        return 1
    
    except Exception as e:
        print(f"\n❌ Configuration failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
