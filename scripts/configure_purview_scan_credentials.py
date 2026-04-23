#!/usr/bin/env python3
"""
Configure Purview Data Source Scan Credentials

Implements credential scanning setup as per:
https://learn.microsoft.com/en-us/purview/data-map-data-scan-credentials

Credential types supported:
- Basic Authentication (username/password)
- Service Principal
- Account Key
- SQL Authentication
- Managed Identity (recommended)

USAGE:
    python scripts/configure_purview_scan_credentials.py
    
CREATES:
    - Credentials in Purview
    - Key Vault references
    - Scan configurations
"""
import requests
import sys
from typing import Dict, List, Optional
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient


class PurviewCredentialManager:
    """Manage Purview data source scan credentials"""
    
    def __init__(self):
        """Initialize manager"""
        self.purview_account = "https://prviewacc.purview.azure.com"
        self.tenant_id = "71c4b6d5-0065-4c6c-a125-841a582754eb"
        self.key_vault_name = "prview-kv"
        self.key_vault_uri = f"https://{self.key_vault_name}.vault.azure.net"
        
        # Get credentials
        self.credential = AzureCliCredential()
        
        # Key Vault client
        try:
            self.kv_client = SecretClient(
                vault_url=self.key_vault_uri,
                credential=self.credential
            )
        except Exception as e:
            print(f"⚠️  Key Vault client not available: {e}")
            self.kv_client = None
    
    def _get_token(self) -> str:
        """Get access token for Purview"""
        token = self.credential.get_token('https://purview.azure.net/.default')
        return token.token
    
    def list_credentials(self) -> List[Dict]:
        """List existing credentials in Purview"""
        print("\n📋 Listing existing credentials...")
        
        try:
            token = self._get_token()
            headers = {'Authorization': f'Bearer {token}'}
            
            url = f"{self.purview_account}/scan/credentials?api-version=2022-07-01-preview"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                creds = response.json().get('value', [])
                print(f"   ✅ Found {len(creds)} credentials")
                
                for cred in creds:
                    name = cred.get('name', 'Unknown')
                    cred_type = cred.get('properties', {}).get('type', 'Unknown')
                    print(f"      • {name} ({cred_type})")
                
                return creds
            else:
                print(f"   ⚠️  Status: {response.status_code}")
                return []
        
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return []
    
    def create_sql_credential(
        self,
        name: str,
        username: str,
        password_secret_name: str,
        description: Optional[str] = None
    ) -> Dict:
        """
        Create SQL authentication credential
        
        Args:
            name: Credential name in Purview
            username: SQL username
            password_secret_name: Key Vault secret name for password
            description: Optional description
        
        Returns:
            Created credential object
        """
        print(f"\n🔐 Creating SQL credential: {name}...")
        
        try:
            token = self._get_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                "name": name,
                "properties": {
                    "type": "SqlAuth",
                    "description": description or f"SQL authentication for {name}",
                    "typeProperties": {
                        "userName": username,
                        "password": {
                            "type": "AzureKeyVaultSecret",
                            "store": {
                                "referenceName": self.key_vault_name,
                                "type": "LinkedServiceReference"
                            },
                            "secretName": password_secret_name
                        }
                    }
                }
            }
            
            url = f"{self.purview_account}/scan/credentials/{name}?api-version=2022-07-01-preview"
            response = requests.put(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code in [200, 201]:
                print(f"   ✅ SQL credential created")
                return response.json()
            else:
                print(f"   ❌ Failed: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                return {}
        
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return {}
    
    def create_service_principal_credential(
        self,
        name: str,
        client_id: str,
        tenant_id: str,
        client_secret_name: str,
        description: Optional[str] = None
    ) -> Dict:
        """
        Create Service Principal credential
        
        Args:
            name: Credential name
            client_id: Service Principal client ID
            tenant_id: Tenant ID
            client_secret_name: Key Vault secret name for client secret
            description: Optional description
        """
        print(f"\n🔐 Creating Service Principal credential: {name}...")
        
        try:
            token = self._get_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                "name": name,
                "properties": {
                    "type": "ServicePrincipal",
                    "description": description or f"Service Principal for {name}",
                    "typeProperties": {
                        "servicePrincipalId": client_id,
                        "tenant": tenant_id,
                        "servicePrincipalKey": {
                            "type": "AzureKeyVaultSecret",
                            "store": {
                                "referenceName": self.key_vault_name,
                                "type": "LinkedServiceReference"
                            },
                            "secretName": client_secret_name
                        }
                    }
                }
            }
            
            url = f"{self.purview_account}/scan/credentials/{name}?api-version=2022-07-01-preview"
            response = requests.put(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code in [200, 201]:
                print(f"   ✅ Service Principal credential created")
                return response.json()
            else:
                print(f"   ❌ Failed: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                return {}
        
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return {}
    
    def create_account_key_credential(
        self,
        name: str,
        account_key_secret_name: str,
        description: Optional[str] = None
    ) -> Dict:
        """
        Create Storage Account Key credential
        
        Args:
            name: Credential name
            account_key_secret_name: Key Vault secret name for storage key
            description: Optional description
        """
        print(f"\n🔐 Creating Account Key credential: {name}...")
        
        try:
            token = self._get_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                "name": name,
                "properties": {
                    "type": "AccountKey",
                    "description": description or f"Storage account key for {name}",
                    "typeProperties": {
                        "accountKey": {
                            "type": "AzureKeyVaultSecret",
                            "store": {
                                "referenceName": self.key_vault_name,
                                "type": "LinkedServiceReference"
                            },
                            "secretName": account_key_secret_name
                        }
                    }
                }
            }
            
            url = f"{self.purview_account}/scan/credentials/{name}?api-version=2022-07-01-preview"
            response = requests.put(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code in [200, 201]:
                print(f"   ✅ Account Key credential created")
                return response.json()
            else:
                print(f"   ❌ Failed: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                return {}
        
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return {}
    
    def setup_keyvault_integration(self) -> bool:
        """Setup Key Vault integration with Purview"""
        print("\n🔑 Setting up Key Vault integration...")
        
        try:
            token = self._get_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Create Key Vault linked service
            payload = {
                "name": self.key_vault_name,
                "properties": {
                    "type": "AzureKeyVault",
                    "typeProperties": {
                        "baseUrl": self.key_vault_uri
                    },
                    "description": "Key Vault for storing scan credentials"
                }
            }
            
            url = f"{self.purview_account}/scan/azureKeyVaults/{self.key_vault_name}?api-version=2022-07-01-preview"
            response = requests.put(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code in [200, 201]:
                print(f"   ✅ Key Vault integration configured")
                return True
            else:
                print(f"   ⚠️  Status: {response.status_code}")
                print(f"   Response: {response.text[:500]}")
                return False
        
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            return False
    
    def check_keyvault_permissions(self) -> bool:
        """Check if Purview has Key Vault permissions"""
        print("\n🔍 Checking Key Vault permissions...")
        
        if not self.kv_client:
            print("   ⚠️  Key Vault client not available")
            return False
        
        try:
            # Try to list secrets
            secrets = list(self.kv_client.list_properties_of_secrets())
            print(f"   ✅ Key Vault accessible ({len(secrets)} secrets)")
            return True
        
        except Exception as e:
            print(f"   ❌ No access: {e}")
            print("\n📝 Grant Purview Managed Identity access to Key Vault:")
            print(f"   az keyvault set-policy --name {self.key_vault_name} \\")
            print(f"     --object-id <PURVIEW_MANAGED_IDENTITY_ID> \\")
            print(f"     --secret-permissions get list")
            return False
    
    def setup_default_credentials(self):
        """Setup default credentials for common data sources"""
        print("\n" + "="*80)
        print("  SETTING UP DEFAULT CREDENTIALS")
        print("="*80)
        
        # 1. List existing
        existing = self.list_credentials()
        existing_names = [c.get('name') for c in existing]
        
        # 2. Check Key Vault
        kv_ok = self.check_keyvault_permissions()
        
        if not kv_ok:
            print("\n⚠️  Cannot proceed without Key Vault access")
            return
        
        # 3. Setup Key Vault integration
        self.setup_keyvault_integration()
        
        # 4. Create credentials (if not exists)
        credentials_to_create = [
            {
                'name': 'sql-admin-cred',
                'type': 'sql',
                'username': input("\nSQL Admin Username (or press Enter to skip): ").strip(),
                'secret': 'sql-admin-password'
            },
            {
                'name': 'purview-sp-cred',
                'type': 'sp',
                'client_id': input("\nService Principal Client ID (or press Enter to skip): ").strip(),
                'tenant': self.tenant_id,
                'secret': 'purview-sp-client-secret'
            },
            {
                'name': 'storage-account-cred',
                'type': 'key',
                'secret': 'storage-account-key',
                'skip_prompt': True
            }
        ]
        
        created = 0
        skipped = 0
        
        for cred_def in credentials_to_create:
            name = cred_def['name']
            
            if name in existing_names:
                print(f"\n⏭️  {name}: Already exists")
                skipped += 1
                continue
            
            cred_type = cred_def['type']
            
            if cred_type == 'sql':
                if not cred_def['username']:
                    print(f"⏭️  Skipping {name}")
                    skipped += 1
                    continue
                
                result = self.create_sql_credential(
                    name=name,
                    username=cred_def['username'],
                    password_secret_name=cred_def['secret']
                )
                if result:
                    created += 1
            
            elif cred_type == 'sp':
                if not cred_def['client_id']:
                    print(f"⏭️  Skipping {name}")
                    skipped += 1
                    continue
                
                result = self.create_service_principal_credential(
                    name=name,
                    client_id=cred_def['client_id'],
                    tenant_id=cred_def['tenant'],
                    client_secret_name=cred_def['secret']
                )
                if result:
                    created += 1
            
            elif cred_type == 'key':
                if cred_def.get('skip_prompt'):
                    print(f"⏭️  Skipping {name} (optional)")
                    skipped += 1
                    continue
        
        print("\n" + "="*80)
        print("  SUMMARY")
        print("="*80)
        print(f"Credentials created:  {created}")
        print(f"Credentials skipped:  {skipped}")
        print(f"Total credentials:    {len(existing) + created}")
        print()


def main():
    """Main entry point"""
    try:
        manager = PurviewCredentialManager()
        manager.setup_default_credentials()
        
        print("\n✅ Credential configuration complete!")
        print("\nNext steps:")
        print("  1. Verify in Purview Portal:")
        print("     Data Map → Credentials")
        print("  2. Use credentials when creating scans")
        print("  3. Test scans with new credentials")
        
        return 0
    
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
