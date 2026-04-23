#!/usr/bin/env python3
"""
Setup Azure Key Vault for Purview Credentials

Stores all sensitive credentials in Azure Key Vault:
- Service Principal credentials
- SQL connection strings  
- Storage account keys
- API keys

Reference: https://learn.microsoft.com/en-us/purview/data-map-data-scan-credentials

USAGE:
    python scripts/setup_keyvault_credentials.py

PREREQUISITES:
    1. Azure Key Vault created
    2. User has Key Vault Secrets Officer role
    3. Service Principal has Key Vault Secrets User role
"""
import os
import sys
from pathlib import Path
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
import json

# Configuration
KEY_VAULT_NAME = "prview-kv"  # Change to your Key Vault name
KEY_VAULT_URI = f"https://{KEY_VAULT_NAME}.vault.azure.net"

# Secret names (following Purview naming conventions)
SECRETS = {
    # Service Principal for Purview
    "purview-sp-tenant-id": "Tenant ID for Service Principal",
    "purview-sp-client-id": "Client ID (Application ID)",
    "purview-sp-client-secret": "Client Secret",
    
    # Purview Account Details
    "purview-account-name": "prviewacc",
    "purview-account-url": "https://prviewacc.purview.azure.com",
    "purview-tenant-id": "71c4b6d5-0065-4c6c-a125-841a582754eb",
    
    # Fabric OneLake Credentials
    "fabric-workspace-id": "afda4639-34ce-4ee9-a82f-ab7b5cfd7334",
    "fabric-lakehouse-bronze-id": "50b8f61e-9f6e-42bf-9f23-4e699c2fac65",
    "fabric-lakehouse-silver-id": "06f20e1a-45f2-4cf8-80a5-a69f72cf0c31",
    "fabric-lakehouse-gold-id": "2960eef0-5de6-4117-80b1-6ee783cdaeec",
    
    # SQL Database Credentials
    "sql-server-name": "SQL Server name",
    "sql-database-name": "SQL Database name",
    "sql-admin-username": "SQL Admin username",
    "sql-admin-password": "SQL Admin password",
    "sql-connection-string": "Full SQL connection string",
    
    # Storage Account Credentials
    "storage-account-name": "Storage account name",
    "storage-account-key": "Storage account key",
    "storage-connection-string": "Storage connection string",
    
    # API Keys
    "openai-api-key": "OpenAI API key (if used)",
    "fabric-api-key": "Fabric API key (if used)",
}


def get_secret_client() -> SecretClient:
    """Get authenticated Key Vault client"""
    credential = AzureCliCredential()
    client = SecretClient(vault_url=KEY_VAULT_URI, credential=credential)
    return client


def check_keyvault_access():
    """Check if we have access to Key Vault"""
    print("🔍 Checking Key Vault access...")
    try:
        client = get_secret_client()
        # Try to list secrets (will fail if no access)
        secrets = list(client.list_properties_of_secrets())
        print(f"✅ Access confirmed to {KEY_VAULT_URI}")
        print(f"   Found {len(secrets)} existing secrets")
        return True
    except Exception as e:
        print(f"❌ Cannot access Key Vault: {e}")
        print("\n⚠️  Required permissions:")
        print("   • Key Vault Secrets Officer (to create secrets)")
        print("   • Or Key Vault Administrator")
        print(f"\n📝 Grant access with:")
        print(f"   az keyvault set-policy --name {KEY_VAULT_NAME} \\")
        print(f"     --object-id $(az ad signed-in-user show --query id -o tsv) \\")
        print(f"     --secret-permissions get list set delete")
        return False


def get_user_input(secret_name: str, description: str, required: bool = True) -> str:
    """Prompt user for secret value"""
    prompt = f"\n{secret_name}\n  {description}\n  Value"
    if not required:
        prompt += " (optional)"
    prompt += ": "
    
    value = input(prompt).strip()
    
    if required and not value:
        print(f"❌ {secret_name} is required!")
        sys.exit(1)
    
    return value


def set_secret(client: SecretClient, name: str, value: str):
    """Set secret in Key Vault"""
    if not value:
        print(f"⏭️  Skipping {name} (no value)")
        return
    
    try:
        client.set_secret(name, value)
        print(f"✅ Stored: {name}")
    except Exception as e:
        print(f"❌ Failed to store {name}: {e}")


def interactive_setup():
    """Interactive setup - prompt for all secrets"""
    print("="*80)
    print("  AZURE KEY VAULT CREDENTIAL SETUP")
    print("="*80)
    print()
    print(f"Target Key Vault: {KEY_VAULT_URI}")
    print()
    print("This will guide you through storing all Purview credentials")
    print("in Azure Key Vault for secure access.")
    print()
    
    # Check access
    if not check_keyvault_access():
        return 1
    
    print("\n" + "="*80)
    print("  STEP 1: SERVICE PRINCIPAL CREDENTIALS")
    print("="*80)
    print()
    print("You need a Service Principal for Purview Unified Catalog API access.")
    print()
    print("Create one with:")
    print("  1. Go to Entra ID → App registrations → New registration")
    print("  2. Name: 'Purview-ServicePrincipal'")
    print("  3. Copy Application (client) ID")
    print("  4. Go to Certificates & secrets → New client secret")
    print("  5. Copy the secret value")
    print()
    
    sp_tenant = input("\nService Principal Tenant ID: ").strip() or "71c4b6d5-0065-4c6c-a125-841a582754eb"
    sp_client = input("Service Principal Client ID: ").strip()
    sp_secret = input("Service Principal Client Secret: ").strip()
    
    if not sp_client or not sp_secret:
        print("\n⚠️  Service Principal credentials are REQUIRED for Unified Catalog API")
        print("   Continuing without them will limit functionality")
        proceed = input("\nContinue anyway? (y/N): ").strip().lower()
        if proceed != 'y':
            return 1
    
    print("\n" + "="*80)
    print("  STEP 2: SQL DATABASE CREDENTIALS (Optional)")
    print("="*80)
    print()
    print("If you have a SQL Database registered in Purview, provide credentials.")
    print("Otherwise, skip this section.")
    print()
    
    sql_server = input("SQL Server name (or press Enter to skip): ").strip()
    sql_database = ""
    sql_username = ""
    sql_password = ""
    sql_connection = ""
    
    if sql_server:
        sql_database = input("SQL Database name: ").strip()
        sql_username = input("SQL Admin username: ").strip()
        sql_password = input("SQL Admin password: ").strip()
        sql_connection = f"Server={sql_server};Database={sql_database};User Id={sql_username};Password={sql_password};Encrypt=True;"
    
    print("\n" + "="*80)
    print("  STEP 3: STORAGE ACCOUNT CREDENTIALS (Optional)")
    print("="*80)
    print()
    
    storage_name = input("Storage Account name (or press Enter to skip): ").strip()
    storage_key = ""
    storage_connection = ""
    
    if storage_name:
        storage_key = input("Storage Account key: ").strip()
        storage_connection = f"DefaultEndpointsProtocol=https;AccountName={storage_name};AccountKey={storage_key};EndpointSuffix=core.windows.net"
    
    print("\n" + "="*80)
    print("  STEP 4: STORING SECRETS IN KEY VAULT")
    print("="*80)
    print()
    
    client = get_secret_client()
    
    # Store all secrets
    secrets_to_store = {
        "purview-sp-tenant-id": sp_tenant,
        "purview-sp-client-id": sp_client,
        "purview-sp-client-secret": sp_secret,
        "purview-account-name": "prviewacc",
        "purview-account-url": "https://prviewacc.purview.azure.com",
        "purview-tenant-id": "71c4b6d5-0065-4c6c-a125-841a582754eb",
        "fabric-workspace-id": "afda4639-34ce-4ee9-a82f-ab7b5cfd7334",
        "fabric-lakehouse-bronze-id": "50b8f61e-9f6e-42bf-9f23-4e699c2fac65",
        "fabric-lakehouse-silver-id": "06f20e1a-45f2-4cf8-80a5-a69f72cf0c31",
        "fabric-lakehouse-gold-id": "2960eef0-5de6-4117-80b1-6ee783cdaeec",
        "sql-server-name": sql_server,
        "sql-database-name": sql_database,
        "sql-admin-username": sql_username,
        "sql-admin-password": sql_password,
        "sql-connection-string": sql_connection,
        "storage-account-name": storage_name,
        "storage-account-key": storage_key,
        "storage-connection-string": storage_connection,
    }
    
    for name, value in secrets_to_store.items():
        set_secret(client, name, value)
    
    print("\n" + "="*80)
    print("  STEP 5: CREATE .env.purview FROM KEY VAULT")
    print("="*80)
    print()
    
    # Create .env.purview file
    env_content = f"""# Purview Credentials (from Azure Key Vault)
# DO NOT COMMIT THIS FILE TO SOURCE CONTROL
# Generated: {Path(__file__).name}

PURVIEW_TENANT_ID={sp_tenant}
PURVIEW_CLIENT_ID={sp_client}
PURVIEW_CLIENT_SECRET={sp_secret}
PURVIEW_ACCOUNT=https://prviewacc.purview.azure.com
UNIFIED_CATALOG_BASE=https://prviewacc.purview.azure.com/datagovernance/catalog
API_VERSION=2025-09-15-preview

# Key Vault Details
KEY_VAULT_NAME={KEY_VAULT_NAME}
KEY_VAULT_URI={KEY_VAULT_URI}
"""
    
    env_file = Path("scripts/.env.purview")
    env_file.write_text(env_content)
    print(f"✅ Created: {env_file}")
    
    # Also create helper script
    helper_content = '''#!/usr/bin/env python3
"""
Get credentials from Azure Key Vault

Usage:
    from get_keyvault_secrets import get_secret, get_all_secrets
    
    client_id = get_secret("purview-sp-client-id")
    all_secrets = get_all_secrets()
"""
from azure.identity import AzureCliCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from typing import Dict, Optional

KEY_VAULT_URI = "''' + KEY_VAULT_URI + '''"

def get_secret_client() -> SecretClient:
    """Get authenticated Key Vault client"""
    try:
        credential = AzureCliCredential()
    except:
        credential = DefaultAzureCredential()
    return SecretClient(vault_url=KEY_VAULT_URI, credential=credential)

def get_secret(secret_name: str) -> Optional[str]:
    """Get single secret from Key Vault"""
    try:
        client = get_secret_client()
        secret = client.get_secret(secret_name)
        return secret.value
    except Exception as e:
        print(f"Failed to get secret {secret_name}: {e}")
        return None

def get_all_secrets() -> Dict[str, str]:
    """Get all secrets from Key Vault"""
    try:
        client = get_secret_client()
        secrets = {}
        for secret_properties in client.list_properties_of_secrets():
            secret = client.get_secret(secret_properties.name)
            secrets[secret_properties.name] = secret.value
        return secrets
    except Exception as e:
        print(f"Failed to get secrets: {e}")
        return {}

def get_purview_credentials() -> Dict[str, str]:
    """Get Purview-specific credentials"""
    return {
        'tenant_id': get_secret('purview-sp-tenant-id'),
        'client_id': get_secret('purview-sp-client-id'),
        'client_secret': get_secret('purview-sp-client-secret'),
        'account_url': get_secret('purview-account-url'),
    }

if __name__ == '__main__':
    # Test
    print("Testing Key Vault access...")
    creds = get_purview_credentials()
    print(f"✅ Successfully retrieved {len([v for v in creds.values() if v])} credentials")
'''
    
    helper_file = Path("scripts/get_keyvault_secrets.py")
    helper_file.write_text(helper_content)
    print(f"✅ Created: {helper_file}")
    
    print("\n" + "="*80)
    print("  ✅ SETUP COMPLETE")
    print("="*80)
    print()
    print("Next steps:")
    print("  1. Test credentials:")
    print("     python scripts/get_keyvault_secrets.py")
    print()
    print("  2. Test Unified Catalog API:")
    print("     python scripts/test_unified_catalog.py")
    print()
    print("  3. Update unified_catalog_client.py to use Key Vault:")
    print("     from get_keyvault_secrets import get_purview_credentials")
    print()
    print("🔒 Security:")
    print("   • All secrets stored in Azure Key Vault")
    print("   • .env.purview created (DO NOT commit to git)")
    print("   • Scripts can now use get_keyvault_secrets.py")
    print()
    
    return 0


def main():
    """Main entry point"""
    try:
        return interactive_setup()
    except KeyboardInterrupt:
        print("\n\n❌ Setup cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Setup failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
