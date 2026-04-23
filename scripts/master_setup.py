#!/usr/bin/env python3
"""
Master Setup Script - Purview Complete Configuration

Runs all setup steps in correct order:
1. Azure CLI verification
2. Key Vault setup
3. Service Principal creation
4. Managed Identity configuration
5. Fabric analytics setup
6. Scan credentials configuration
7. Initial automation runs

USAGE:
    python scripts/master_setup.py
    
    # Or step-by-step
    python scripts/master_setup.py --step 1  # Only Key Vault
    python scripts/master_setup.py --step 2  # Only Service Principal
    # etc.
"""
import sys
import subprocess
import json
from pathlib import Path
from typing import Dict, List, Optional


class MasterSetup:
    """Master setup coordinator for all Purview configuration"""
    
    def __init__(self):
        """Initialize setup"""
        self.subscription_id = "5b44c9f3-bbe7-464c-aa3e-562726a12004"
        self.resource_group = "purview"
        self.purview_account = "prviewacc"
        self.key_vault_name = "prview-kv"
        self.location = "swedencentral"
        
        self.status = {
            'azure_cli': False,
            'key_vault': False,
            'service_principal': False,
            'managed_identity': False,
            'fabric_analytics': False,
            'scan_credentials': False,
            'automation': False
        }
    
    def run_command(self, command: str, shell: bool = True) -> tuple:
        """Run shell command and return output"""
        try:
            result = subprocess.run(
                command,
                shell=shell,
                capture_output=True,
                text=True,
                timeout=30
            )
            return result.returncode == 0, result.stdout, result.stderr
        except Exception as e:
            return False, "", str(e)
    
    def step1_verify_azure_cli(self) -> bool:
        """Step 1: Verify Azure CLI is logged in"""
        print("\n" + "="*80)
        print("  STEP 1: VERIFY AZURE CLI")
        print("="*80)
        print()
        
        print("🔍 Checking Azure CLI installation...")
        success, stdout, _ = self.run_command("az --version")
        
        if not success:
            print("❌ Azure CLI not installed")
            print("\n📝 Install Azure CLI:")
            print("   winget install Microsoft.AzureCLI")
            return False
        
        print("✅ Azure CLI installed")
        
        print("\n🔍 Checking Azure login status...")
        success, stdout, _ = self.run_command("az account show")
        
        if not success:
            print("❌ Not logged in to Azure")
            print("\n📝 Login to Azure:")
            print("   az login")
            return False
        
        # Parse account info
        try:
            account = json.loads(stdout)
            current_sub = account.get('id')
            
            print(f"✅ Logged in to subscription: {current_sub}")
            
            if current_sub != self.subscription_id:
                print(f"\n⚠️  Wrong subscription. Switching to {self.subscription_id}...")
                success, _, _ = self.run_command(
                    f"az account set --subscription {self.subscription_id}"
                )
                
                if success:
                    print("✅ Subscription switched")
                else:
                    print("❌ Failed to switch subscription")
                    return False
            
            self.status['azure_cli'] = True
            return True
        
        except Exception as e:
            print(f"❌ Failed to parse account info: {e}")
            return False
    
    def step2_setup_key_vault(self) -> bool:
        """Step 2: Create and configure Key Vault"""
        print("\n" + "="*80)
        print("  STEP 2: KEY VAULT SETUP")
        print("="*80)
        print()
        
        print(f"🔍 Checking if Key Vault '{self.key_vault_name}' exists...")
        success, stdout, _ = self.run_command(
            f"az keyvault show --name {self.key_vault_name} --resource-group {self.resource_group}"
        )
        
        if not success:
            print(f"📦 Creating Key Vault '{self.key_vault_name}'...")
            success, _, stderr = self.run_command(
                f"az keyvault create --name {self.key_vault_name} "
                f"--resource-group {self.resource_group} "
                f"--location {self.location}"
            )
            
            if not success:
                print(f"❌ Failed to create Key Vault: {stderr}")
                return False
            
            print("✅ Key Vault created")
        else:
            print("✅ Key Vault already exists")
        
        print("\n🔑 Granting current user access to Key Vault...")
        success, stdout, _ = self.run_command(
            "az ad signed-in-user show --query id -o tsv"
        )
        
        if success:
            user_id = stdout.strip()
            success, _, _ = self.run_command(
                f"az keyvault set-policy --name {self.key_vault_name} "
                f"--object-id {user_id} "
                f"--secret-permissions get list set delete"
            )
            
            if success:
                print("✅ Access granted")
            else:
                print("⚠️  Failed to grant access (may already have it)")
        
        print("\n⚙️  Running interactive Key Vault setup...")
        print("    This will prompt for Service Principal credentials...")
        print()
        
        # Run the Key Vault setup script
        success, _, _ = self.run_command(
            "python scripts/setup_keyvault_credentials.py"
        )
        
        if success:
            self.status['key_vault'] = True
            print("\n✅ Key Vault setup complete")
            return True
        else:
            print("\n⚠️  Key Vault setup incomplete (user may have skipped)")
            return False
    
    def step3_setup_service_principal(self) -> bool:
        """Step 3: Create Service Principal"""
        print("\n" + "="*80)
        print("  STEP 3: SERVICE PRINCIPAL SETUP")
        print("="*80)
        print()
        
        print("📝 Creating Service Principal for Unified Catalog API...")
        print()
        print("This requires:")
        print("  1. Creating App Registration in Entra ID")
        print("  2. Creating client secret")
        print("  3. Assigning Data Steward role in Purview")
        print()
        
        proceed = input("Proceed with Service Principal creation? (y/N): ").strip().lower()
        
        if proceed != 'y':
            print("⏭️  Skipping Service Principal setup")
            return False
        
        # Run the setup guide
        success, _, _ = self.run_command(
            "python scripts/setup_unified_catalog_access.py"
        )
        
        if success:
            self.status['service_principal'] = True
            print("\n✅ Service Principal setup complete")
            return True
        else:
            print("\n⚠️  Service Principal setup incomplete")
            return False
    
    def step4_setup_managed_identity(self) -> bool:
        """Step 4: Enable Purview Managed Identity"""
        print("\n" + "="*80)
        print("  STEP 4: PURVIEW MANAGED IDENTITY")
        print("="*80)
        print()
        
        print(f"🔍 Checking Managed Identity on {self.purview_account}...")
        success, stdout, _ = self.run_command(
            f"az purview account show --name {self.purview_account} "
            f"--resource-group {self.resource_group} "
            f"--query 'identity.type' -o tsv"
        )
        
        if success and "SystemAssigned" in stdout:
            print("✅ Managed Identity already enabled")
            
            # Get Principal ID
            success, principal_id, _ = self.run_command(
                f"az purview account show --name {self.purview_account} "
                f"--resource-group {self.resource_group} "
                f"--query 'identity.principalId' -o tsv"
            )
            
            if success:
                principal_id = principal_id.strip()
                print(f"   Principal ID: {principal_id}")
                
                # Grant Key Vault access
                print("\n🔑 Granting Key Vault access to Managed Identity...")
                success, _, _ = self.run_command(
                    f"az keyvault set-policy --name {self.key_vault_name} "
                    f"--object-id {principal_id} "
                    f"--secret-permissions get list"
                )
                
                if success:
                    print("✅ Key Vault access granted")
                else:
                    print("⚠️  Failed to grant Key Vault access")
            
            self.status['managed_identity'] = True
            return True
        
        else:
            print("⚠️  Managed Identity not enabled")
            print("\n📝 Enable Managed Identity:")
            print(f"   az purview account update --name {self.purview_account} "
                  f"--resource-group {self.resource_group} --mi-system-assigned")
            print()
            
            enable = input("Enable now? (y/N): ").strip().lower()
            
            if enable == 'y':
                success, _, stderr = self.run_command(
                    f"az purview account update --name {self.purview_account} "
                    f"--resource-group {self.resource_group} --mi-system-assigned"
                )
                
                if success:
                    print("✅ Managed Identity enabled")
                    self.status['managed_identity'] = True
                    return True
                else:
                    print(f"❌ Failed: {stderr}")
                    return False
            else:
                return False
    
    def step5_setup_fabric_analytics(self) -> bool:
        """Step 5: Configure Fabric Self-Serve Analytics"""
        print("\n" + "="*80)
        print("  STEP 5: FABRIC SELF-SERVE ANALYTICS")
        print("="*80)
        print()
        
        print("⚙️  Running Fabric analytics configuration...")
        
        # Run the Fabric setup script
        success, _, _ = self.run_command(
            "python scripts/configure_purview_fabric_analytics.py"
        )
        
        if success:
            self.status['fabric_analytics'] = True
            print("\n✅ Fabric analytics configured")
            return True
        else:
            print("\n⚠️  Fabric analytics configuration incomplete")
            print("\n📝 Manual steps required:")
            print("   1. Go to Fabric workspace 'DataGovernenne'")
            print("   2. Manage access → Add Purview Managed Identity")
            print("   3. Grant Contributor or Admin role")
            return False
    
    def step6_setup_scan_credentials(self) -> bool:
        """Step 6: Configure scan credentials"""
        print("\n" + "="*80)
        print("  STEP 6: SCAN CREDENTIALS")
        print("="*80)
        print()
        
        print("⚙️  Running scan credential configuration...")
        
        # Run the scan credential setup
        success, _, _ = self.run_command(
            "python scripts/configure_purview_scan_credentials.py"
        )
        
        if success:
            self.status['scan_credentials'] = True
            print("\n✅ Scan credentials configured")
            return True
        else:
            print("\n⚠️  Scan credential configuration incomplete")
            return False
    
    def step7_run_automation(self) -> bool:
        """Step 7: Run initial automation"""
        print("\n" + "="*80)
        print("  STEP 7: INITIAL AUTOMATION")
        print("="*80)
        print()
        
        print("🤖 Running automated domain linking (dry-run)...")
        success, _, _ = self.run_command(
            "python scripts/automate_domain_linking.py"
        )
        
        if success:
            proceed = input("\nApply domain linking changes? (y/N): ").strip().lower()
            
            if proceed == 'y':
                success, _, _ = self.run_command(
                    "python scripts/automate_domain_linking.py --live"
                )
                
                if success:
                    print("✅ Domain linking applied")
                else:
                    print("⚠️  Domain linking failed")
        
        print("\n🔍 Running initial health check...")
        success, _, _ = self.run_command(
            "python scripts/purview_monitoring.py"
        )
        
        if success:
            self.status['automation'] = True
            print("\n✅ Initial automation complete")
            return True
        else:
            print("\n⚠️  Automation incomplete")
            return False
    
    def print_summary(self):
        """Print setup summary"""
        print("\n" + "="*80)
        print("  SETUP SUMMARY")
        print("="*80)
        print()
        
        status_emoji = {True: '✅', False: '❌'}
        
        for step, completed in self.status.items():
            emoji = status_emoji[completed]
            print(f"{emoji} {step.replace('_', ' ').title()}: {'Complete' if completed else 'Incomplete'}")
        
        print()
        
        completed_count = sum(1 for v in self.status.values() if v)
        total_count = len(self.status)
        
        if completed_count == total_count:
            print(f"🎉 All {total_count} steps completed successfully!")
        else:
            print(f"⚠️  {completed_count}/{total_count} steps completed")
            print("\n📝 Next steps:")
            
            if not self.status['azure_cli']:
                print("   • Login to Azure CLI: az login")
            if not self.status['key_vault']:
                print("   • Run: python scripts/setup_keyvault_credentials.py")
            if not self.status['service_principal']:
                print("   • Run: python scripts/setup_unified_catalog_access.py")
            if not self.status['managed_identity']:
                print("   • Enable Managed Identity in Azure Portal")
            if not self.status['fabric_analytics']:
                print("   • Grant Fabric workspace permissions")
            if not self.status['scan_credentials']:
                print("   • Run: python scripts/configure_purview_scan_credentials.py")
        
        print()
    
    def run_all_steps(self):
        """Run all setup steps"""
        print("="*80)
        print("  PURVIEW MASTER SETUP")
        print("="*80)
        print()
        print("This will run all setup steps in sequence:")
        print("  1. Verify Azure CLI")
        print("  2. Setup Key Vault")
        print("  3. Create Service Principal")
        print("  4. Configure Managed Identity")
        print("  5. Setup Fabric Analytics")
        print("  6. Configure Scan Credentials")
        print("  7. Run Initial Automation")
        print()
        
        proceed = input("Proceed with full setup? (y/N): ").strip().lower()
        
        if proceed != 'y':
            print("\n❌ Setup cancelled")
            return 1
        
        # Run all steps
        steps = [
            self.step1_verify_azure_cli,
            self.step2_setup_key_vault,
            self.step3_setup_service_principal,
            self.step4_setup_managed_identity,
            self.step5_setup_fabric_analytics,
            self.step6_setup_scan_credentials,
            self.step7_run_automation
        ]
        
        for i, step in enumerate(steps, 1):
            try:
                result = step()
                if not result:
                    print(f"\n⚠️  Step {i} incomplete, continuing...")
            except Exception as e:
                print(f"\n❌ Step {i} failed: {e}")
                import traceback
                traceback.print_exc()
        
        # Print summary
        self.print_summary()
        
        return 0


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Master setup for Purview configuration")
    parser.add_argument('--step', type=int, help='Run specific step only (1-7)')
    
    args = parser.parse_args()
    
    try:
        setup = MasterSetup()
        
        if args.step:
            # Run specific step
            steps = [
                setup.step1_verify_azure_cli,
                setup.step2_setup_key_vault,
                setup.step3_setup_service_principal,
                setup.step4_setup_managed_identity,
                setup.step5_setup_fabric_analytics,
                setup.step6_setup_scan_credentials,
                setup.step7_run_automation
            ]
            
            if 1 <= args.step <= len(steps):
                steps[args.step - 1]()
            else:
                print(f"❌ Invalid step number: {args.step}")
                print(f"   Must be between 1 and {len(steps)}")
                return 1
        else:
            # Run all steps
            return setup.run_all_steps()
        
        return 0
    
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
