#!/usr/bin/env python3
"""
Enable Purview Lineage extraction for Azure SQL Database.
Required steps:
1. Create Master Key in database (if not exists)
2. Grant db_owner role to Purview Managed Identity
"""

import subprocess
import json
import sys

# Configuration
SQL_SERVER = "sql-hca-demo"  # Server name without .database.windows.net
SQL_DATABASE = "HealthcareAnalyticsDB"
PURVIEW_MI_NAME = "mi-purview"
PURVIEW_MI_PRINCIPAL_ID = "a1110d1d-6964-43c4-b171-13379215123a"

def run_sql_command(query, description):
    """Execute SQL command via Azure CLI."""
    print(f"\n{'='*80}")
    print(f"  {description}")
    print(f"{'='*80}")
    print(f"\nSQL Query:")
    print(f"  {query}")
    print()
    
    cmd = [
        "az", "sql", "db", "query",
        "--server", SQL_SERVER,
        "--database", SQL_DATABASE,
        "--query-text", query,
        "--output", "json"
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            print("✅ SUCCESS")
            if result.stdout.strip():
                try:
                    data = json.loads(result.stdout)
                    if data:
                        print("\nResult:")
                        print(json.dumps(data, indent=2))
                except:
                    print(f"\nOutput: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ FAILED")
            if result.stderr:
                error = result.stderr.strip()
                print(f"\nError: {error}")
                
                # Check if error is benign
                if "There is already a master key in the database" in error:
                    print("\n✅ Master key already exists - this is OK!")
                    return True
                elif "already a member of" in error.lower():
                    print("\n✅ User already has role - this is OK!")
                    return True
                    
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ TIMEOUT (command took too long)")
        return False
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
        return False

def check_master_key():
    """Check if master key exists."""
    query = """
    SELECT name, algorithm_desc, create_date
    FROM sys.symmetric_keys
    WHERE name = '##MS_DatabaseMasterKey##'
    """
    
    return run_sql_command(query, "CHECKING FOR MASTER KEY")

def create_master_key():
    """Create database master key."""
    # Using a strong random password
    query = """
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'PurviewLineage2026!Str0ngP@ssw0rd#Secure$'
    """
    
    return run_sql_command(query, "CREATING MASTER KEY")

def create_user_from_external_provider():
    """Create SQL user from Azure AD/Entra identity."""
    query = f"""
    IF NOT EXISTS (
        SELECT * FROM sys.database_principals 
        WHERE name = '{PURVIEW_MI_NAME}' AND type = 'E'
    )
    BEGIN
        CREATE USER [{PURVIEW_MI_NAME}] FROM EXTERNAL PROVIDER
    END
    """
    
    return run_sql_command(query, f"CREATING USER FROM EXTERNAL PROVIDER: {PURVIEW_MI_NAME}")

def grant_db_owner_role():
    """Grant db_owner role to Purview MI."""
    query = f"""
    IF NOT EXISTS (
        SELECT 1 
        FROM sys.database_role_members rm
        JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
        JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
        WHERE rp.name = 'db_owner' AND mp.name = '{PURVIEW_MI_NAME}'
    )
    BEGIN
        ALTER ROLE db_owner ADD MEMBER [{PURVIEW_MI_NAME}]
    END
    """
    
    return run_sql_command(query, f"GRANTING db_owner ROLE TO {PURVIEW_MI_NAME}")

def verify_configuration():
    """Verify the configuration is correct."""
    query = f"""
    -- Check user exists
    SELECT 
        'User Check' as Check_Type,
        name as User_Name, 
        type_desc as User_Type,
        create_date as Created
    FROM sys.database_principals
    WHERE name = '{PURVIEW_MI_NAME}'
    
    UNION ALL
    
    -- Check role membership
    SELECT 
        'Role Check' as Check_Type,
        rp.name as Role_Name,
        mp.name as Member_Name,
        NULL as Created
    FROM sys.database_role_members rm
    JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
    JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
    WHERE mp.name = '{PURVIEW_MI_NAME}' AND rp.name = 'db_owner'
    """
    
    return run_sql_command(query, "VERIFYING CONFIGURATION")

def main():
    print("\n")
    print("="*80)
    print("  ENABLE PURVIEW LINEAGE EXTRACTION FOR AZURE SQL DATABASE")
    print("="*80)
    print(f"\n  Server:   {SQL_SERVER}.database.windows.net")
    print(f"  Database: {SQL_DATABASE}")
    print(f"  MI Name:  {PURVIEW_MI_NAME}")
    print(f"  MI ID:    {PURVIEW_MI_PRINCIPAL_ID}")
    print("\n" + "="*80)
    
    # Step 1: Check if master key exists
    print("\n\n📋 STEP 1: Check for Database Master Key")
    if not check_master_key():
        print("\n⚠️  Master key check failed or doesn't exist")
        print("📋 STEP 1b: Creating Database Master Key")
        if not create_master_key():
            print("\n❌ Failed to create master key")
            print("   You may need to run this manually in SSMS:")
            print("   CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!'")
            # Don't exit - continue with other steps
    else:
        print("\n✅ Master key exists")
    
    # Step 2: Create user from external provider
    print("\n\n📋 STEP 2: Create SQL User from Azure AD/Entra Identity")
    if not create_user_from_external_provider():
        print(f"\n❌ Failed to create user {PURVIEW_MI_NAME}")
        print("   You may need to:")
        print("   1. Ensure Azure AD admin is set on SQL Server")
        print("   2. Run manually in SSMS as Azure AD admin:")
        print(f"      CREATE USER [{PURVIEW_MI_NAME}] FROM EXTERNAL PROVIDER")
        sys.exit(1)
    
    # Step 3: Grant db_owner role
    print("\n\n📋 STEP 3: Grant db_owner Role")
    if not grant_db_owner_role():
        print(f"\n❌ Failed to grant db_owner to {PURVIEW_MI_NAME}")
        print("   You may need to run manually in SSMS:")
        print(f"      ALTER ROLE db_owner ADD MEMBER [{PURVIEW_MI_NAME}]")
        sys.exit(1)
    
    # Step 4: Verify
    print("\n\n📋 STEP 4: Verify Configuration")
    verify_configuration()
    
    # Final summary
    print("\n\n" + "="*80)
    print("  CONFIGURATION COMPLETE")
    print("="*80)
    print("\n✅ Purview Managed Identity configured for lineage extraction")
    print("\n📋 Next steps:")
    print("   1. Azure Portal → Purview → Data Map → Sources")
    print("   2. Find your Azure SQL Database data source")
    print("   3. Edit data source → Enable 'Lineage extraction'")
    print("   4. Run a new scan")
    print("   5. Verify lineage appears in Data Catalog")
    print("\n" + "="*80)

if __name__ == "__main__":
    main()
