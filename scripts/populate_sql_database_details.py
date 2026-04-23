#!/usr/bin/env python3
"""
Populate SQL Database Details - Critical Elements & OKRs
Adds business metadata to SQL Server database and tables in Purview
"""
import requests
import time
from azure.identity import AzureCliCredential

# ══════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════
PURVIEW_ACCOUNT = "prviewacc"
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
ATLAS_API = f"{PURVIEW_ENDPOINT}/catalog/api/atlas/v2"

SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DATABASE = "sql-hca-demo"

# ══════════════════════════════════════════════════════════
# SQL DATABASE METADATA
# ══════════════════════════════════════════════════════════
DATABASE_DETAILS = {
    "description": "Azure SQL Database för Healthcare Analytics demo. Innehåller kliniska tabeller med patientdata, diagnoser (ICD-10), läkemedel (ATC), laboratorieprov (LOINC) och DICOM-metadata för radiologiska studier. Används för analytisk datavarehousing och BI-rapportering.",
    "owner": "joandolf@microsoft.com",
    "steward": "Healthcare Database Team",
    "data_quality_contact": "joandolf@microsoft.com",
    "critical_elements": [
        {
            "name": "PersonnummerColumn",
            "type": "PII",
            "sensitivity": "Very High",
            "retention": "10 years (legal requirement)",
            "description": "Kolumner innehållande svenska personnummer - kräver strict access control",
            "columns": ["patient_id i patients-tabellen", "person_id i andra tabeller"]
        },
        {
            "name": "DiagnosisData",
            "type": "Clinical",
            "sensitivity": "High",
            "retention": "10 years",
            "description": "ICD-10 diagnoskoder och diagnosbeskrivningar",
            "columns": ["diagnosis_code", "diagnosis_description"]
        },
        {
            "name": "MedicationData",
            "type": "Clinical",
            "sensitivity": "High",
            "retention": "10 years",
            "description": "ATC-koder och läkemedelsinformation",
            "columns": ["atc_code", "medication_name", "dosage"]
        },
        {
            "name": "LabResults",
            "type": "Clinical",
            "sensitivity": "Medium",
            "retention": "10 years",
            "description": "LOINC-koder och laboratorieprovresultat",
            "columns": ["loinc_code", "test_result", "unit"]
        },
        {
            "name": "DICOMMetadata",
            "type": "Clinical",
            "sensitivity": "High",
            "retention": "10 years",
            "description": "Metadata för radiologiska studier och bildundersökningar",
            "columns": ["study_instance_uid", "series_instance_uid", "modality"]
        }
    ],
    "okrs": [
        {
            "objective": "Säkerställ hög datatillgänglighet för BI och Analytics",
            "key_results": [
                "99.9% uptime för SQL database",
                "Query response time p95 <= 500ms",
                "Nightly backup success rate >= 99.5%"
            ]
        },
        {
            "objective": "Förbättra datakvalitet i SQL-tabeller",
            "key_results": [
                ">=98% NOT NULL constraints enforced på critical columns",
                "<=1% orphaned foreign key references",
                "Data validation checks körs dagligen med >=95% pass rate"
            ]
        },
        {
            "objective": "Efterlev säkerhet och compliance krav",
            "key_results": [
                "100% av PII-kolumner har Dynamic Data Masking enabled",
                "Audit logging på alla SELECT/UPDATE/DELETE på sensitive tables",
                "Row Level Security (RLS) implementerad för multi-tenant scenarios"
            ]
        }
    ],
    "sla": {
        "availability": "99.9%",
        "backup_frequency": "Daily full + hourly differential",
        "recovery_time_objective": "4 hours",
        "recovery_point_objective": "1 hour"
    }
}

# Table-specific metadata (for the 11 scanned SQL tables)
TABLE_DETAILS = {
    "patients": {
        "description": "Master patient table med demografi och Swedish personnummer",
        "pii_columns": ["patient_id", "personnummer", "name", "address"],
        "sensitivity": "Very High",
        "row_count_estimate": "~10000"
    },
    "encounters": {
        "description": "Vårdtillfällen och sjukvårdskontakter",
        "pii_columns": ["patient_id"],
        "sensitivity": "High",
        "row_count_estimate": "~50000"
    },
    "diagnoses": {
        "description": "ICD-10 diagnoser kopplade till vårdtillfällen",
        "pii_columns": ["patient_id"],
        "sensitive_columns": ["diagnosis_code", "diagnosis_description"],
        "sensitivity": "High",
        "row_count_estimate": "~80000"
    },
    "medications": {
        "description": "ATC-kodade läkemedelsordinationer och förskrivningar",
        "pii_columns": ["patient_id"],
        "sensitive_columns": ["atc_code", "medication_name", "dosage"],
        "sensitivity": "High",
        "row_count_estimate": "~120000"
    },
    "lab_results": {
        "description": "LOINC-kodade laboratorieprovresultat",
        "pii_columns": ["patient_id"],
        "sensitive_columns": ["loinc_code", "test_result"],
        "sensitivity": "Medium",
        "row_count_estimate": "~200000"
    },
    "observations": {
        "description": "Vitala parametrar och kliniska observationer",
        "pii_columns": ["patient_id"],
        "sensitivity": "Medium",
        "row_count_estimate": "~150000"
    },
    "dicom_studies": {
        "description": "Metadata för radiologiska DICOM-studier",
        "pii_columns": ["patient_id"],
        "sensitive_columns": ["study_instance_uid", "accession_number"],
        "sensitivity": "High",
        "row_count_estimate": "~5000"
    },
    "dicom_series": {
        "description": "DICOM series metadata",
        "sensitive_columns": ["series_instance_uid"],
        "sensitivity": "Medium",
        "row_count_estimate": "~15000"
    },
    "practitioners": {
        "description": "Vårdpersonal och läkare",
        "pii_columns": ["practitioner_id", "name"],
        "sensitivity": "Medium",
        "row_count_estimate": "~500"
    },
    "organizations": {
        "description": "Vårdenheter och organisationsdata",
        "sensitivity": "Low",
        "row_count_estimate": "~50"
    },
    "codebooks": {
        "description": "Kodverk för ICD-10, ATC, LOINC mappning",
        "sensitivity": "Low",
        "row_count_estimate": "~100000"
    }
}

# ══════════════════════════════════════════════════════════
# AUTHENTICATION
# ══════════════════════════════════════════════════════════
print("🔐 Authenticating with Azure...")
credential = AzureCliCredential(process_timeout=30)
token = credential.get_token("https://purview.azure.net/.default").token
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ══════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ══════════════════════════════════════════════════════════
def sep(title=""):
    """Print section separator"""
    print(f"\n{'═'*70}")
    if title:
        print(f"  {title}")
        print('═'*70)

def find_sql_database_entity():
    """Find SQL database entity GUID"""
    try:
        body = {
            "keywords": SQL_DATABASE,
            "limit": 20,
            "filter": {"or": [
                {"entityType": "azure_sql_db"},
                {"entityType": "azure_sql_database"}
            ]}
        }
        
        r = requests.post(
            f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
            headers=headers,
            json=body,
            timeout=30
        )
        
        if r.status_code == 200:
            results = r.json().get("value", [])
            for entity in results:
                entity_name = entity.get("name", "")
                if SQL_DATABASE in entity_name or SQL_SERVER in entity_name:
                    return entity.get("id")
        
        return None
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def find_sql_tables():
    """Find all SQL table entities"""
    try:
        tables = {}
        
        for table_name in TABLE_DETAILS.keys():
            body = {
                "keywords": table_name,
                "limit": 20,
                "filter": {"or": [
                    {"entityType": "azure_sql_table"},
                    {"entityType": "sql_table"}
                ]}
            }
            
            r = requests.post(
                f"{PURVIEW_ENDPOINT}/catalog/api/search/query?api-version=2022-08-01-preview",
                headers=headers,
                json=body,
                timeout=30
            )
            
            if r.status_code == 200:
                results = r.json().get("value", [])
                for entity in results:
                    entity_name = entity.get("name", "")
                    if table_name == entity_name or table_name in entity_name.lower():
                        tables[table_name] = entity.get("id")
                        break
        
        return tables
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return {}

def update_database_metadata(entity_guid):
    """Update SQL database entity with Critical Elements and OKRs"""
    try:
        # Get existing entity
        r = requests.get(
            f"{ATLAS_API}/entity/guid/{entity_guid}",
            headers=headers,
            timeout=30
        )
        
        if r.status_code != 200:
            print(f"   ⚠️  Could not fetch entity (HTTP {r.status_code})")
            return False
        
        entity = r.json().get("entity", {})
        attributes = entity.get("attributes", {})
        
        # Add business metadata
        attributes["userDescription"] = DATABASE_DETAILS["description"]
        attributes["owner"] = DATABASE_DETAILS["owner"]
        attributes["steward"] = DATABASE_DETAILS["steward"]
        attributes["dataQualityContact"] = DATABASE_DETAILS["data_quality_contact"]
        attributes["criticalElements"] = str(DATABASE_DETAILS["critical_elements"])
        attributes["okrs"] = str(DATABASE_DETAILS["okrs"])
        attributes["slaAvailability"] = DATABASE_DETAILS["sla"]["availability"]
        attributes["slaBackupFrequency"] = DATABASE_DETAILS["sla"]["backup_frequency"]
        attributes["slaRTO"] = DATABASE_DETAILS["sla"]["recovery_time_objective"]
        attributes["slaRPO"] = DATABASE_DETAILS["sla"]["recovery_point_objective"]
        
        entity["attributes"] = attributes
        
        # Update entity
        r2 = requests.put(
            f"{ATLAS_API}/entity",
            headers=headers,
            json={"entity": entity},
            timeout=30
        )
        
        if r2.status_code in [200, 201]:
            print(f"   ✅ Updated database metadata")
            print(f"      - {len(DATABASE_DETAILS['critical_elements'])} critical elements")
            print(f"      - {len(DATABASE_DETAILS['okrs'])} OKRs")
            return True
        else:
            print(f"   ⚠️  Update failed (HTTP {r2.status_code})")
            return False
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def update_table_metadata(table_name, table_guid, details):
    """Update SQL table entity with sensitivity and metadata"""
    try:
        # Get existing entity
        r = requests.get(
            f"{ATLAS_API}/entity/guid/{table_guid}",
            headers=headers,
            timeout=30
        )
        
        if r.status_code != 200:
            return False
        
        entity = r.json().get("entity", {})
        attributes = entity.get("attributes", {})
        
        # Add table-specific metadata
        attributes["userDescription"] = details.get("description", "")
        attributes["sensitivity"] = details.get("sensitivity", "Medium")
        attributes["piiColumns"] = ", ".join(details.get("pii_columns", []))
        attributes["sensitiveColumns"] = ", ".join(details.get("sensitive_columns", []))
        attributes["estimatedRowCount"] = details.get("row_count_estimate", "Unknown")
        
        entity["attributes"] = attributes
        
        # Update entity
        r2 = requests.put(
            f"{ATLAS_API}/entity",
            headers=headers,
            json={"entity": entity},
            timeout=30
        )
        
        return r2.status_code in [200, 201]
        
    except Exception as e:
        return False

# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def main():
    sep("🗄️  POPULATING SQL DATABASE DETAILS")
    
    # Step 1: Find and update database
    sep("Database-Level Metadata")
    print("🔍 Finding SQL database entity...")
    db_guid = find_sql_database_entity()
    
    if db_guid:
        print(f"   ✅ Found database GUID: {db_guid}")
        print("📝 Updating database metadata...")
        db_success = update_database_metadata(db_guid)
    else:
        print(f"   ⚠️  Could not find database entity for '{SQL_DATABASE}'")
        print(f"   💡 Make sure SQL Server is scanned in Purview:")
        print(f"      Server: {SQL_SERVER}")
        print(f"      Database: {SQL_DATABASE}")
        db_success = False
    
    # Step 2: Find and update tables
    sep("Table-Level Metadata")
    print(f"🔍 Finding {len(TABLE_DETAILS)} SQL table entities...")
    tables = find_sql_tables()
    print(f"   ✅ Found {len(tables)}/{len(TABLE_DETAILS)} tables")
    
    success_count = 0
    fail_count = 0
    
    print(f"\n📝 Updating table metadata...")
    for table_name, details in TABLE_DETAILS.items():
        if table_name in tables:
            table_guid = tables[table_name]
            if update_table_metadata(table_name, table_guid, details):
                print(f"   ✅ {table_name} (sensitivity: {details['sensitivity']})")
                success_count += 1
            else:
                print(f"   ⚠️  {table_name} (update failed)")
                fail_count += 1
        else:
            print(f"   ⚠️  {table_name} (not found in Purview)")
            fail_count += 1
        
        time.sleep(0.5)  # Rate limiting
    
    sep("SUMMARY")
    print(f"Database: {'✅ Updated' if db_success else '❌ Failed'}")
    print(f"Tables:   ✅ {success_count} updated, ⚠️  {fail_count} failed")
    
    if db_success or success_count > 0:
        print(f"\n📊 View in Purview Portal:")
        print(f"   https://web.purview.azure.com/resource/{PURVIEW_ACCOUNT}")
        print(f"   → Data Catalog → Browse → Azure SQL Database")
        print(f"\n✅ SQL database now has:")
        print(f"   - Business description and ownership")
        print(f"   - 5 critical data elements (PII, diagnosis, medication, lab, DICOM)")
        print(f"   - 3 OKRs (availability, data quality, security)")
        print(f"   - SLA commitments (99.9% uptime, backup strategy, RTO/RPO)")
        print(f"\n✅ Tables now have:")
        print(f"   - Sensitivity levels (Very High/High/Medium/Low)")
        print(f"   - PII column identification")
        print(f"   - Sensitive column identification")
        print(f"   - Row count estimates")
    
    return 0 if (db_success and fail_count == 0) else 1

if __name__ == "__main__":
    try:
        exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
