#!/usr/bin/env python3
"""
Data Quality API Implementation for Unified Catalog

Implements all Data Quality operations:
- Connections (4 methods)
- Rules (5 methods)
- Profiling (2 methods)
- Scans (3 methods)
- Scores (1 method)

Reference: https://learn.microsoft.com/en-us/rest/api/purview/datagovernance/data-quality

USAGE:
    from unified_catalog_data_quality import DataQualityClient
    
    client = DataQualityClient()
    
    # Create connection
    conn = client.create_dq_connection(
        name="SQL_Connection",
        source_type="AzureSqlDatabase",
        connection_details={...}
    )
    
    # Create rule
    rule = client.create_dq_rule(
        name="Null_Check",
        rule_type="COMPLETENESS",
        logic="column IS NOT NULL"
    )
"""
import os
import requests
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv


class DataQualityClient:
    """Client for Purview Data Quality API"""
    
    def __init__(self, env_file: str = "scripts/.env.purview"):
        """Initialize client with credentials from Key Vault or .env file"""
        # Try to load from .env first
        env_path = Path(env_file)
        if env_path.exists():
            load_dotenv(env_path)
            self.tenant_id = os.getenv('PURVIEW_TENANT_ID')
            self.client_id = os.getenv('PURVIEW_CLIENT_ID')
            self.client_secret = os.getenv('PURVIEW_CLIENT_SECRET')
            self.account_url = os.getenv('PURVIEW_ACCOUNT', 'https://prviewacc.purview.azure.com')
        else:
            # Try Key Vault
            try:
                from get_keyvault_secrets import get_purview_credentials
                creds = get_purview_credentials()
                self.tenant_id = creds['tenant_id']
                self.client_id = creds['client_id']
                self.client_secret = creds['client_secret']
                self.account_url = creds['account_url']
            except:
                raise RuntimeError(
                    "Credentials not found. Either:\n"
                    "1. Create scripts/.env.purview with credentials\n"
                    "2. Or run: python scripts/setup_keyvault_credentials.py"
                )
        
        self.api_version = "2025-09-15-preview"
        self.base_url = f"{self.account_url}/datagovernance/catalog"
        self._token = None
    
    def _get_access_token(self) -> str:
        """Get OAuth2 access token"""
        if self._token:
            return self._token
        
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'resource': 'https://purview.azure.net'
        }
        
        response = requests.post(token_url, data=data, timeout=30)
        response.raise_for_status()
        self._token = response.json()['access_token']
        return self._token
    
    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make authenticated request"""
        token = self._get_access_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        url = f"{self.base_url}/{endpoint}?api-version={self.api_version}"
        response = requests.request(method, url, headers=headers, timeout=30, **kwargs)
        return response
    
    # ========== CONNECTIONS (4 METHODS) ==========
    
    def create_dq_connection(
        self,
        name: str,
        source_type: str,
        connection_details: Dict[str, Any],
        description: Optional[str] = None,
        **kwargs
    ) -> Dict:
        """
        Create data quality connection
        
        Args:
            name: Connection name
            source_type: Type (AzureSqlDatabase, AzureBlobStorage, etc.)
            connection_details: Connection configuration
            description: Optional description
        
        Returns:
            Created connection object
        
        Example:
            conn = client.create_dq_connection(
                name="SQL_Prod",
                source_type="AzureSqlDatabase",
                connection_details={
                    "server": "myserver.database.windows.net",
                    "database": "mydb",
                    "credential": "sql-admin-password"  # From Key Vault
                }
            )
        """
        payload = {
            "name": name,
            "sourceType": source_type,
            "connectionDetails": connection_details,
            "description": description,
            **kwargs
        }
        
        response = self._request('POST', 'dataQuality/connections', json=payload)
        response.raise_for_status()
        return response.json()
    
    def list_dq_connections(self, source_type: Optional[str] = None) -> List[Dict]:
        """
        List all data quality connections
        
        Args:
            source_type: Filter by source type (optional)
        
        Returns:
            List of connection objects
        """
        endpoint = 'dataQuality/connections'
        if source_type:
            endpoint += f"&sourceType={source_type}"
        
        response = self._request('GET', endpoint)
        response.raise_for_status()
        return response.json().get('value', [])
    
    def get_dq_connection(self, connection_id: str) -> Dict:
        """
        Get data quality connection by ID
        
        Args:
            connection_id: Connection ID
        
        Returns:
            Connection object
        """
        response = self._request('GET', f'dataQuality/connections/{connection_id}')
        response.raise_for_status()
        return response.json()
    
    def delete_dq_connection(self, connection_id: str) -> bool:
        """
        Delete data quality connection
        
        Args:
            connection_id: Connection ID
        
        Returns:
            True if deleted successfully
        """
        response = self._request('DELETE', f'dataQuality/connections/{connection_id}')
        return response.status_code in [200, 204]
    
    # ========== RULES (5 METHODS) ==========
    
    def create_dq_rule(
        self,
        name: str,
        rule_type: str,
        logic: str,
        connection_id: str,
        target_asset: str,
        description: Optional[str] = None,
        severity: str = "HIGH",
        **kwargs
    ) -> Dict:
        """
        Create data quality rule
        
        Args:
            name: Rule name
            rule_type: Type (COMPLETENESS, ACCURACY, CONSISTENCY, VALIDITY, UNIQUENESS, TIMELINESS)
            logic: Rule logic/expression
            connection_id: Data source connection ID
            target_asset: Asset to apply rule to
            description: Optional description
            severity: Severity (HIGH, MEDIUM, LOW)
        
        Returns:
            Created rule object
        
        Example:
            rule = client.create_dq_rule(
                name="Patient_ID_Not_Null",
                rule_type="COMPLETENESS",
                logic="patient_id IS NOT NULL",
                connection_id="conn-123",
                target_asset="dbo.patients",
                severity="HIGH"
            )
        """
        payload = {
            "name": name,
            "ruleType": rule_type,
            "logic": logic,
            "connectionId": connection_id,
            "targetAsset": target_asset,
            "description": description,
            "severity": severity,
            **kwargs
        }
        
        response = self._request('POST', 'dataQuality/rules', json=payload)
        response.raise_for_status()
        return response.json()
    
    def list_dq_rules(
        self,
        connection_id: Optional[str] = None,
        rule_type: Optional[str] = None
    ) -> List[Dict]:
        """
        List all data quality rules
        
        Args:
            connection_id: Filter by connection (optional)
            rule_type: Filter by rule type (optional)
        
        Returns:
            List of rule objects
        """
        endpoint = 'dataQuality/rules'
        filters = []
        if connection_id:
            filters.append(f"connectionId={connection_id}")
        if rule_type:
            filters.append(f"ruleType={rule_type}")
        
        if filters:
            endpoint += "&" + "&".join(filters)
        
        response = self._request('GET', endpoint)
        response.raise_for_status()
        return response.json().get('value', [])
    
    def get_dq_rule(self, rule_id: str) -> Dict:
        """Get data quality rule by ID"""
        response = self._request('GET', f'dataQuality/rules/{rule_id}')
        response.raise_for_status()
        return response.json()
    
    def update_dq_rule(self, rule_id: str, updates: Dict) -> Dict:
        """
        Update data quality rule
        
        Args:
            rule_id: Rule ID
            updates: Fields to update
        
        Returns:
            Updated rule object
        """
        response = self._request('PATCH', f'dataQuality/rules/{rule_id}', json=updates)
        response.raise_for_status()
        return response.json()
    
    def delete_dq_rule(self, rule_id: str) -> bool:
        """Delete data quality rule"""
        response = self._request('DELETE', f'dataQuality/rules/{rule_id}')
        return response.status_code in [200, 204]
    
    # ========== PROFILING (2 METHODS) ==========
    
    def run_data_profiling(
        self,
        asset_id: str,
        connection_id: str,
        profile_type: str = "FULL",
        columns: Optional[List[str]] = None
    ) -> Dict:
        """
        Run data profiling on an asset
        
        Args:
            asset_id: Asset to profile
            connection_id: Data source connection
            profile_type: FULL or SAMPLE
            columns: Specific columns (optional, None = all)
        
        Returns:
            Profiling job object with job_id
        
        Example:
            job = client.run_data_profiling(
                asset_id="asset-123",
                connection_id="conn-123",
                profile_type="FULL"
            )
            # Later, get results:
            results = client.get_profiling_results(job['jobId'])
        """
        payload = {
            "assetId": asset_id,
            "connectionId": connection_id,
            "profileType": profile_type,
            "columns": columns
        }
        
        response = self._request('POST', 'dataQuality/profiling', json=payload)
        response.raise_for_status()
        return response.json()
    
    def get_profiling_results(self, profiling_id: str) -> Dict:
        """
        Get data profiling results
        
        Args:
            profiling_id: Profiling job ID (from run_data_profiling)
        
        Returns:
            Profiling results with statistics
        """
        response = self._request('GET', f'dataQuality/profiling/{profiling_id}')
        response.raise_for_status()
        return response.json()
    
    # ========== SCANS (3 METHODS) ==========
    
    def schedule_quality_scan(
        self,
        name: str,
        connection_id: str,
        rule_ids: List[str],
        schedule: Dict[str, Any],
        **kwargs
    ) -> Dict:
        """
        Schedule recurring quality scan
        
        Args:
            name: Scan name
            connection_id: Data source connection
            rule_ids: List of rule IDs to run
            schedule: Schedule configuration (cron expression)
        
        Returns:
            Created scan schedule object
        
        Example:
            scan = client.schedule_quality_scan(
                name="Daily_Patient_Scan",
                connection_id="conn-123",
                rule_ids=["rule-1", "rule-2"],
                schedule={
                    "type": "CRON",
                    "expression": "0 2 * * *"  # Daily at 2 AM
                }
            )
        """
        payload = {
            "name": name,
            "connectionId": connection_id,
            "ruleIds": rule_ids,
            "schedule": schedule,
            **kwargs
        }
        
        response = self._request('POST', 'dataQuality/scans', json=payload)
        response.raise_for_status()
        return response.json()
    
    def run_quality_scan(self, scan_id: str) -> Dict:
        """
        Run quality scan immediately (ad-hoc)
        
        Args:
            scan_id: Scan ID
        
        Returns:
            Scan run object with run_id
        """
        response = self._request('POST', f'dataQuality/scans/{scan_id}/run')
        response.raise_for_status()
        return response.json()
    
    def get_scan_status(self, scan_id: str, run_id: Optional[str] = None) -> Dict:
        """
        Get quality scan status
        
        Args:
            scan_id: Scan ID
            run_id: Specific run ID (optional, None = latest)
        
        Returns:
            Scan status and results
        """
        endpoint = f'dataQuality/scans/{scan_id}'
        if run_id:
            endpoint += f'/runs/{run_id}'
        
        response = self._request('GET', endpoint)
        response.raise_for_status()
        return response.json()
    
    # ========== SCORES (1 METHOD) ==========
    
    def get_quality_scores(
        self,
        asset_id: str,
        dimension: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict:
        """
        Get data quality scores for an asset
        
        Args:
            asset_id: Asset ID
            dimension: Quality dimension (COMPLETENESS, ACCURACY, etc.) - optional
            start_date: Start date (ISO format) - optional
            end_date: End date (ISO format) - optional
        
        Returns:
            Quality scores and trends
        
        Example:
            scores = client.get_quality_scores(
                asset_id="asset-123",
                dimension="COMPLETENESS",
                start_date="2026-01-01",
                end_date="2026-04-22"
            )
        """
        endpoint = f'dataQuality/assets/{asset_id}/scores'
        filters = []
        if dimension:
            filters.append(f"dimension={dimension}")
        if start_date:
            filters.append(f"startDate={start_date}")
        if end_date:
            filters.append(f"endDate={end_date}")
        
        if filters:
            endpoint += "&" + "&".join(filters)
        
        response = self._request('GET', endpoint)
        response.raise_for_status()
        return response.json()


# ========== HELPER FUNCTIONS ==========

def example_usage():
    """Example usage of Data Quality Client"""
    print("="*80)
    print("  DATA QUALITY API - EXAMPLE USAGE")
    print("="*80)
    
    client = DataQualityClient()
    
    # 1. Create connection
    print("\n1. Creating SQL connection...")
    try:
        conn = client.create_dq_connection(
            name="Healthcare_SQL",
            source_type="AzureSqlDatabase",
            connection_details={
                "server": "myserver.database.windows.net",
                "database": "healthcare",
                "credentialReference": "sql-admin-password"  # From Key Vault
            },
            description="Production healthcare database"
        )
        print(f"✅ Connection created: {conn.get('id')}")
        conn_id = conn['id']
    except Exception as e:
        print(f"❌ Failed: {e}")
        return
    
    # 2. Create quality rules
    print("\n2. Creating quality rules...")
    rules = [
        {
            "name": "Patient_ID_Not_Null",
            "type": "COMPLETENESS",
            "logic": "patient_id IS NOT NULL",
            "asset": "dbo.patients"
        },
        {
            "name": "Valid_Birth_Date",
            "type": "VALIDITY",
            "logic": "birth_date <= GETDATE()",
            "asset": "dbo.patients"
        }
    ]
    
    rule_ids = []
    for rule_def in rules:
        try:
            rule = client.create_dq_rule(
                name=rule_def['name'],
                rule_type=rule_def['type'],
                logic=rule_def['logic'],
                connection_id=conn_id,
                target_asset=rule_def['asset'],
                severity="HIGH"
            )
            print(f"✅ Rule created: {rule['name']}")
            rule_ids.append(rule['id'])
        except Exception as e:
            print(f"❌ Failed to create rule {rule_def['name']}: {e}")
    
    # 3. Schedule scan
    print("\n3. Scheduling daily quality scan...")
    try:
        scan = client.schedule_quality_scan(
            name="Daily_Healthcare_Quality_Check",
            connection_id=conn_id,
            rule_ids=rule_ids,
            schedule={
                "type": "CRON",
                "expression": "0 2 * * *"  # Daily at 2 AM
            }
        )
        print(f"✅ Scan scheduled: {scan['name']}")
    except Exception as e:
        print(f"❌ Failed: {e}")
    
    print("\n✅ Data Quality setup complete!")


if __name__ == '__main__':
    example_usage()
