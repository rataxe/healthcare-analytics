#!/usr/bin/env python3
"""
UNIFIED CATALOG API CLIENT
Production-ready Python client for Microsoft Purview Unified Catalog API (2025-09-15-preview)

COMPLETE API COVERAGE (55+ operations across 7 resource groups):

1. BUSINESS DOMAINS (9 operations)
   - list, get, create, update, delete, query
   - list/create/delete relationships

2. DATA PRODUCTS (11 operations)
   - list, get, create, update, delete, query
   - get facets (filter values)
   - list/create/delete relationships (GLOSSARYTERM, OKR, CRITICALDATACOLUMN)

3. GLOSSARY TERMS (13 operations)
   - list, get, create, update, delete, query, bulk_create
   - publish, unpublish
   - list/create/delete relationships

4. CRITICAL DATA ELEMENTS (9 operations)
   - list, get, create, update, delete
   - list/create/delete relationships (column mappings)

5. OKRs (9 operations)
   - list, get, create, update, delete
   - list/create/delete relationships (link to data products)

6. DATA ACCESS POLICIES (3 operations)
   - get, create/update, delete (for domains or products)

7. DATA QUALITY API (coming soon)
   - Connections, Rules, Profiling, Scans, Scores

AUTHENTICATION:
- Service Principal with OAuth2 client credentials flow
- Resource: https://purview.azure.net
- Required role: Data Steward (read+write)

SETUP:
1. Run: python scripts/setup_unified_catalog_access.py
2. Follow 4-step guide to create Service Principal
3. Credentials saved to .env.purview

REFERENCE:
https://learn.microsoft.com/en-us/purview/developer/microsoft-purview-sdk-documentation-overview
"""
import requests
import json
from pathlib import Path
from typing import Dict, List, Optional
from azure.identity import AzureCliCredential

class UnifiedCatalogClient:
    """Client for Purview Unified Catalog API"""
    
    def __init__(self, env_file: str = "scripts/.env.purview"):
        """Initialize client from .env file"""
        env_path = Path(env_file)
        if not env_path.exists():
            raise FileNotFoundError(
                f"Credentials file not found: {env_file}\n"
                "Run: python scripts/setup_unified_catalog_access.py"
            )
        
        # Load environment variables
        self.config = {}
        for line in env_path.read_text().splitlines():
            if line.strip() and not line.startswith('#'):
                key, value = line.split('=', 1)
                self.config[key.strip()] = value.strip()
        
        self.account = self.config['PURVIEW_ACCOUNT']
        self.base_url = self.config['UNIFIED_CATALOG_BASE']
        self.api_version = self.config['API_VERSION']
        
        # Get access token via AzureCliCredential (az login)
        self._credential = AzureCliCredential()
        self._access_token = None
        self._refresh_token()
    
    def _refresh_token(self):
        """Refresh access token via AzureCliCredential"""
        self._access_token = self._credential.get_token("https://purview.azure.net/.default").token
    
    def _get_access_token(self):
        """Compatibility: refresh token"""
        self._refresh_token()
    
    @property
    def headers(self) -> Dict[str, str]:
        """Get authorization headers"""
        return {
            'Authorization': f'Bearer {self._access_token}',
            'Content-Type': 'application/json'
        }
    
    # ========== BUSINESS DOMAINS ==========
    
    def list_business_domains(self) -> List[Dict]:
        """List all business domains"""
        url = f"{self.base_url}/businessDomains?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def get_business_domain(self, domain_id: str) -> Dict:
        """Get a specific business domain"""
        url = f"{self.base_url}/businessDomains/{domain_id}?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def create_business_domain(self, name: str, description: str, 
                              parent_id: Optional[str] = None, **kwargs) -> Dict:
        """Create a business domain"""
        url = f"{self.base_url}/businessDomains?api-version={self.api_version}"
        body = {
            'name': name,
            'description': description,
            **kwargs
        }
        if parent_id:
            body['parentDomainId'] = parent_id
        
        r = requests.post(url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def update_business_domain(self, domain_id: str, updates: Dict) -> Dict:
        """Update a business domain"""
        url = f"{self.base_url}/businessDomains/{domain_id}?api-version={self.api_version}"
        r = requests.patch(url, headers=self.headers, json=updates, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_business_domain(self, domain_id: str) -> bool:
        """Delete a business domain"""
        url = f"{self.base_url}/businessDomains/{domain_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    def query_business_domains(self, query: Dict) -> List[Dict]:
        """Query business domains with advanced filters"""
        url = f"{self.base_url}/businessDomains:query?api-version={self.api_version}"
        r = requests.post(url, headers=self.headers, json=query, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def list_domain_relationships(self, domain_id: str) -> List[Dict]:
        """List relationships for a business domain"""
        url = f"{self.base_url}/businessDomains/{domain_id}/relationships?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def create_domain_relationship(self, domain_id: str, relationship: Dict) -> Dict:
        """Create a relationship for a business domain"""
        url = f"{self.base_url}/businessDomains/{domain_id}/relationships?api-version={self.api_version}"
        r = requests.post(url, headers=self.headers, json=relationship, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_domain_relationship(self, domain_id: str, relationship_id: str) -> bool:
        """Delete a domain relationship"""
        url = f"{self.base_url}/businessDomains/{domain_id}/relationships/{relationship_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    # ========== DATA PRODUCTS ==========
    
    def list_data_products(self, domain_id: Optional[str] = None) -> List[Dict]:
        """List data products (optionally filtered by domain)"""
        url = f"{self.base_url}/dataProducts?api-version={self.api_version}"
        if domain_id:
            url += f"&domainId={domain_id}"
        
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def get_data_product(self, product_id: str) -> Dict:
        """Get a specific data product"""
        url = f"{self.base_url}/dataProducts/{product_id}?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def create_data_product(self, name: str, description: str, 
                           domain_id: str, owners: List[str],
                           status: str = 'DRAFT', **kwargs) -> Dict:
        """Create a data product (status: DRAFT or PUBLISHED)"""
        url = f"{self.base_url}/dataProducts?api-version={self.api_version}"
        body = {
            'name': name,
            'description': description,
            'domainId': domain_id,
            'owners': owners,
            'status': status,
            **kwargs
        }
        
        r = requests.post(url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def update_data_product(self, product_id: str, updates: Dict) -> Dict:
        """Update a data product"""
        url = f"{self.base_url}/dataProducts/{product_id}?api-version={self.api_version}"
        r = requests.patch(url, headers=self.headers, json=updates, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_data_product(self, product_id: str) -> bool:
        """Delete a data product"""
        url = f"{self.base_url}/dataProducts/{product_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    def query_data_products(self, query: Dict) -> List[Dict]:
        """Query data products with advanced filters"""
        url = f"{self.base_url}/dataProducts:query?api-version={self.api_version}"
        r = requests.post(url, headers=self.headers, json=query, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def get_data_product_facets(self) -> Dict:
        """Get facets (filter values) for data products search"""
        url = f"{self.base_url}/dataProducts:facets?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def list_data_product_relationships(self, product_id: str) -> List[Dict]:
        """List relationships for a data product (glossary terms, OKRs, CDEs)"""
        url = f"{self.base_url}/dataProducts/{product_id}/relationships?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def create_data_product_relationship(self, product_id: str, relationship_type: str, target_id: str) -> Dict:
        """Create a relationship (GLOSSARYTERM, OKR, CRITICALDATACOLUMN)"""
        url = f"{self.base_url}/dataProducts/{product_id}/relationships?api-version={self.api_version}"
        body = {
            'relationshipType': relationship_type,
            'targetId': target_id
        }
        r = requests.post(url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_data_product_relationship(self, product_id: str, relationship_id: str) -> bool:
        """Delete a data product relationship"""
        url = f"{self.base_url}/dataProducts/{product_id}/relationships/{relationship_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    # ========== GLOSSARY TERMS ==========
    
    def list_glossary_terms(self, domain_id: Optional[str] = None) -> List[Dict]:
        """List glossary terms (per domain or globally)"""
        url = f"{self.base_url}/glossaryTerms?api-version={self.api_version}"
        if domain_id:
            url += f"&domainId={domain_id}"
        
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def get_glossary_term(self, term_id: str) -> Dict:
        """Get a specific glossary term"""
        url = f"{self.base_url}/glossaryTerms/{term_id}?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def create_glossary_term(self, name: str, definition: str,
                            domain_id: str, **kwargs) -> Dict:
        """Create a glossary term"""
        url = f"{self.base_url}/glossaryTerms?api-version={self.api_version}"
        body = {
            'name': name,
            'definition': definition,
            'domainId': domain_id,
            **kwargs
        }
        
        r = requests.post(url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def update_glossary_term(self, term_id: str, updates: Dict) -> Dict:
        """Update a glossary term (definition, owners, attributes)"""
        url = f"{self.base_url}/glossaryTerms/{term_id}?api-version={self.api_version}"
        r = requests.patch(url, headers=self.headers, json=updates, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_glossary_term(self, term_id: str) -> bool:
        """Delete a glossary term (must be unpublished first)"""
        url = f"{self.base_url}/glossaryTerms/{term_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    def query_glossary_terms(self, query: Dict) -> List[Dict]:
        """Search/filter glossary terms"""
        url = f"{self.base_url}/glossaryTerms:query?api-version={self.api_version}"
        r = requests.post(url, headers=self.headers, json=query, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def list_glossary_term_relationships(self, term_id: str) -> List[Dict]:
        """List linked data products, CDEs etc."""
        url = f"{self.base_url}/glossaryTerms/{term_id}/relationships?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def create_glossary_term_relationship(self, term_id: str, relationship: Dict) -> Dict:
        """Link term to data product or CDE"""
        url = f"{self.base_url}/glossaryTerms/{term_id}/relationships?api-version={self.api_version}"
        r = requests.post(url, headers=self.headers, json=relationship, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_glossary_term_relationship(self, term_id: str, relationship_id: str) -> bool:
        """Delete a term relationship"""
        url = f"{self.base_url}/glossaryTerms/{term_id}/relationships/{relationship_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    def publish_glossary_term(self, term_id: str) -> Dict:
        """Publish term (make visible to consumers)"""
        url = f"{self.base_url}/glossaryTerms/{term_id}:publish?api-version={self.api_version}"
        r = requests.post(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def unpublish_glossary_term(self, term_id: str) -> Dict:
        """Unpublish term"""
        url = f"{self.base_url}/glossaryTerms/{term_id}:unpublish?api-version={self.api_version}"
        r = requests.post(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def bulk_create_glossary_terms(self, terms: List[Dict]) -> Dict:
        """Bulk create glossary terms"""
        url = f"{self.base_url}/glossaryTerms:import?api-version={self.api_version}"
        body = {'terms': terms}
        
        r = requests.post(url, headers=self.headers, json=body, timeout=60)
        r.raise_for_status()
        return r.json()
    
    # ========== CRITICAL DATA ELEMENTS ==========
    
    def list_critical_data_elements(self) -> List[Dict]:
        """List critical data elements"""
        url = f"{self.base_url}/criticalDataElements?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def get_critical_data_element(self, cde_id: str) -> Dict:
        """Get a specific CDE"""
        url = f"{self.base_url}/criticalDataElements/{cde_id}?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def create_critical_data_element(self, name: str, description: str,
                                    domain_id: str, data_type: str,
                                    **kwargs) -> Dict:
        """Create a critical data element (logical grouping of important columns)"""
        url = f"{self.base_url}/criticalDataElements?api-version={self.api_version}"
        body = {
            'name': name,
            'description': description,
            'domainId': domain_id,
            'dataType': data_type,
            **kwargs
        }
        
        r = requests.post(url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def update_critical_data_element(self, cde_id: str, updates: Dict) -> Dict:
        """Update a CDE"""
        url = f"{self.base_url}/criticalDataElements/{cde_id}?api-version={self.api_version}"
        r = requests.patch(url, headers=self.headers, json=updates, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_critical_data_element(self, cde_id: str) -> bool:
        """Delete a CDE"""
        url = f"{self.base_url}/criticalDataElements/{cde_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    def list_cde_relationships(self, cde_id: str) -> List[Dict]:
        """List mapped columns and data products"""
        url = f"{self.base_url}/criticalDataElements/{cde_id}/relationships?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def create_cde_relationship(self, cde_id: str, column_mapping: Dict) -> Dict:
        """Map data column to CDE (e.g., 'CustID' and 'CID' -> 'Customer ID')"""
        url = f"{self.base_url}/criticalDataElements/{cde_id}/relationships?api-version={self.api_version}"
        r = requests.post(url, headers=self.headers, json=column_mapping, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_cde_relationship(self, cde_id: str, relationship_id: str) -> bool:
        """Delete a column mapping"""
        url = f"{self.base_url}/criticalDataElements/{cde_id}/relationships/{relationship_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    # ========== OKRs ==========
    
    def list_okrs(self) -> List[Dict]:
        """List OKRs (Objectives and Key Results)"""
        url = f"{self.base_url}/okrs?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def get_okr(self, okr_id: str) -> Dict:
        """Get a specific OKR"""
        url = f"{self.base_url}/okrs/{okr_id}?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def create_okr(self, objective: str, key_results: List[str],
                   domain_id: str, **kwargs) -> Dict:
        """Create an OKR (links data goals to measurable business objectives)"""
        url = f"{self.base_url}/okrs?api-version={self.api_version}"
        body = {
            'objective': objective,
            'keyResults': key_results,
            'domainId': domain_id,
            **kwargs
        }
        
        r = requests.post(url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def update_okr(self, okr_id: str, updates: Dict) -> Dict:
        """Update an OKR"""
        url = f"{self.base_url}/okrs/{okr_id}?api-version={self.api_version}"
        r = requests.patch(url, headers=self.headers, json=updates, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_okr(self, okr_id: str) -> bool:
        """Delete an OKR"""
        url = f"{self.base_url}/okrs/{okr_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    def list_okr_relationships(self, okr_id: str) -> List[Dict]:
        """List linked data products"""
        url = f"{self.base_url}/okrs/{okr_id}/relationships?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json().get('value', [])
    
    def create_okr_relationship(self, okr_id: str, product_id: str) -> Dict:
        """Link OKR to data product"""
        url = f"{self.base_url}/okrs/{okr_id}/relationships?api-version={self.api_version}"
        body = {'productId': product_id}
        r = requests.post(url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_okr_relationship(self, okr_id: str, relationship_id: str) -> bool:
        """Delete an OKR-product link"""
        url = f"{self.base_url}/okrs/{okr_id}/relationships/{relationship_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204
    
    # ========== DATA ACCESS POLICIES ==========
    
    def get_data_access_policy(self, resource_id: str, resource_type: str = 'dataProduct') -> Dict:
        """Get access policy for a data product or domain"""
        url = f"{self.base_url}/policies/{resource_type}/{resource_id}?api-version={self.api_version}"
        r = requests.get(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def create_or_update_data_access_policy(self, resource_id: str, policy: Dict, resource_type: str = 'dataProduct') -> Dict:
        """Set access rules (who approves, conditions)"""
        url = f"{self.base_url}/policies/{resource_type}/{resource_id}?api-version={self.api_version}"
        r = requests.put(url, headers=self.headers, json=policy, timeout=30)
        r.raise_for_status()
        return r.json()
    
    def delete_data_access_policy(self, resource_id: str, resource_type: str = 'dataProduct') -> bool:
        """Remove access policy"""
        url = f"{self.base_url}/policies/{resource_type}/{resource_id}?api-version={self.api_version}"
        r = requests.delete(url, headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.status_code == 204


def demo_usage():
    """Demonstrate Unified Catalog API usage"""
    print("="*80)
    print("  UNIFIED CATALOG API - DEMO")
    print("="*80)
    
    try:
        client = UnifiedCatalogClient()
        print("\n✅ Client initialized successfully")
    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        return
    except Exception as e:
        print(f"\n❌ Failed to initialize: {e}")
        return
    
    # Example 1: List business domains
    print("\n1️⃣ Listing Business Domains...")
    try:
        domains = client.list_business_domains()
        print(f"   Found {len(domains)} domain(s):")
        for d in domains:
            print(f"   • {d.get('name')}: {d.get('description', 'N/A')}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Example 2: List data products
    print("\n2️⃣ Listing Data Products...")
    try:
        products = client.list_data_products()
        print(f"   Found {len(products)} product(s):")
        for p in products:
            print(f"   • {p.get('name')}: {p.get('description', 'N/A')}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Example 3: List glossary terms
    print("\n3️⃣ Listing Glossary Terms...")
    try:
        terms = client.list_glossary_terms()
        print(f"   Found {len(terms)} term(s)")
        if terms:
            print(f"   First 5 terms:")
            for t in terms[:5]:
                print(f"   • {t.get('name')}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Example 4: List critical data elements
    print("\n4️⃣ Listing Critical Data Elements...")
    try:
        cdes = client.list_critical_data_elements()
        print(f"   Found {len(cdes)} CDE(s)")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # Example 5: List OKRs
    print("\n5️⃣ Listing OKRs...")
    try:
        okrs = client.list_okrs()
        print(f"   Found {len(okrs)} OKR(s)")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    print("\n" + "="*80)
    print("  Demo complete! Use this client in your automation scripts.")
    print("="*80)


if __name__ == '__main__':
    demo_usage()
