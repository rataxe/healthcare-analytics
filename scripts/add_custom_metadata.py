"""
Add custom metadata to existing Purview data products.

Defines a HealthcareGovernance business metadata definition in Atlas and
applies per-product metadata to the existing healthcare data products.

Usage:
  python scripts/add_custom_metadata.py
"""

import unicodedata

import requests
from azure.identity import AzureCliCredential


cred = AzureCliCredential(process_timeout=30)
ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
UNIFIED = f"{ACCT}/datagovernance/catalog"
API_VER = "2025-09-15-preview"

GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"

BM_DEF_NAME = "HealthcareGovernance"
APPLICABLE_ENTITY_TYPES = '["DataSet", "healthcare_data_product"]'

KNOWN_ATLAS_GUIDS = {
    "Klinisk Patientanalys": "e7010e17-8987-4c31-af29-b06fcf4b2142",
    "BrainChild Barncancerforskning": "f8fe756c-6987-41ac-ab90-451237b946d5",
    "OMOP Forskningsdata": "0a034311-74c8-4ac1-9893-99c3f4a88d4a",
    "ML Feature Store": "68956c65-361b-4c55-afad-3fa1b7d87167",
}


CUSTOM_METADATA: dict[str, dict[str, str]] = {
    "Klinisk Patientanalys": {
        "data_classification": "PHI",
        "gdpr_legal_basis": "Art. 9(2)(h) - vard och behandling",
        "data_standards": "FHIR R4, OMOP CDM v5.4",
        "sla_hours": "24",
        "update_frequency": "Daily",
        "retention_years": "10",
        "sensitivity_level": "High",
        "pii_present": "true",
        "data_steward": "Healthcare Analytics Team",
        "review_date": "2026-12-31",
    },
    "OMOP Forskningsdata": {
        "data_classification": "Pseudonymized",
        "gdpr_legal_basis": "Art. 9(2)(j) - forskning med pseudonymisering",
        "data_standards": "OMOP CDM v5.4, SNOMED CT, RxNorm",
        "sla_hours": "96",
        "update_frequency": "Weekly",
        "retention_years": "15",
        "sensitivity_level": "Medium",
        "pii_present": "false",
        "data_steward": "Research Data Team",
        "review_date": "2026-12-31",
    },
    "BrainChild Barncancerforskning": {
        "data_classification": "PHI",
        "gdpr_legal_basis": "Art. 9(2)(j) - medicinsk forskning, barncancer",
        "data_standards": "FHIR R4, DICOM, HL7 GMS, BTB, SBCR",
        "sla_hours": "24",
        "update_frequency": "Daily",
        "retention_years": "20",
        "sensitivity_level": "High",
        "pii_present": "true",
        "data_steward": "BrainChild Research Team",
        "review_date": "2026-12-31",
    },
    "ML Feature Store": {
        "data_classification": "Anonymized",
        "gdpr_legal_basis": "Art. 6(1)(f) - legitima intressen, anonymiserad",
        "data_standards": "Internal ML Feature Standard v1.0",
        "sla_hours": "1",
        "update_frequency": "OnDemand",
        "retention_years": "3",
        "sensitivity_level": "Low",
        "pii_present": "false",
        "data_steward": "ML Engineering Team",
        "review_date": "2026-12-31",
    },
    "Hälsosjukvård Datastyrning": {
        "data_classification": "Internal",
        "gdpr_legal_basis": "Art. 6(1)(c) - rattslig forpliktelse, GDPR-styrning",
        "data_standards": "ISO 27001, GDPR, Purview Data Catalog",
        "sla_hours": "4",
        "update_frequency": "Daily",
        "retention_years": "7",
        "sensitivity_level": "Medium",
        "pii_present": "false",
        "data_steward": "Data Governance Office",
        "review_date": "2026-12-31",
    },
}


ATTRIBUTE_DEFS = [
    {
        "name": "data_classification",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "50",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "PHI | Pseudonymized | Anonymized | Internal | Public",
    },
    {
        "name": "gdpr_legal_basis",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "500",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "GDPR Article reference and processing basis",
    },
    {
        "name": "data_standards",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "500",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "Technical standards: FHIR, OMOP, DICOM, HL7 etc.",
    },
    {
        "name": "sla_hours",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "50",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "Max acceptable data latency in hours",
    },
    {
        "name": "update_frequency",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "50",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "Daily | Weekly | OnDemand | Realtime",
    },
    {
        "name": "retention_years",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "50",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "Data retention requirement in years",
    },
    {
        "name": "sensitivity_level",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "50",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "High | Medium | Low",
    },
    {
        "name": "pii_present",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "50",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "true | false - whether PII/PHI is present",
    },
    {
        "name": "data_steward",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "200",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "Responsible data steward or team",
    },
    {
        "name": "review_date",
        "typeName": "string",
        "isOptional": True,
        "cardinality": "SINGLE",
        "options": {
            "maxStrLength": "50",
            "applicableEntityTypes": APPLICABLE_ENTITY_TYPES,
        },
        "description": "Next scheduled governance review date (YYYY-MM-DD)",
    },
]


def refresh_token() -> dict[str, str]:
    token = cred.get_token("https://purview.azure.net/.default").token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def ok(msg: str) -> None:
    print(f"  {GREEN}OK{RESET} {msg}")


def warn(msg: str) -> None:
    print(f"  {YELLOW}WARN{RESET} {msg}")


def info(msg: str) -> None:
    print(f"  {CYAN}INFO{RESET} {msg}")


def hdr(title: str) -> None:
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")


def canonical_name(name: str) -> str:
    return unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")


def metadata_for_product(name: str) -> dict[str, str] | None:
    if name in CUSTOM_METADATA:
        return CUSTOM_METADATA[name]
    return CUSTOM_METADATA.get(canonical_name(name))


def qualified_name_candidates(name: str) -> list[str]:
    raw = name.lower().replace(" ", "-")
    ascii_name = canonical_name(name).lower().replace(" ", "-")
    candidates = [f"dp://{raw}"]
    if ascii_name and ascii_name != raw:
        candidates.append(f"dp://{ascii_name}")
    return candidates


def ensure_bm_typedef(headers: dict[str, str]) -> bool:
    hdr(f"1. DEFINE CUSTOM METADATA GROUP - {BM_DEF_NAME}")

    response = requests.get(
        f"{ATLAS}/types/businessmetadatadef/name/{BM_DEF_NAME}",
        headers=headers,
        timeout=30,
    )

    typedef_payload = {
        "businessMetadataDefs": [
            {
                "category": "BUSINESS_METADATA",
                "name": BM_DEF_NAME,
                "typeVersion": "1.0",
                "description": (
                    "Healthcare-specific governance metadata for data products: "
                    "GDPR legal basis, data classification, sensitivity, SLA and standards."
                ),
                "attributeDefs": ATTRIBUTE_DEFS,
            }
        ]
    }

    if response.status_code == 200:
        current_def = response.json()
        current_attrs = current_def.get("attributeDefs", [])
        needs_update = any(
            attr.get("options", {}).get("applicableEntityTypes") != APPLICABLE_ENTITY_TYPES
            for attr in current_attrs
        ) or len(current_attrs) != len(ATTRIBUTE_DEFS)
        if not needs_update:
            ok(f"'{BM_DEF_NAME}' already defined")
            return True

        typedef_payload["businessMetadataDefs"][0]["guid"] = current_def.get("guid")
        typedef_payload["businessMetadataDefs"][0]["version"] = current_def.get("version", 1)
        update_response = requests.put(
            f"{ATLAS}/types/typedefs",
            headers=headers,
            json=typedef_payload,
            timeout=60,
        )
        if update_response.status_code in (200, 201):
            ok(f"Updated '{BM_DEF_NAME}' with applicable entity types")
            return True

        warn(f"typedef update failed: HTTP {update_response.status_code} - {update_response.text[:300]}")
        return False

    response = requests.post(
        f"{ATLAS}/types/typedefs",
        headers=headers,
        json=typedef_payload,
        timeout=60,
    )
    if response.status_code in (200, 201):
        ok(f"Created '{BM_DEF_NAME}' with {len(ATTRIBUTE_DEFS)} custom attributes")
        return True

    warn(f"typedef creation failed: HTTP {response.status_code} - {response.text[:300]}")
    return False


def lookup_atlas_guid(headers: dict[str, str], product_name: str) -> str | None:
    if product_name in KNOWN_ATLAS_GUIDS:
        return KNOWN_ATLAS_GUIDS[product_name]

    for qualified_name in qualified_name_candidates(product_name):
        response = requests.get(
            f"{ATLAS}/entity/uniqueAttribute/type/healthcare_data_product",
            params={"attr:qualifiedName": qualified_name},
            headers=headers,
            timeout=60,
        )
        if response.status_code == 200:
            entity = response.json().get("entity", {})
            guid = entity.get("guid")
            if guid:
                info(f"Resolved Atlas GUID for '{product_name}' via {qualified_name}")
                return guid

    response = requests.get(
        f"{ATLAS}/entity/uniqueAttribute/type/healthcare_data_product",
        params={"attr:name": product_name},
        headers=headers,
        timeout=60,
    )
    if response.status_code == 200:
        entity = response.json().get("entity", {})
        guid = entity.get("guid")
        if guid:
            info(f"Resolved Atlas GUID for '{product_name}' via attr:name")
            return guid

    return None


def create_atlas_entity(headers: dict[str, str], product: dict) -> str | None:
    product_name = product.get("name", "")
    contacts = product.get("contacts", {}).get("owner", [])
    owners = ", ".join(owner.get("id", "") for owner in contacts if owner.get("id"))
    entity_payload = {
        "entity": {
            "typeName": "healthcare_data_product",
            "attributes": {
                "qualifiedName": f"dp://{product_name.lower().replace(' ', '-')}",
                "name": product_name,
                "description": product.get("description", ""),
                "product_type": product.get("type", ""),
                "product_status": product.get("status", ""),
                "product_owners": owners,
                "sla": product.get("additionalProperties", {}).get("sla", ""),
                "use_cases": product.get("businessUse", ""),
                "tables": str(product.get("additionalProperties", {}).get("assetCount", "")),
            },
        }
    }

    response = requests.post(
        f"{ATLAS}/entity",
        headers=headers,
        json=entity_payload,
        timeout=60,
    )
    if response.status_code not in (200, 201):
        warn(f"{product_name} entity creation failed: HTTP {response.status_code} - {response.text[:200]}")
        return None

    info(f"Created Atlas entity for '{product_name}'")
    return lookup_atlas_guid(headers, product_name)


def apply_atlas_business_metadata(
    headers: dict[str, str], atlas_guid: str, metadata_values: dict[str, str]
) -> tuple[bool, str]:
    payload = {BM_DEF_NAME: metadata_values}
    response = requests.post(
        f"{ATLAS}/entity/guid/{atlas_guid}/businessmetadata",
        params={"isOverwrite": "true"},
        headers=headers,
        json=payload,
        timeout=60,
    )
    if response.status_code in (200, 204):
        return True, "atlas"
    return False, f"HTTP {response.status_code} - {response.text[:200]}"


def patch_unified_product(
    headers: dict[str, str], product_id: str, metadata_values: dict[str, str]
) -> tuple[bool, str]:
    response = requests.patch(
        f"{UNIFIED}/dataProducts/{product_id}",
        params={"api-version": API_VER},
        headers=headers,
        json={"additionalProperties": metadata_values},
        timeout=90,
    )
    if response.status_code in (200, 204):
        return True, "unified-catalog"
    return False, f"HTTP {response.status_code} - {response.text[:200]}"


def apply_custom_metadata(headers: dict[str, str]) -> dict[str, str]:
    hdr("2. APPLY CUSTOM METADATA TO DATA PRODUCTS")

    response = requests.get(
        f"{UNIFIED}/dataProducts",
        params={"api-version": API_VER},
        headers=headers,
        timeout=60,
    )
    if response.status_code != 200:
        warn(f"Cannot list data products: HTTP {response.status_code}")
        return {}

    products = response.json().get("value", [])
    info(f"Found {len(products)} data products in Unified Catalog")

    outcomes: dict[str, str] = {}

    for product in products:
        product_name = product.get("name", "")
        product_id = product.get("id", "")
        metadata_values = metadata_for_product(product_name)

        if not metadata_values:
            outcomes[product_name] = "skipped"
            info(f"No custom metadata defined for '{product_name}'")
            continue

        atlas_guid = lookup_atlas_guid(headers, product_name)
        if not atlas_guid:
            atlas_guid = create_atlas_entity(headers, product)
        if atlas_guid:
            applied, detail = apply_atlas_business_metadata(headers, atlas_guid, metadata_values)
            if applied:
                outcomes[product_name] = detail
                ok(f"{product_name} - applied via Atlas")
                continue
            warn(f"{product_name} Atlas apply failed: {detail}")

        applied, detail = patch_unified_product(headers, product_id, metadata_values)
        if applied:
            outcomes[product_name] = detail
            ok(f"{product_name} - applied via Unified Catalog PATCH")
        else:
            outcomes[product_name] = f"failed: {detail}"
            warn(f"{product_name} - {detail}")

    applied_count = sum(
        1 for value in outcomes.values() if not value.startswith("failed") and value != "skipped"
    )
    skipped_count = sum(1 for value in outcomes.values() if value == "skipped")
    print(f"\n  Applied: {applied_count} | Skipped: {skipped_count} | Total: {len(products)}")
    return outcomes


def print_summary(outcomes: dict[str, str]) -> None:
    hdr("3. SUMMARY")
    print(f"\n  {'Product':<35} {'Classification':<18} {'Sensitivity':<10} {'Result'}")
    print(f"  {'-' * 35} {'-' * 18} {'-' * 10} {'-' * 20}")
    preferred_names: dict[str, str] = {}
    for product_name in CUSTOM_METADATA:
        canonical = canonical_name(product_name)
        if canonical != product_name or canonical not in preferred_names:
            preferred_names[canonical] = product_name

    for product_name in preferred_names.values():
        metadata_values = CUSTOM_METADATA[product_name]
        classification = metadata_values["data_classification"]
        sensitivity = metadata_values["sensitivity_level"]
        result = outcomes.get(product_name, outcomes.get(canonical_name(product_name), "not processed"))
        print(f"  {product_name:<35} {classification:<18} {sensitivity:<10} {result}")

    print("\n  View in Purview: https://purview.microsoft.com")


def main() -> None:
    print(
        f"""
{BOLD}======================================================================
  ADD CUSTOM METADATA TO DATA PRODUCTS
  HealthcareGovernance - GDPR, Sensitivity, SLA, Standards
======================================================================{RESET}
"""
    )

    headers = refresh_token()
    if not ensure_bm_typedef(headers):
        warn("Continuing even though typedef creation did not succeed")

    headers = refresh_token()
    outcomes = apply_custom_metadata(headers)
    print_summary(outcomes)


if __name__ == "__main__":
    main()
