"""
Purview Complete Setup — Allt som saknas
=========================================
1. Skapa governance-domäner (via flera API-varianter)
2. Flytta BrainChild-entiteter till fabric-brainchild (korrekt API)
3. Registrera FHIR & DICOM som sökbara entiteter (custom typeDefs)
4. Skapa 13 dataprodukter (inkl. FHIR & DICOM)
5. Aktivera klassificering / trigga scan med scanruleset
6. Koppla glossary-termer till FHIR/DICOM-entiteter
"""
import json
import os
import sys
import time

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import requests
from azure.identity import AzureCliCredential
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

cred = AzureCliCredential(process_timeout=30)
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])))

# ── ENDPOINTS ──
ACCT = "https://prviewacc.purview.azure.com"
TENANT_ID = "71c4b6d5-0065-4c6c-a125-841a582754eb"
TENANT_EP = f"https://{TENANT_ID}-api.purview-service.microsoft.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
SCAN_EP = f"{ACCT}/scan"
COLL_API = "2019-11-01-preview"
SCAN_API = "2022-07-01-preview"

# BrainChild workspace
BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"
HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"

# Health Data Services
FHIR_EP = "https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com"
DICOM_EP = "https://brainchildhdws-brainchilddicom.dicom.azurehealthcareapis.com"
HDWS_NAME = "brainchildhdws"
FHIR_NAME = "brainchildfhir"
DICOM_NAME = "brainchilddicom"

# ── Formatting ──
B = "\033[94m"
G = "\033[92m"
Y = "\033[93m"
R = "\033[91m"
C = "\033[96m"
D = "\033[2m"
BOLD = "\033[1m"
RST = "\033[0m"
stats = {"ok": 0, "fixed": 0, "errors": 0}


def hdr(title):
    print(f"\n{BOLD}{B}{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}{RST}")


def ok(msg):
    stats["ok"] += 1
    print(f"  {G}OK{RST}    {msg}")


def fixed(msg):
    stats["fixed"] += 1
    print(f"  {G}FIXED{RST} {msg}")


def err(msg):
    stats["errors"] += 1
    print(f"  {R}ERR{RST}   {msg}")


def warn(msg):
    print(f"  {Y}WARN{RST}  {msg}")


def info(msg):
    print(f"  {D}INFO{RST}  {msg}")


_tokens = {}


def get_headers(scope="purview"):
    scope_url = {
        "purview": "https://purview.azure.net/.default",
    }[scope]
    if scope not in _tokens or _tokens[scope][1] < time.time() - 2400:
        token = cred.get_token(scope_url)
        _tokens[scope] = (token.token, time.time())
    return {"Authorization": f"Bearer {_tokens[scope][0]}", "Content-Type": "application/json"}


# ══════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════

def search_entities(h, entity_type, keywords="*", limit=100):
    """Search for entities by type."""
    body = {"keywords": keywords, "filter": {"entityType": entity_type}, "limit": limit}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        return r.json().get("value", [])
    return []


def search_all_entities(h, keywords="*", limit=100):
    """Search for all entities."""
    body = {"keywords": keywords, "limit": limit}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code == 200:
        return r.json().get("value", [])
    return []


# ══════════════════════════════════════════════════════════════════════
# STEP 1: GOVERNANCE DOMAINS
# ══════════════════════════════════════════════════════════════════════

DOMAINS = [
    {
        "name": "Klinisk Vård",
        "description": "Domän för klinisk patientdata — vårdbesök, diagnoser, medicinering, labresultat och ML-prediktioner.",
    },
    {
        "name": "Barncancerforskning",
        "description": "Domän för barncancerforskningsdata — FHIR-resurser, DICOM-bilder, genomik, biobanksdata och kvalitetsregister.",
    },
]


def create_governance_domains(h):
    hdr("1. SKAPA GOVERNANCE-DOMÄNER")
    domain_guids = {}

    # Try MANY different API endpoint variants
    api_variants = [
        # Datamap governance domains (newer API)
        (f"{ACCT}/datamap/api/governance-domains", "2023-10-01-preview", "POST"),
        (f"{ACCT}/datamap/api/governance-domains", "2023-09-01", "POST"),
        # Catalog data governance
        (f"{ACCT}/catalog/api/governance-domains", "2023-10-01-preview", "POST"),
        # DataGovernance at tenant endpoint
        (f"{TENANT_EP}/datagovernance/catalog/domains", "2025-09-15-preview", "POST"),
        (f"{TENANT_EP}/datagovernance/catalog/domains", "2024-03-01-preview", "POST"),
        # Account-scoped data governance
        (f"{ACCT}/datagovernance/catalog/domains", "2025-09-15-preview", "POST"),
        (f"{ACCT}/datagovernance/catalog/domains", "2024-03-01-preview", "POST"),
        # Datamap domains  
        (f"{ACCT}/datamap/api/domains", "2023-10-01-preview", "POST"),
    ]

    # First, try to LIST domains to see which endpoint works
    list_variants = [
        (f"{ACCT}/datamap/api/governance-domains", "2023-10-01-preview"),
        (f"{TENANT_EP}/datagovernance/catalog/domains", "2025-09-15-preview"),
        (f"{ACCT}/datagovernance/catalog/domains", "2025-09-15-preview"),
        (f"{ACCT}/catalog/api/governance-domains", "2023-10-01-preview"),
        (f"{ACCT}/datamap/api/domains", "2023-10-01-preview"),
    ]

    working_list_ep = None
    for base_url, api_ver in list_variants:
        r = sess.get(f"{base_url}?api-version={api_ver}", headers=h, timeout=15)
        info(f"LIST {base_url.replace(ACCT,'').replace(TENANT_EP,'{TENANT}')}?v={api_ver} -> {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            existing = data.get("value", data) if isinstance(data, dict) else data
            if isinstance(existing, list):
                for d in existing:
                    name = d.get("name", d.get("displayName", "?"))
                    did = d.get("id") or d.get("guid") or d.get("qualifiedName")
                    domain_guids[name] = did
                    ok(f"Domän finns: {name} (id={str(did)[:30]})")
            working_list_ep = (base_url, api_ver)
            break

    if not domain_guids:
        info("Inga befintliga domäner hittade — skapar nya")

    for domain in DOMAINS:
        if domain["name"] in domain_guids:
            continue

        created = False
        body = {
            "name": domain["name"],
            "description": domain["description"],
        }

        for base_url, api_ver, method in api_variants:
            url = f"{base_url}?api-version={api_ver}"
            try:
                if method == "POST":
                    r = sess.post(url, headers=h, json=body, timeout=20)
                else:
                    r = sess.put(url, headers=h, json=body, timeout=20)
            except Exception as e:
                continue

            info(f"CREATE {base_url.replace(ACCT,'').replace(TENANT_EP,'{TENANT}')} -> {r.status_code}")
            if r.status_code in (200, 201):
                resp = r.json()
                did = resp.get("id") or resp.get("guid") or resp.get("qualifiedName")
                domain_guids[domain["name"]] = did
                fixed(f"Domän: {domain['name']} (id={str(did)[:30]})")
                created = True
                break
            elif r.status_code == 409:
                ok(f"Domän {domain['name']} finns redan")
                created = True
                break
            elif r.status_code in (404, 405):
                continue  # Try next endpoint
            else:
                info(f"  Response: {r.text[:120]}")

        if not created:
            warn(f"Kunde inte skapa domän '{domain['name']}' via REST API")
            warn("  -> Skapa manuellt: Purview portal -> Data Catalog -> Governance domains -> + New")
            warn(f"     Namn: {domain['name']}")
            warn(f"     Beskrivning: {domain['description']}")

    return domain_guids


# ══════════════════════════════════════════════════════════════════════
# STEP 2: FLYTTA BRAINCHILD-ENTITETER
# ══════════════════════════════════════════════════════════════════════

def move_bc_entities(h):
    hdr("2. FLYTTA BRAINCHILD-ENTITETER → fabric-brainchild")

    # First verify fabric-brainchild collection exists
    r = sess.get(f"{ACCT}/account/collections/fabric-brainchild?api-version={COLL_API}", headers=h, timeout=15)
    if r.status_code != 200:
        warn(f"Collection fabric-brainchild: {r.status_code}")
        info("Skapar collection fabric-brainchild...")
        body = {
            "name": "fabric-brainchild",
            "friendlyName": "Fabric BrainChild",
            "parentCollection": {"referenceName": "barncancer"},
        }
        r2 = sess.put(f"{ACCT}/account/collections/fabric-brainchild?api-version={COLL_API}",
                       headers=h, json=body, timeout=15)
        if r2.status_code in (200, 201):
            fixed("Skapade collection: fabric-brainchild -> barncancer")
        else:
            err(f"Kan inte skapa collection: {r2.status_code} {r2.text[:120]}")
            return
    else:
        ok("Collection fabric-brainchild finns")

    # Find BrainChild entities (tables + lakehouse)
    bc_entities = []
    for etype in ["fabric_lakehouse_table", "fabric_lake_warehouse"]:
        for ent in search_entities(h, etype):
            qname = ent.get("qualifiedName", "")
            if BC_WS.lower() in qname.lower():
                bc_entities.append(ent)

    info(f"Hittade {len(bc_entities)} BrainChild-entiteter")

    # Filter: only those NOT already in fabric-brainchild
    to_move = [e for e in bc_entities if e.get("collectionId", "") != "fabric-brainchild"]
    already = len(bc_entities) - len(to_move)
    if already:
        info(f"{already} redan i fabric-brainchild")

    if not to_move:
        ok("Alla entiteter redan i rätt collection")
        return

    guids_to_move = [e["id"] for e in to_move]
    info(f"Flyttar {len(guids_to_move)} entiteter...")

    # Strategy 1: Datamap moveTo API (newer)
    move_apis = [
        (f"{ACCT}/datamap/api/entity/moveTo", "2023-09-01", {"entityGuids": guids_to_move}),
        (f"{ACCT}/datamap/api/entity/moveTo", "2023-10-01-preview", {"entityGuids": guids_to_move}),
        (f"{ACCT}/catalog/api/collections/fabric-brainchild/entity", "2022-08-01-preview", {"entityGuids": guids_to_move}),
    ]

    for url_base, api_ver, body in move_apis:
        url = f"{url_base}?collectionId=fabric-brainchild&api-version={api_ver}" if "moveTo" in url_base else f"{url_base}?api-version={api_ver}"
        r = sess.post(url, headers=h, json=body, timeout=30)
        info(f"MOVE via {url_base.replace(ACCT,'')} (v={api_ver}) -> {r.status_code}")
        if r.status_code in (200, 204):
            fixed(f"Flyttade {len(guids_to_move)} entiteter till fabric-brainchild")
            return
        elif r.status_code == 207:
            # Partial success
            resp = r.json()
            info(f"Partiellt resultat: {json.dumps(resp)[:200]}")
            return

    # Strategy 2: Update each entity individually via partial update
    info("Bulk-flytt misslyckades — försöker individuellt via entity update...")
    moved = 0
    for ent in to_move:
        guid = ent["id"]
        name = ent.get("name", "?")
        etype = ent.get("entityType", "?")

        # Get full entity
        r = sess.get(f"{ATLAS}/entity/guid/{guid}?minExtInfo=true", headers=h, timeout=15)
        if r.status_code != 200:
            err(f"Kan inte hämta {name}: {r.status_code}")
            continue

        entity_data = r.json().get("entity", {})
        qname = entity_data.get("attributes", {}).get("qualifiedName", "")
        type_name = entity_data.get("typeName", etype)

        # Try updating with collection reference
        update_body = {
            "entity": {
                "typeName": type_name,
                "attributes": {
                    "qualifiedName": qname,
                },
                "collectionId": "fabric-brainchild",
            }
        }
        r2 = sess.post(f"{DATAMAP}/entity", headers=h, json=update_body, timeout=15)
        if r2.status_code in (200, 201):
            fixed(f"{name} ({etype}) -> fabric-brainchild")
            moved += 1
        else:
            # Try ATLAS API
            r3 = sess.post(f"{ATLAS}/entity", headers=h, json=update_body, timeout=15)
            if r3.status_code in (200, 201):
                fixed(f"{name} ({etype}) -> fabric-brainchild (atlas)")
                moved += 1
            else:
                # Try with full entity body
                entity_data["collectionId"] = "fabric-brainchild"
                r4 = sess.post(f"{DATAMAP}/entity", headers=h,
                               json={"entity": entity_data}, timeout=15)
                if r4.status_code in (200, 201):
                    fixed(f"{name} ({etype}) -> fabric-brainchild (full)")
                    moved += 1
                else:
                    err(f"{name}: {r2.status_code}/{r3.status_code}/{r4.status_code}")

        time.sleep(0.3)

    info(f"Flyttade {moved}/{len(to_move)} entiteter")


# ══════════════════════════════════════════════════════════════════════
# STEP 3: REGISTRERA FHIR & DICOM SOM SÖKBARA ENTITETER
# ══════════════════════════════════════════════════════════════════════

FHIR_RESOURCES = [
    ("Patient", "FHIR Patient-resurs — patientdemografi"),
    ("Encounter", "FHIR Encounter-resurs — vårdkontakter"),
    ("Condition", "FHIR Condition-resurs — diagnoser"),
    ("Observation", "FHIR Observation-resurs — labresultat och vitalparametrar"),
    ("Specimen", "FHIR Specimen-resurs — biologiska prover"),
    ("ImagingStudy", "FHIR ImagingStudy-resurs — koppling till DICOM-bildstudier"),
    ("DiagnosticReport", "FHIR DiagnosticReport — diagnostiska rapporter inkl. GMS/genomik"),
    ("MedicationRequest", "FHIR MedicationRequest — läkemedelsordinationer"),
]

DICOM_MODALITIES = [
    ("MRI_Brain", "MR-hjärna (T1, T2, FLAIR) — primärtumör och uppföljning"),
    ("Pathology", "Histopatologi — H&E-färgning av tumörvävnad"),
]


def register_fhir_dicom(h):
    hdr("3. REGISTRERA FHIR & DICOM SOM SÖKBARA ENTITETER")

    # Step 3a: Create custom type definitions
    info("Skapar custom type definitions...")

    type_defs = {
        "entityDefs": [
            {
                "name": "healthcare_fhir_service",
                "description": "Azure Health Data Services — FHIR R4 Server",
                "superTypes": ["DataSet"],
                "serviceType": "Azure Health Data Services",
                "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "endpoint", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "description": "FHIR server endpoint URL"},
                    {"name": "fhirVersion", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "description": "FHIR version (R4)"},
                    {"name": "resourceCount", "typeName": "int", "isOptional": True,
                     "cardinality": "SINGLE", "description": "Number of resources"},
                ],
            },
            {
                "name": "healthcare_fhir_resource_type",
                "description": "FHIR Resource Type (Patient, Encounter, etc.)",
                "superTypes": ["DataSet"],
                "serviceType": "Azure Health Data Services",
                "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "resourceType", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "description": "FHIR resource type name"},
                    {"name": "endpoint", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "description": "Resource endpoint URL"},
                    {"name": "fhirVersion", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "description": "FHIR version"},
                ],
            },
            {
                "name": "healthcare_dicom_service",
                "description": "Azure Health Data Services — DICOM Server",
                "superTypes": ["DataSet"],
                "serviceType": "Azure Health Data Services",
                "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "endpoint", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "description": "DICOM server endpoint URL"},
                    {"name": "modalities", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "description": "Supported modalities"},
                ],
            },
            {
                "name": "healthcare_dicom_modality",
                "description": "DICOM Modality (MRI, Pathology, etc.)",
                "superTypes": ["DataSet"],
                "serviceType": "Azure Health Data Services",
                "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "modality", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "description": "Modality type"},
                    {"name": "endpoint", "typeName": "string", "isOptional": True,
                     "cardinality": "SINGLE", "description": "DICOM endpoint URL"},
                    {"name": "studyCount", "typeName": "int", "isOptional": True,
                     "cardinality": "SINGLE", "description": "Number of studies"},
                ],
            },
        ],
    }

    # Try creating type definitions
    typedef_created = False
    for api_base in [DATAMAP, ATLAS]:
        r = sess.post(f"{api_base}/types/typedefs", headers=h, json=type_defs, timeout=30)
        info(f"TypeDefs via {api_base.replace(ACCT,'')} -> {r.status_code}")
        if r.status_code in (200, 201):
            fixed("Custom type definitions skapade (FHIR + DICOM)")
            typedef_created = True
            break
        elif r.status_code == 409:
            ok("Custom type definitions finns redan")
            typedef_created = True
            break
        else:
            # Might partially fail if some types exist
            resp_text = r.text[:300]
            if "already exists" in resp_text.lower() or "ATLAS-409" in resp_text:
                ok("Custom type definitions finns redan (partial)")
                typedef_created = True
                break
            info(f"  {resp_text}")

    if not typedef_created:
        warn("Kunde inte skapa custom typedefs — försöker med generisk typ istället")

    time.sleep(1)

    # Step 3b: Create FHIR Service entity
    info("Registrerar FHIR-server...")
    fhir_entity_guid = None
    fhir_qname = f"healthcare://{FHIR_EP}"

    fhir_entity = {
        "entity": {
            "typeName": "healthcare_fhir_service" if typedef_created else "DataSet",
            "attributes": {
                "qualifiedName": fhir_qname,
                "name": "BrainChild FHIR Server (R4)",
                "description": (
                    "Azure Health Data Services FHIR R4 — BrainChild barncancerprojektet. "
                    "Innehåller Patient, Encounter, Condition, Observation, Specimen, "
                    "ImagingStudy, DiagnosticReport och MedicationRequest."
                ),
                "endpoint": FHIR_EP,
                "fhirVersion": "R4",
                "resourceCount": 8,
                "userDescription": (
                    "FHIR R4 API for BrainChild. Endpoint: " + FHIR_EP + ". "
                    "Resurser: Patient (50), Encounter, Condition, Observation, Specimen, "
                    "ImagingStudy, DiagnosticReport, MedicationRequest."
                ),
            },
            "collectionId": "fabric-brainchild",
        }
    }

    for api_base in [DATAMAP, ATLAS]:
        r = sess.post(f"{api_base}/entity", headers=h, json=fhir_entity, timeout=30)
        if r.status_code in (200, 201):
            resp = r.json()
            guids = resp.get("guidAssignments", resp.get("mutatedEntities", {}).get("CREATE", []))
            if isinstance(guids, dict):
                fhir_entity_guid = list(guids.values())[0] if guids else None
            elif isinstance(guids, list) and guids:
                fhir_entity_guid = guids[0].get("guid") if isinstance(guids[0], dict) else guids[0]
            if not fhir_entity_guid:
                # Try to extract from response
                fhir_entity_guid = resp.get("guidAssignments", {}).get("-1") or resp.get("guid")
            fixed(f"FHIR Server registrerad (guid={str(fhir_entity_guid)[:20]})")
            break
        else:
            info(f"  FHIR via {api_base.replace(ACCT,'')} -> {r.status_code}: {r.text[:150]}")
            # If custom type failed, try with DataSet
            if "healthcare_fhir_service" in str(fhir_entity):
                fhir_entity["entity"]["typeName"] = "DataSet"
                r2 = sess.post(f"{api_base}/entity", headers=h, json=fhir_entity, timeout=30)
                if r2.status_code in (200, 201):
                    resp = r2.json()
                    fhir_entity_guid = resp.get("guidAssignments", {}).get("-1")
                    fixed(f"FHIR Server registrerad som DataSet (guid={str(fhir_entity_guid)[:20]})")
                    break

    # Step 3c: Create individual FHIR resource type entities
    fhir_resource_guids = {}
    for res_name, res_desc in FHIR_RESOURCES:
        res_qname = f"healthcare://{FHIR_EP}/{res_name}"
        res_entity = {
            "entity": {
                "typeName": "healthcare_fhir_resource_type" if typedef_created else "DataSet",
                "attributes": {
                    "qualifiedName": res_qname,
                    "name": f"FHIR {res_name}",
                    "description": res_desc,
                    "resourceType": res_name,
                    "endpoint": f"{FHIR_EP}/{res_name}",
                    "fhirVersion": "R4",
                    "userDescription": f"{res_desc}. Endpoint: {FHIR_EP}/{res_name}",
                },
                "collectionId": "fabric-brainchild",
            }
        }

        for api_base in [DATAMAP, ATLAS]:
            r = sess.post(f"{api_base}/entity", headers=h, json=res_entity, timeout=15)
            if r.status_code in (200, 201):
                resp = r.json()
                guid = resp.get("guidAssignments", {}).get("-1")
                if not guid:
                    # Try UPDATE path
                    updated = resp.get("mutatedEntities", {}).get("UPDATE", [])
                    if updated:
                        guid = updated[0].get("guid")
                fhir_resource_guids[res_name] = guid
                fixed(f"FHIR {res_name}")
                break
            elif r.status_code in (200,) and "UPDATE" in r.text:
                ok(f"FHIR {res_name} (redan finns)")
                break
        time.sleep(0.2)

    # Step 3d: Create DICOM Service entity
    info("Registrerar DICOM-server...")
    dicom_entity_guid = None
    dicom_qname = f"healthcare://{DICOM_EP}"

    dicom_entity = {
        "entity": {
            "typeName": "healthcare_dicom_service" if typedef_created else "DataSet",
            "attributes": {
                "qualifiedName": dicom_qname,
                "name": "BrainChild DICOM Server",
                "description": (
                    "Azure Health Data Services DICOMweb — BrainChild barncancerprojektet. "
                    "MR-hjärna (T1/T2/FLAIR) och histopatologi (H&E-färgning)."
                ),
                "endpoint": DICOM_EP,
                "modalities": "MR, SM (Slide Microscopy)",
                "userDescription": (
                    "DICOMweb API for BrainChild. Endpoint: " + DICOM_EP + ". "
                    "Modaliteter: MR-hjärna (T1, T2, FLAIR), Patologi (H&E)."
                ),
            },
            "collectionId": "fabric-brainchild",
        }
    }

    for api_base in [DATAMAP, ATLAS]:
        r = sess.post(f"{api_base}/entity", headers=h, json=dicom_entity, timeout=30)
        if r.status_code in (200, 201):
            resp = r.json()
            dicom_entity_guid = resp.get("guidAssignments", {}).get("-1")
            fixed(f"DICOM Server registrerad (guid={str(dicom_entity_guid)[:20]})")
            break
        else:
            if "healthcare_dicom_service" in str(dicom_entity):
                dicom_entity["entity"]["typeName"] = "DataSet"
                r2 = sess.post(f"{api_base}/entity", headers=h, json=dicom_entity, timeout=30)
                if r2.status_code in (200, 201):
                    resp = r2.json()
                    dicom_entity_guid = resp.get("guidAssignments", {}).get("-1")
                    fixed(f"DICOM Server registrerad som DataSet")
                    break

    # Step 3e: Create DICOM modality entities
    dicom_modality_guids = {}
    for mod_name, mod_desc in DICOM_MODALITIES:
        mod_qname = f"healthcare://{DICOM_EP}/{mod_name}"
        mod_entity = {
            "entity": {
                "typeName": "healthcare_dicom_modality" if typedef_created else "DataSet",
                "attributes": {
                    "qualifiedName": mod_qname,
                    "name": f"DICOM {mod_name}",
                    "description": mod_desc,
                    "modality": mod_name.split("_")[0],
                    "endpoint": DICOM_EP,
                    "userDescription": f"{mod_desc}. DICOMweb: {DICOM_EP}",
                },
                "collectionId": "fabric-brainchild",
            }
        }

        for api_base in [DATAMAP, ATLAS]:
            r = sess.post(f"{api_base}/entity", headers=h, json=mod_entity, timeout=15)
            if r.status_code in (200, 201):
                resp = r.json()
                guid = resp.get("guidAssignments", {}).get("-1")
                dicom_modality_guids[mod_name] = guid
                fixed(f"DICOM {mod_name}")
                break
        time.sleep(0.2)

    return fhir_entity_guid, fhir_resource_guids, dicom_entity_guid, dicom_modality_guids


# ══════════════════════════════════════════════════════════════════════
# STEP 4: KOPPLA GLOSSARY-TERMER TILL FHIR/DICOM
# ══════════════════════════════════════════════════════════════════════

def link_terms_to_fhir_dicom(h, fhir_entity_guid, fhir_resource_guids,
                              dicom_entity_guid, dicom_modality_guids):
    hdr("4. KOPPLA GLOSSARY-TERMER TILL FHIR/DICOM-ENTITETER")

    # Re-discover entity GUIDs via search (more reliable than create responses)
    info("Söker FHIR/DICOM-entiteter i katalogen...")
    fhir_guids = {}
    dicom_guids = {}

    for query in ["healthcare_fhir", "healthcare_dicom", "BrainChild FHIR", "BrainChild DICOM", "FHIR", "DICOM"]:
        body = {"keywords": query, "limit": 50}
        r = sess.post(SEARCH, headers=h, json=body, timeout=15)
        if r.status_code == 200:
            for ent in r.json().get("value", []):
                name = ent.get("name", "")
                guid = ent.get("id", "")
                etype = ent.get("entityType", "")
                if "fhir" in etype.lower() or "fhir" in name.lower():
                    if "FHIR Server" in name or etype == "healthcare_fhir_service":
                        fhir_guids["_server"] = guid
                    for res_name in ["Patient", "Encounter", "Condition", "Observation",
                                     "Specimen", "ImagingStudy", "DiagnosticReport", "MedicationRequest"]:
                        if res_name in name:
                            fhir_guids[res_name] = guid
                if "dicom" in etype.lower() or "dicom" in name.lower():
                    if "DICOM Server" in name or etype == "healthcare_dicom_service":
                        dicom_guids["_server"] = guid
                    for mod_name in ["MRI_Brain", "Pathology"]:
                        if mod_name in name or mod_name.replace("_", " ") in name:
                            dicom_guids[mod_name] = guid

    info(f"Hittade {len(fhir_guids)} FHIR-entiteter, {len(dicom_guids)} DICOM-entiteter via sökning")

    # Also use the passed-in GUIDs as fallback
    if fhir_entity_guid and "_server" not in fhir_guids:
        fhir_guids["_server"] = fhir_entity_guid
    for k, v in fhir_resource_guids.items():
        if v and k not in fhir_guids:
            fhir_guids[k] = v
    if dicom_entity_guid and "_server" not in dicom_guids:
        dicom_guids["_server"] = dicom_entity_guid
    for k, v in dicom_modality_guids.items():
        if v and k not in dicom_guids:
            dicom_guids[k] = v

    # First, get glossary term GUIDs
    r = sess.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    if r.status_code != 200:
        err(f"Kan inte läsa glossary: {r.status_code}")
        return

    glossaries = r.json() if isinstance(r.json(), list) else [r.json()]
    glossary_guid = None
    for g in glossaries:
        if "Sjukvård" in g.get("name", "") or g.get("guid") == "d939ea20-9c67-48af-98d9-b66965f7cde1":
            glossary_guid = g["guid"]
            break

    if not glossary_guid and glossaries:
        glossary_guid = glossaries[0]["guid"]

    if not glossary_guid:
        warn("Ingen glossary hittad")
        return

    # Get all terms
    r = sess.get(f"{ATLAS}/glossary/{glossary_guid}/terms?limit=100", headers=h, timeout=30)
    if r.status_code != 200:
        err(f"Kan inte läsa termer: {r.status_code}")
        return

    terms = {t["name"]: t["guid"] for t in r.json()}
    info(f"Hittade {len(terms)} glossary-termer")

    # Map terms to FHIR/DICOM entities
    all_fhir = [g for g in fhir_guids.values() if g]
    all_dicom = [g for g in dicom_guids.values() if g]
    term_entity_map = {
        "FHIR R4": all_fhir,
        "FHIR Patient": [fhir_guids.get("Patient")],
        "FHIR Encounter": [fhir_guids.get("Encounter")],
        "FHIR Condition": [fhir_guids.get("Condition")],
        "FHIR Observation": [fhir_guids.get("Observation")],
        "FHIR Specimen": [fhir_guids.get("Specimen")],
        "FHIR ImagingStudy": [fhir_guids.get("ImagingStudy")],
        "FHIR DiagnosticReport": [fhir_guids.get("DiagnosticReport")],
        "FHIR MedicationRequest": [fhir_guids.get("MedicationRequest")],
        "DICOM": all_dicom,
        "Genomic Medicine Sweden (GMS)": [fhir_guids.get("DiagnosticReport")],
        "BTB (Barntumörbanken)": [fhir_guids.get("Specimen")],
        "SNOMED-CT": [fhir_guids.get("Specimen"), fhir_guids.get("Condition")],
        "ICD-O-3": [fhir_guids.get("Condition")],
        "LOINC": [fhir_guids.get("Observation")],
        "HGVS-nomenklatur": [fhir_guids.get("DiagnosticReport")],
    }

    mapped = 0
    for term_name, entity_guids_list in term_entity_map.items():
        if term_name not in terms:
            continue
        term_guid = terms[term_name]
        valid_guids = [{"guid": g} for g in entity_guids_list if g]
        if not valid_guids:
            continue

        r = sess.post(
            f"{ATLAS}/glossary/terms/{term_guid}/assignedEntities",
            headers=h, json=valid_guids, timeout=15,
        )
        if r.status_code in (200, 201, 204):
            fixed(f"{term_name} -> {len(valid_guids)} FHIR/DICOM-entiteter")
            mapped += 1
        elif r.status_code == 409:
            ok(f"{term_name} redan kopplad")
        else:
            warn(f"{term_name}: {r.status_code}")
        time.sleep(0.2)

    info(f"Kopplade {mapped} termer")


# ══════════════════════════════════════════════════════════════════════
# STEP 5: SKAPA DATAPRODUKTER (13 st inkl. FHIR & DICOM)
# ══════════════════════════════════════════════════════════════════════

ALL_DATA_PRODUCTS = {
    "Klinisk Vård": [
        {"name": "Patientdemografi",
         "description": "Demografisk patientdata. Källa: hca.patients (SQL). Standard: FHIR Patient, OMOP Person."},
        {"name": "Vårdbesök & utfall",
         "description": "Vårdbesöksdata med LOS och återinläggningsrisk."},
        {"name": "Diagnoser (ICD-10)",
         "description": "Diagnosinformation klassificerad med ICD-10."},
        {"name": "Medicinering (ATC)",
         "description": "Läkemedelsdata klassificerad med ATC."},
        {"name": "Vitalparametrar & labb",
         "description": "Vitalparametrar och labresultat."},
        {"name": "ML-prediktion (LOS & readmission)",
         "description": "ML-modell för vårdtid och återinläggningsprediktion."},
    ],
    "Barncancerforskning": [
        {"name": "FHIR Patientresurser",
         "description": "BrainChild FHIR R4-resurser: Patient, Encounter, Condition, Observation, Specimen."},
        {"name": "Medicinsk bilddiagnostik (DICOM)",
         "description": "MR-hjärna och patologidata i DICOM-format via DICOMweb API."},
        {"name": "Genomik (GMS/VCF)",
         "description": "Genomiska varianter i VCF-format och GMS DiagnosticReports."},
        {"name": "Biobanksdata (BTB)",
         "description": "Barntumörbankens provdata — FHIR Specimen med VCF-koppling."},
        {"name": "Kvalitetsregister (SBCR)",
         "description": "Svenska Barncancerregistret — registrering, behandling, uppföljning."},
        {"name": "FHIR R4 API",
         "description": f"FHIR R4 REST API endpoint: {FHIR_EP}. 8 resurstyper: Patient, Encounter, Condition, Observation, Specimen, ImagingStudy, DiagnosticReport, MedicationRequest."},
        {"name": "DICOMweb API",
         "description": f"DICOMweb REST API endpoint: {DICOM_EP}. Modaliteter: MR-hjärna (T1/T2/FLAIR) och histopatologi (H&E)."},
    ],
}


def create_data_products(h, domain_guids):
    hdr("5. SKAPA DATAPRODUKTER (13 st inkl. FHIR & DICOM API)")

    if not domain_guids:
        warn("Inga domän-ID:n — kan inte skapa dataprodukter")
        warn("Kör steg 1 först eller skapa domäner manuellt i portalen")
        return

    # Map domain names (flexible matching)
    domain_map = {}
    for expected in ["Klinisk Vård", "Barncancerforskning"]:
        if expected in domain_guids:
            domain_map[expected] = domain_guids[expected]
        else:
            # Try partial match
            for actual, did in domain_guids.items():
                if (expected.lower() in actual.lower() or
                        actual.lower() in expected.lower() or
                        ("forskning" in actual.lower() and "cancer" in expected.lower())):
                    domain_map[expected] = did
                    info(f"Mappade '{expected}' -> '{actual}'")
                    break

    created = 0
    for domain_name, products in ALL_DATA_PRODUCTS.items():
        domain_id = domain_map.get(domain_name)
        if not domain_id:
            warn(f"Inget domän-ID för '{domain_name}' — hoppar över {len(products)} dataprodukter")
            continue

        for product in products:
            body = {
                "name": product["name"],
                "description": product["description"],
                "domainId": domain_id,
            }

            success = False
            for base in [
                f"{TENANT_EP}/datagovernance/catalog",
                f"{ACCT}/datagovernance/catalog",
                f"{ACCT}/datamap/api",
            ]:
                for api_ver in ["2025-09-15-preview", "2024-03-01-preview", "2023-10-01-preview"]:
                    r = sess.post(f"{base}/dataProducts?api-version={api_ver}", headers=h, json=body, timeout=20)
                    if r.status_code in (200, 201):
                        fixed(f"[{domain_name}] {product['name']}")
                        created += 1
                        success = True
                        break
                    elif r.status_code == 409:
                        ok(f"[{domain_name}] {product['name']} (redan finns)")
                        success = True
                        break
                if success:
                    break

            if not success:
                warn(f"[{domain_name}] {product['name']}: kunde inte skapas via API")
            time.sleep(0.3)

    info(f"Skapade {created} dataprodukter")


# ══════════════════════════════════════════════════════════════════════
# STEP 6: KLASSIFICERING — Aktivera classification rules på scan
# ══════════════════════════════════════════════════════════════════════

def fix_classification(h):
    hdr("6. KLASSIFICERING — Scan Rules & Classification")

    # Check existing scan rule sets
    r = sess.get(f"{SCAN_EP}/scanrulesets?api-version={SCAN_API}", headers=h, timeout=15)
    if r.status_code == 200:
        rulesets = r.json().get("value", [])
        info(f"Antal scan rule sets: {len(rulesets)}")
        for rs in rulesets[:10]:
            rtype = rs.get("kind", "?")
            rname = rs.get("name", "?")
            info(f"  {rname} (kind={rtype})")

    # Check system scan rule sets
    r2 = sess.get(f"{SCAN_EP}/systemScanRulesets?api-version={SCAN_API}", headers=h, timeout=15)
    if r2.status_code == 200:
        system_rulesets = r2.json().get("value", [])
        info(f"System scan rule sets: {len(system_rulesets)}")
        for rs in system_rulesets[:5]:
            info(f"  {rs.get('name', '?')} (kind={rs.get('kind', '?')})")

    # List classification rules
    r3 = sess.get(f"{SCAN_EP}/classificationrules?api-version={SCAN_API}", headers=h, timeout=15)
    if r3.status_code == 200:
        rules = r3.json().get("value", [])
        info(f"Classification rules: {len(rules)}")
        for rule in rules[:5]:
            info(f"  {rule.get('name', '?')} (kind={rule.get('kind', '?')})")

    # For SQL scan — ensure classification is enabled
    info("Kontrollerar SQL-scan klassificering...")
    r4 = sess.get(f"{SCAN_EP}/datasources/sql-hca-demo/scans?api-version={SCAN_API}", headers=h, timeout=15)
    if r4.status_code == 200:
        sql_scans = r4.json().get("value", [])
        for scan in sql_scans:
            sname = scan["name"]
            props = scan.get("properties", {})
            ruleset = props.get("scanRulesetName", "?")
            ruleset_type = props.get("scanRulesetType", "?")
            info(f"  SQL scan: {sname} (ruleset={ruleset}, type={ruleset_type})")

            # Check if classification is enabled
            if ruleset == "AzureSqlDatabase" and ruleset_type == "System":
                ok(f"SQL scan {sname}: system classification enabled")
            else:
                info(f"  Uppdaterar scan till system ruleset...")
                scan["properties"]["scanRulesetName"] = "AzureSqlDatabase"
                scan["properties"]["scanRulesetType"] = "System"
                r5 = sess.put(
                    f"{SCAN_EP}/datasources/sql-hca-demo/scans/{sname}?api-version={SCAN_API}",
                    headers=h, json=scan, timeout=15
                )
                if r5.status_code in (200, 201):
                    fixed(f"SQL scan {sname}: classification aktiverad")
                else:
                    warn(f"SQL scan {sname}: {r5.status_code}")

    # For Fabric scan — check and document
    info("Kontrollerar Fabric-scan...")
    r5 = sess.get(f"{SCAN_EP}/datasources/Fabric/scans?api-version={SCAN_API}", headers=h, timeout=15)
    if r5.status_code == 200:
        fabric_scans = r5.json().get("value", [])
        for scan in fabric_scans:
            sname = scan["name"]
            scan_kind = scan.get("kind", "?")
            props = scan.get("properties", {})
            info(f"  Fabric scan: {sname} (kind={scan_kind})")
            info(f"  Properties: {json.dumps(props, default=str)[:200]}")

            # PowerBI/Fabric scans use sensitivity labels from MIP, not Purview classification rules
            info("  -> Fabric/PowerBI-scans klassificerar via Microsoft Information Protection (MIP)")
            info("     Sensitivity labels sätts i Fabric admin portal, inte via Purview scan rules")


# ══════════════════════════════════════════════════════════════════════
# STEP 7: TRIGGA SCAN (korrekt metod: PUT med run-id)
# ══════════════════════════════════════════════════════════════════════

def trigger_scans(h):
    hdr("7. TRIGGA SCANS (SQL + Fabric)")

    for ds_name in ["sql-hca-demo", "Fabric"]:
        try:
            r = sess.get(f"{SCAN_EP}/datasources/{ds_name}/scans?api-version={SCAN_API}", headers=h, timeout=15)
        except Exception as e:
            warn(f"Kan inte nå {ds_name} scans: {e}")
            continue
        if r.status_code != 200:
            warn(f"Kan inte lista scans för {ds_name}: {r.status_code}")
            continue

        scans = r.json().get("value", [])
        if not scans:
            warn(f"Inga scans för {ds_name}")
            continue

        for scan in scans:
            sname = scan["name"]
            # Check if already running
            try:
                r2 = sess.get(
                    f"{SCAN_EP}/datasources/{ds_name}/scans/{sname}/runs?api-version={SCAN_API}",
                    headers=h, timeout=15,
                )
                if r2.status_code == 200:
                    runs = r2.json().get("value", [])
                    if runs and runs[0].get("status") in ("InProgress", "Queued"):
                        info(f"{ds_name}/{sname}: redan igång ({runs[0]['status']})")
                        continue
            except Exception:
                pass

            # Trigger with PUT + run-id (correct method — POST returns 405)
            run_id = f"run-{int(time.time())}"
            try:
                # Use a fresh session without retry on 500 for scan trigger
                r3 = requests.put(
                    f"{SCAN_EP}/datasources/{ds_name}/scans/{sname}/runs/{run_id}?api-version={SCAN_API}",
                    headers=h, json={}, timeout=60,
                )
                if r3.status_code in (200, 201, 202):
                    fixed(f"Scan triggad: {ds_name}/{sname}")
                elif r3.status_code == 500:
                    warn(f"Scan {ds_name}/{sname}: 500 (server error, ruleset kanske uppdateras)")
                else:
                    err(f"Scan {ds_name}/{sname}: {r3.status_code} {r3.text[:100]}")
            except Exception as e:
                warn(f"Scan {ds_name}/{sname}: {e}")
            time.sleep(2)


# ══════════════════════════════════════════════════════════════════════
# STEP 8: VERIFIERA SÖKBARHET
# ══════════════════════════════════════════════════════════════════════

def verify_searchability(h):
    hdr("8. VERIFIERA SÖKBARHET")

    test_queries = [
        ("FHIR", "FHIR-relaterade entiteter"),
        ("DICOM", "DICOM-relaterade entiteter"),
        ("BrainChild", "BrainChild-entiteter"),
        ("Patient", "Patient-relaterade entiteter"),
        ("genomik", "Genomik-relaterade entiteter"),
        ("healthcare_fhir", "Custom FHIR types"),
    ]

    for query, desc in test_queries:
        body = {"keywords": query, "limit": 10}
        r = sess.post(SEARCH, headers=h, json=body, timeout=15)
        if r.status_code == 200:
            results = r.json().get("value", [])
            count = r.json().get("@search.count", len(results))
            if count > 0:
                ok(f"'{query}' -> {count} resultat ({desc})")
                for ent in results[:3]:
                    ename = ent.get("name", "?")
                    etype = ent.get("entityType", "?")
                    coll = ent.get("collectionId", "?")
                    print(f"         {ename} ({etype}) [{coll}]")
            else:
                warn(f"'{query}' -> 0 resultat")
        else:
            err(f"Sök '{query}': {r.status_code}")
        time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"\n{BOLD}{B}{'=' * 70}")
    print(f"  PURVIEW COMPLETE SETUP — Domäner, FHIR/DICOM, Dataprodukter")
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 70}{RST}")

    h = get_headers()

    # 1. Governance Domains
    domain_guids = create_governance_domains(h)

    # 2. Move BC entities
    move_bc_entities(h)

    # 3. Register FHIR & DICOM
    fhir_guid, fhir_res_guids, dicom_guid, dicom_mod_guids = register_fhir_dicom(h)

    # 4. Link glossary terms to FHIR/DICOM
    link_terms_to_fhir_dicom(h, fhir_guid, fhir_res_guids, dicom_guid, dicom_mod_guids)

    # 5. Data Products (needs domain IDs)
    create_data_products(h, domain_guids)

    # 6. Classification
    fix_classification(h)

    # 7. Trigger scans
    trigger_scans(h)

    # 8. Verify searchability
    verify_searchability(h)

    # Summary
    hdr("SAMMANFATTNING")
    print(f"  {G}Fixat:   {stats['fixed']}{RST}")
    print(f"  {R}Fel:     {stats['errors']}{RST}")
    print(f"  OK:      {stats['ok']}")
    print(f"\n  {D}Scans tar 2-5 minuter. Kör sedan:{RST}")
    print(f"  {D}  python scripts/purview_full_diagnostic.py --diag-only{RST}")
    print()
