#!/usr/bin/env python3
"""
FIX ALL PURVIEW ISSUES (v2 - corrected API patterns)
====================================================
1. Update Data Product metadata (PUT, not PATCH)
2. Create Glossary Terms in Unified Catalog (POST /terms with domain field)
3. Link Glossary Terms to Data Products
4. Create Critical Data Elements

All 3 data products already have domain links.

Run from: c:/code/healthcare-analytics/healthcare-analytics
"""
import requests
import json
import time
from azure.identity import AzureCliCredential

# ─── AUTH ───────────────────────────────────────────────
credential = AzureCliCredential()
token = credential.get_token("https://purview.azure.net/.default").token
H = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

BASE   = "https://prviewacc.purview.azure.com/datagovernance/catalog"
ATLAS  = "https://prviewacc.purview.azure.com/catalog/api/atlas/v2"
VER    = "2025-09-15-preview"


def _req(method, path, body=None, base=BASE, retries=3):
    url = f"{base}{path}"
    for attempt in range(retries):
        try:
            kwargs = {"headers": H, "timeout": 30}
            if body is not None:
                kwargs["json"] = body
            r = getattr(requests, method)(url, **kwargs)
            return r.status_code, (r.json() if r.content else {})
        except requests.exceptions.SSLError as e:
            if attempt < retries - 1:
                print(f"    SSL error, retry {attempt+1}/{retries}...")
                time.sleep(2)
            else:
                return -1, {"error": str(e)}
        except Exception as e:
            return -1, {"error": str(e)}


def get(path, base=BASE):        return _req("get", path, base=base)
def post(path, body, base=BASE): return _req("post", path, body=body, base=base)
def put(path, body, base=BASE):  return _req("put", path, body=body, base=base)


def link_product_term(prod_id, term_id):
    """Try known relationship payload variants for linking terms to products."""
    payloads = [
        (
            f"/dataProducts/{prod_id}/relationships?api-version={VER}&entityType=TERM",
            {
                "description": "Automated glossary-to-product link",
                "relationshipType": "Related",
                "assetId": prod_id,
                "entityId": term_id,
            },
        ),
        (
            f"/dataProducts/{prod_id}/relationships?api-version={VER}",
            {
                "entityType": "TERM",
                "relationship": {
                    "relationshipType": "Related",
                    "assetId": prod_id,
                    "entityId": term_id,
                },
            },
        ),
        (
            f"/dataProducts/{prod_id}/relationships?api-version={VER}",
            {
                "entityType": "TERM",
                "relationshipType": "Related",
                "assetId": prod_id,
                "entityId": term_id,
            },
        ),
    ]
    last = (None, {})
    for path, body in payloads:
        s, resp = post(path, body)
        last = (s, resp)
        if s in (200, 201, 204, 409):
            return True, s, resp
    return False, last[0], last[1]


def create_cde(name, description, domain_id, contacts):
    """Create CDE using required fields discovered from live API validation."""
    body = {
        "name": name,
        "description": description,
        "status": "Published",
        "dataType": "Text",
        "domain": domain_id,
        "contacts": contacts,
    }
    s, resp = post(f"/criticalDataElements?api-version={VER}", body)
    return (s in (200, 201, 409)), s, resp


print("=" * 70)
print("PURVIEW COMPREHENSIVE FIX v2")
print("=" * 70)

# ─── STEP 1: Get current state ──────────────────────────
print("\n[1/5] Loading current Purview state...")

s, resp = get(f"/businessDomains?api-version={VER}")
domains = {d["name"]: d["id"] for d in resp.get("value", [])}
print(f"  Domains: {list(domains.keys())}")

s, resp = get(f"/dataProducts?api-version={VER}")
products = {p["name"]: p for p in resp.get("value", [])}
print(f"  Products:")
for name, p in products.items():
    dom_id = p.get("domain", "NONE")
    dom_name = next((k for k, v in domains.items() if v == dom_id), dom_id)
    print(f"    - {name} -> {dom_name}")

# Reuse a known-good contacts shape from existing products for CDE creation.
DEFAULT_CONTACTS = None
for p in products.values():
    c = p.get("contacts")
    if isinstance(c, dict) and c.get("owner"):
        DEFAULT_CONTACTS = c
        break
if DEFAULT_CONTACTS is None:
    DEFAULT_CONTACTS = {"owner": [{"id": "9350a243-7bcf-4053-8f7e-996364f4de24", "description": "Creator"}]}

# ─── STEP 2: Update Data Product descriptions/metadata ──
print("\n[2/5] Updating Data Product metadata (using PUT)...")

DOMAIN_CLINICAL = domains.get("Klinisk Vård", "0a57cab0-2b5e-4e0d-bb9f-1643902355ec")
DOMAIN_RESEARCH = domains.get("Forskning & Genomik", "1f845461-e1e6-4189-80f0-74db9381fb48")
DOMAIN_INTEROP  = domains.get("Interoperabilitet & Standarder", "bdd4b57c-bcef-4435-b7ee-b86974319c7f")
DOMAIN_DATA     = domains.get("Data & Analytics", "f70887db-952b-471b-951e-6f001fdcc727")
DOMAIN_HEALTH   = domains.get("Halsosjukvard", domains.get("Hälsosjukvård", "e364cf58-4c85-4a27-80d5-1ace9c573c88"))

PRODUCT_UPDATES = {
    "Klinisk Patientanalys": {
        "description": "Patientdemografi, diagnoser (ICD-10), lakemedel (ATC), laboratorieprov (LOINC) och radiologiska studier (DICOM). Innehaller data for 10 000 patienter med fokus pa vardkvalitet och prediktiv analys.",
        "businessUse": "Patient 360 Dashboard, kvalitetsmätningar, prediktiv aterinlaggningsanalys och lakemedelsgranskning.",
        "endorsed": True,
        "audience": ["DataEngineer", "BIEngineer", "DataScientist", "DataAnalyst"],
    },
    "BrainChild Barncancerforskning": {
        "description": "Genomisk data fran barncancerpatienter: VCF-filer, CNV-analys, COSMIC varianter och BTB-protokoll. Integrerar med BrainChild biobank for avancerad barncancerforskning.",
        "businessUse": "Genomisk forskning, biomarkorupptackt, kliniska provningar och barncancerprotokoll.",
        "endorsed": True,
        "audience": ["DataScientist", "DataEngineer"],
    },
    "OMOP Forskningsdata": {
        "description": "Standardiserad forskningsdata enligt OMOP CDM v5.4 med SNOMED CT, ATC, LOINC och ICD-10. Anvands for observationsstudier, real-world evidence och population health analytics.",
        "businessUse": "Observationsstudier, real-world evidence, population health analytics och regulatoriska inlamningar.",
        "endorsed": True,
        "audience": ["DataScientist", "DataAnalyst"],
    },
}

for prod_name, updates in PRODUCT_UPDATES.items():
    if prod_name not in products:
        print(f"  - {prod_name}: not found")
        continue
    prod = products[prod_name]
    prod_id = prod["id"]
    body = {k: v for k, v in prod.items() if k not in ("systemData", "additionalProperties")}
    body.update(updates)
    s, resp = put(f"/dataProducts/{prod_id}?api-version={VER}", body)
    if s in (200, 201, 204):
        print(f"  OK  {prod_name}")
        if resp:
            products[prod_name] = resp
    else:
        print(f"  !! {prod_name}: {s} {str(resp)[:100]}")

# ─── STEP 3: Create Glossary Terms ─────────────────────
print("\n[3/5] Creating Glossary Terms in Unified Catalog...")

s, resp = get(f"/terms?api-version={VER}")
existing_terms = {}
if s == 200:
    for t in resp.get("value", []):
        tname = t.get("name", "")
        tid = t.get("id")
        if tname == "TestTerm_Delete" and tid:
            requests.delete(f"{BASE}/terms/{tid}?api-version={VER}", headers=H, timeout=30)
            print(f"  Deleted test term")
        else:
            existing_terms[tname] = tid
print(f"  Existing terms: {len(existing_terms)}")

GLOSSARY_TERMS = [
    {"name": "Patient",           "domain": DOMAIN_CLINICAL,  "abbr": "PAT",    "desc": "En person som mottar eller har mottagit halso- och sjukvard."},
    {"name": "Encounter",         "domain": DOMAIN_CLINICAL,  "abbr": "ENC",    "desc": "Sjukvardstilfalle eller vardkontakt mellan patient och vardgivare."},
    {"name": "Condition",         "domain": DOMAIN_CLINICAL,  "abbr": "COND",   "desc": "Medicinsk diagnos eller tillstand kodat enligt ICD-10."},
    {"name": "Medication",        "domain": DOMAIN_CLINICAL,  "abbr": "MED",    "desc": "Lakemedel ordinerat till patient, kodat enligt ATC-systemet."},
    {"name": "Observation",       "domain": DOMAIN_CLINICAL,  "abbr": "OBS",    "desc": "Klinisk observation inklusive laboratorievarden och vitala parametrar."},
    {"name": "DICOM",             "domain": DOMAIN_CLINICAL,  "abbr": "DICOM",  "desc": "Digital Imaging and Communications in Medicine - standard for medicinsk bilddata."},
    {"name": "ICD-10",            "domain": DOMAIN_CLINICAL,  "abbr": "ICD10",  "desc": "Internationell klassifikation av sjukdomar, version 10 (WHO)."},
    {"name": "LOINC",             "domain": DOMAIN_INTEROP,   "abbr": "LOINC",  "desc": "Logical Observation Identifiers Names and Codes - standard for kliniska observationer."},
    {"name": "SNOMED CT",         "domain": DOMAIN_INTEROP,   "abbr": "SNOMED", "desc": "Systematized Nomenclature of Medicine Clinical Terms - klinisk terminologi."},
    {"name": "ATC",               "domain": DOMAIN_CLINICAL,  "abbr": "ATC",    "desc": "Anatomical Therapeutic Chemical Classification System for lakemedel."},
    {"name": "FHIR R4",           "domain": DOMAIN_INTEROP,   "abbr": "FHIR",   "desc": "Fast Healthcare Interoperability Resources version 4 - HL7-standard for halsodata."},
    {"name": "HL7 FHIR",          "domain": DOMAIN_INTEROP,   "abbr": "HL7",    "desc": "Health Level Seven FHIR - internationell standard for utbyte av halsoinformation."},
    {"name": "VCF",               "domain": DOMAIN_RESEARCH,  "abbr": "VCF",    "desc": "Variant Call Format - filformat for genomiska varianter."},
    {"name": "NGS",               "domain": DOMAIN_RESEARCH,  "abbr": "NGS",    "desc": "Next Generation Sequencing - teknologi for DNA-sekvensering."},
    {"name": "Specimen",          "domain": DOMAIN_RESEARCH,  "abbr": "SPEC",   "desc": "Biologiskt prov eller biopsi taget fran en patient."},
    {"name": "OMOP CDM",          "domain": DOMAIN_RESEARCH,  "abbr": "OMOP",   "desc": "Observational Medical Outcomes Partnership Common Data Model for forskningsdata."},
    {"name": "Bronze Layer",      "domain": DOMAIN_DATA,                         "desc": "Radata-lagret i medallion-arkitekturen. Innehaller obearbetad kalldata."},
    {"name": "Silver Layer",      "domain": DOMAIN_DATA,                         "desc": "Validerat och rensat datalager i medallion-arkitekturen."},
    {"name": "Gold Layer",        "domain": DOMAIN_DATA,                         "desc": "Aggregerat och analysklart datalager i medallion-arkitekturen."},
    {"name": "Delta Lake",        "domain": DOMAIN_DATA,                         "desc": "Oppen kallkod ACID-transaktionsformat for data lakehouse."},
    {"name": "Data Lakehouse",    "domain": DOMAIN_DATA,                         "desc": "Dataarkitektur som kombinerar data lake och data warehouse."},
    {"name": "Personnummer",      "domain": DOMAIN_HEALTH,    "abbr": "PNR",    "desc": "Svenskt personnummer - unik identifierare for folkbokforda personer i Sverige."},
    {"name": "De-identification", "domain": DOMAIN_HEALTH,                       "desc": "Process for att ta bort personidentifierande information fran data."},
    {"name": "Cohort",            "domain": DOMAIN_RESEARCH,                     "desc": "Definierad grupp patienter med gemensamma karakteristika for forskning."},
    {"name": "Real-World Evidence","domain": DOMAIN_RESEARCH, "abbr": "RWE",    "desc": "Klinisk evidens baserad pa data fran verklig sjukvard (observationsstudier)."},
    {"name": "Medallion Architecture","domain": DOMAIN_DATA,                     "desc": "Datalagerarkitektur med bronze/silver/gold-skikt for progressiv datakvalitet."},
    {"name": "Data Governance",   "domain": DOMAIN_HEALTH,                       "desc": "Ramverk och processer for att sakerstalla datakvalitet, integritet och efterlevnad."},
    {"name": "Master Patient Index","domain": DOMAIN_HEALTH,  "abbr": "MPI",    "desc": "Centralt register som identifierar och lankar patientposter i olika system."},
    {"name": "Biobank",           "domain": DOMAIN_RESEARCH,                     "desc": "Samling av biologiska prover med tillhorande patientdata for forskning."},
    {"name": "GMS",               "domain": DOMAIN_RESEARCH,  "abbr": "GMS",    "desc": "Genomic Medicine Service - nationell genomiktjanst for diagnostik och forskning."},
]

created = 0
skipped = 0
failed = 0

for term in GLOSSARY_TERMS:
    name = term["name"]
    if name in existing_terms:
        skipped += 1
        continue
    body = {
        "name": name,
        "description": term.get("desc", ""),
        "status": "Published",
        "domain": term["domain"],
    }
    if "abbr" in term:
        body["abbreviation"] = term["abbr"]
    s, resp = post(f"/terms?api-version={VER}", body)
    if s in (200, 201):
        existing_terms[name] = resp.get("id")
        created += 1
    else:
        failed += 1
        print(f"  !! '{name}': {s} {str(resp)[:100]}")
    time.sleep(0.3)

print(f"  Terms: {created} created, {skipped} skipped, {failed} failed")
print(f"  Total in catalog: {len(existing_terms)}")

# ─── STEP 4: Link Terms to Data Products ───────────────
print("\n[4/5] Linking Glossary Terms to Data Products...")

PRODUCT_TERMS_MAP = {
    "Klinisk Patientanalys": [
        "Patient", "Encounter", "Condition", "Medication", "Observation",
        "DICOM", "ICD-10", "LOINC", "ATC", "SNOMED CT", "FHIR R4",
        "Personnummer", "Master Patient Index"
    ],
    "BrainChild Barncancerforskning": [
        "VCF", "NGS", "Specimen", "OMOP CDM", "De-identification",
        "Biobank", "GMS", "Cohort"
    ],
    "OMOP Forskningsdata": [
        "OMOP CDM", "SNOMED CT", "ICD-10", "LOINC", "ATC", "Cohort",
        "De-identification", "HL7 FHIR", "Real-World Evidence"
    ],
}

for prod_name, term_names in PRODUCT_TERMS_MAP.items():
    if prod_name not in products:
        continue
    prod_id = products[prod_name]["id"]
    linked = 0
    first_error = None
    for term_name in term_names:
        term_id = existing_terms.get(term_name)
        if not term_id:
            continue
        ok, s, resp = link_product_term(prod_id, term_id)
        if ok:
            linked += 1
        elif first_error is None:
            first_error = (term_name, s, str(resp)[:200])
        time.sleep(0.2)
    print(f"  OK  {prod_name}: {linked}/{len(term_names)} terms linked")
    if first_error:
        tname, estatus, emsg = first_error
        print(f"      First link error ({tname}): {estatus} {emsg}")

# ─── STEP 5: Create Critical Data Elements ─────────────
print("\n[5/5] Creating Critical Data Elements...")

s, resp = get(f"/criticalDataElements?api-version={VER}")
existing_cdes = {}
if s == 200:
    for c in resp.get("value", []):
        existing_cdes[c.get("name")] = c.get("id")
print(f"  Existing CDEs: {len(existing_cdes)}")

CDES = [
    {"name": "Patient Personnummer",  "desc": "Unikt identifieringsnummer for patienter i svenska vardregister.",      "domain": DOMAIN_HEALTH},
    {"name": "ICD-10 Diagnoskod",     "desc": "Diagnoskod enligt WHO:s ICD-10 klassifikationssystem.",                 "domain": DOMAIN_CLINICAL},
    {"name": "LOINC Laboratoriekod",  "desc": "Standardiserad kod for laboratorieprov och kliniska observationer.",    "domain": DOMAIN_INTEROP},
    {"name": "Genomisk Variant ID",   "desc": "Unik identifierare for genomisk variant (rsID eller HGVS-notation).",   "domain": DOMAIN_RESEARCH},
    {"name": "DICOM Study UID",       "desc": "Unikt identifieringsnummer for radiologisk studie i DICOM-format.",     "domain": DOMAIN_CLINICAL},
    {"name": "ATC Lakemedelskod",     "desc": "Anatomical Therapeutic Chemical kod for klassificering av lakemedel.",  "domain": DOMAIN_CLINICAL},
    {"name": "SNOMED CT Kod",         "desc": "Systematiserad terminologikod for kliniska begrepp.",                   "domain": DOMAIN_INTEROP},
]

cde_created = 0
for cde in CDES:
    if cde["name"] in existing_cdes:
        continue
    ok, s, resp = create_cde(cde["name"], cde["desc"], cde["domain"], DEFAULT_CONTACTS)
    if ok:
        existing_cdes[cde["name"]] = resp.get("id")
        cde_created += 1
        print(f"  OK  {cde['name']}")
    else:
        print(f"  !! {cde['name']}: {s} {str(resp)[:100]}")
    time.sleep(0.3)

print(f"  CDEs: {cde_created} created, total {len(existing_cdes)}")

# ─── SUMMARY ────────────────────────────────────────────
print("\n" + "=" * 70)
print("FIX COMPLETE - Final state:")
print("=" * 70)

s, resp = get(f"/dataProducts?api-version={VER}")
print("\nData Products:")
for p in resp.get("value", []):
    dom_id = p.get("domain", "NONE")
    dom_name = next((k for k, v in domains.items() if v == dom_id), dom_id)
    print(f"  - {p['name']} -> {dom_name}")

s, resp = get(f"/terms?api-version={VER}")
term_count = len(resp.get("value", [])) if s == 200 else 0
print(f"\nGlossary Terms: {term_count}")

s, resp = get(f"/criticalDataElements?api-version={VER}")
cde_count = len(resp.get("value", [])) if s == 200 else 0
print(f"Critical Data Elements: {cde_count}")

print("\nDone! Check Purview GUI: https://web.purview.azure.com")
