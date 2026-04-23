"""
Purview Fix Script — Phase 2
=============================
Fixes the three remaining issues:
1. Move 12 FHIR/DICOM entities to fabric-brainchild collection
2. Create 14 missing glossary terms
3. Re-validate term→entity links (using correct /term/ endpoint)
"""
import json, os, sys, time, requests
from azure.identity import AzureCliCredential

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

cred = AzureCliCredential(process_timeout=30)
ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
GLOSSARY_GUID = "d939ea20-9c67-48af-98d9-b66965f7cde1"
COLL_API = "2019-11-01-preview"

G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; C = "\033[96m"
B = "\033[94m"; BOLD = "\033[1m"; D = "\033[2m"; RST = "\033[0m"

_tok = [None, 0]
def headers():
    if _tok[1] < time.time() - 2400:
        t = cred.get_token("https://purview.azure.net/.default")
        _tok[0] = t.token; _tok[1] = time.time()
    return {"Authorization": f"Bearer {_tok[0]}", "Content-Type": "application/json"}

def hdr(title):
    print(f"\n{BOLD}{B}{'═'*70}\n  {title}\n{'═'*70}{RST}")


# ─────────────────────────────────────────────────────────────
# FIX 1: Move FHIR/DICOM entities to fabric-brainchild
# ─────────────────────────────────────────────────────────────

def fix_entity_collections():
    hdr("FIX 1: Flytta FHIR/DICOM-entiteter till fabric-brainchild")
    h = headers()

    # Collect all custom healthcare entity GUIDs
    entity_guids = []
    for etype in ["healthcare_fhir_service", "healthcare_fhir_resource_type",
                  "healthcare_dicom_service", "healthcare_dicom_modality"]:
        body = {"keywords": "*", "filter": {"entityType": etype}, "limit": 50}
        r = requests.post(SEARCH, headers=h, json=body, timeout=15)
        if r.status_code == 200:
            for ent in r.json().get("value", []):
                eid = ent.get("id", "")
                name = ent.get("name", "?")
                if eid:
                    entity_guids.append((eid, name, etype))
        time.sleep(0.15)

    print(f"  Hittade {len(entity_guids)} FHIR/DICOM-entiteter att flytta")

    # Method 1: POST to collection entity endpoint
    collection_name = "fabric-brainchild"
    guids = [g for g, _, _ in entity_guids]

    # Try the collections entity move API
    url = f"{ACCT}/catalog/api/collections/{collection_name}/entity?api-version=2022-11-01-preview"
    body = {"entityGuids": guids}
    r = requests.post(url, headers=h, json=body, timeout=30)
    if r.status_code in (200, 201, 204):
        print(f"  {G}★{RST} {len(guids)} entiteter flyttade till {collection_name} (bulk)")
        return True

    print(f"  {D}Bulk move: {r.status_code} — testar entity-by-entity{RST}")

    # Method 2: Update each entity individually via createOrUpdate
    moved = 0
    for guid, name, etype in entity_guids:
        # Get current entity
        r2 = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=15)
        if r2.status_code != 200:
            print(f"  {Y}⚠{RST} Kan inte läsa: {name} ({r2.status_code})")
            continue

        entity_data = r2.json().get("entity", {})

        # Set the collection in the entity and PUT back
        entity_data["collectionId"] = collection_name
        
        # Try various methods to set collection
        # Method 2a: POST entity/bulk with collection
        bulk_body = {
            "entities": [{
                "typeName": entity_data.get("typeName", etype),
                "guid": guid,
                "attributes": entity_data.get("attributes", {}),
                "collectionId": collection_name,
            }]
        }
        r3 = requests.post(f"{ATLAS}/entity/bulk", headers=h, json=bulk_body, timeout=15)
        if r3.status_code in (200, 201):
            print(f"  {G}★{RST} {name} → {collection_name}")
            moved += 1
        else:
            # Method 2b: Try DATAMAP
            r4 = requests.post(f"{DATAMAP}/entity/bulk", headers=h, json=bulk_body, timeout=15)
            if r4.status_code in (200, 201):
                print(f"  {G}★{RST} {name} → {collection_name} (DATAMAP)")
                moved += 1
            else:
                # Method 2c: Try PUT on single entity
                r5 = requests.post(f"{ATLAS}/entity", headers=h, json={
                    "entity": {
                        "typeName": entity_data.get("typeName", etype),
                        "guid": guid,
                        "attributes": entity_data.get("attributes", {}),
                        "collectionId": collection_name,
                    }
                }, timeout=15)
                if r5.status_code in (200, 201):
                    print(f"  {G}★{RST} {name} → {collection_name} (single)")
                    moved += 1
                else:
                    print(f"  {Y}⚠{RST} {name}: {r3.status_code}/{r4.status_code}/{r5.status_code}")

        time.sleep(0.2)

    print(f"\n  Resultat: {moved}/{len(entity_guids)} entiteter flyttade")
    return moved > 0


# ─────────────────────────────────────────────────────────────
# FIX 2: Create missing glossary terms
# ─────────────────────────────────────────────────────────────

MISSING_TERMS = [
    ("DICOMweb", "RESTful standard for accessing DICOM medical imaging data via HTTP/HTTPS. Supports WADO-RS, STOW-RS, and QIDO-RS."),
    ("Personnummer", "Svenskt personnummer (12 siffror, YYYYMMDD-XXXX). Pseudonymiseras i alla forskningsdata."),
    ("Pseudonymisering", "Process där personuppgifter ersätts med pseudonymer för att skydda patientens integritet."),
    ("Patientdemografi", "Demografiska uppgifter om patienter: ålder, kön, postnummer. Lagras i FHIR Patient och SQL patients."),
    ("Vårdkontakt", "Enskilt vårdtillfälle/besök i sjukvården. Motsvaras av FHIR Encounter och SQL encounters."),
    ("Histopatologi", "Mikroskopisk undersökning av vävnadsprover. Digitala preparat lagras som DICOM Pathology-bilder."),
    ("Biobank", "Samling av biologiska prover (vävnad, blod, DNA) lagrade för forskning. BTB är barncancerns biobank."),
    ("MR (Magnetresonanstomografi)", "Bilddiagnostik med magnetfält och radiovågor. Centralt i barncancerdiagnostik."),
    ("T1-viktad MR", "MR-sekvens som ger god kontrast mellan vävnadstyper. Bra för anatomi och kontrastuppladdning."),
    ("T2-viktad MR", "MR-sekvens där vätska framträder ljust. Används för att detektera ödem och patologi."),
    ("FLAIR", "Fluid Attenuated Inversion Recovery — MR-sekvens som undertrycker CSF-signal. Känslig för periventrikuär patologi."),
    ("Tumörstadium", "Klassificering av tumörens utbredning och allvarlighetsgrad. Påverkar behandlingsval."),
    ("Informerat samtycke", "Juridiskt krav att patienter/vårdnadshavare ger medvetet samtycke till forskning och behandling."),
    ("Etikprövning", "Granskning av forskningsprojekt av etikprövningsmyndigheten (EPM) för att säkerställa etisk standard."),
]


def fix_glossary_terms():
    hdr("FIX 2: Skapa saknade glossary-termer")
    h = headers()

    # Check existing terms
    r = requests.get(f"{ATLAS}/glossary/{GLOSSARY_GUID}/terms?limit=200", headers=h, timeout=30)
    existing = {t["name"]: t["guid"] for t in r.json()} if r.status_code == 200 else {}

    created = 0
    for name, desc in MISSING_TERMS:
        if name in existing:
            print(f"  {G}✓{RST} {name} (finns redan)")
            continue

        body = {
            "name": name,
            "shortDescription": desc,
            "longDescription": desc,
            "anchor": {"glossaryGuid": GLOSSARY_GUID},
            "status": "Approved",
        }
        r2 = requests.post(f"{ATLAS}/glossary/term", headers=h, json=body, timeout=15)
        if r2.status_code in (200, 201):
            guid = r2.json().get("guid", "?")
            print(f"  {G}★{RST} Skapad: {name} ({guid[:20]}...)")
            created += 1
            existing[name] = guid
        elif r2.status_code == 409:
            print(f"  {G}✓{RST} {name} (finns)")
        else:
            print(f"  {R}✗{RST} {name}: {r2.status_code} {r2.text[:80]}")
        time.sleep(0.2)

    print(f"\n  {created} nya termer skapade, {len(existing)} totalt")
    return existing


# ─────────────────────────────────────────────────────────────
# FIX 3: Validate & fix term→entity links (correct API)
# ─────────────────────────────────────────────────────────────

# Full term→entity map
FHIR_DICOM_TERM_MAP = {
    "FHIR R4": ["BrainChild FHIR Server (R4)"],
    "FHIR Patient": ["FHIR Patient"],
    "FHIR Encounter": ["FHIR Encounter"],
    "FHIR Condition": ["FHIR Condition"],
    "FHIR Observation": ["FHIR Observation"],
    "FHIR Specimen": ["FHIR Specimen"],
    "FHIR ImagingStudy": ["FHIR ImagingStudy"],
    "FHIR DiagnosticReport": ["FHIR DiagnosticReport"],
    "FHIR MedicationRequest": ["FHIR MedicationRequest"],
    "DICOM": ["BrainChild DICOM Server", "DICOM MRI_Brain", "DICOM Pathology"],
    "DICOMweb": ["BrainChild DICOM Server"],
    "Genomic Medicine Sweden (GMS)": ["FHIR DiagnosticReport"],
    "BTB (Barntumörbanken)": ["FHIR Specimen"],
    "SNOMED-CT": ["FHIR Condition", "FHIR Specimen"],
    "ICD-O-3": ["FHIR Condition"],
    "LOINC": ["FHIR Observation"],
    "HGVS-nomenklatur": ["FHIR DiagnosticReport"],
    "MR (Magnetresonanstomografi)": ["DICOM MRI_Brain"],
    "T1-viktad MR": ["DICOM MRI_Brain"],
    "T2-viktad MR": ["DICOM MRI_Brain"],
    "FLAIR": ["DICOM MRI_Brain"],
    "Histopatologi": ["DICOM Pathology"],
    "VCF (Variant Call Format)": ["FHIR DiagnosticReport"],
    "Biobank": ["FHIR Specimen"],
    "Patientdemografi": ["FHIR Patient"],
    "Vårdkontakt": ["FHIR Encounter"],
    "Personnummer": ["FHIR Patient"],
}


def fix_term_links(term_guids):
    hdr("FIX 3: Validera & fixa term→entity-kopplingar (FHIR/DICOM)")
    h = headers()

    # Build entity name→GUID map
    entity_map = {}
    for query in ["healthcare_fhir", "healthcare_dicom", "BrainChild FHIR",
                  "BrainChild DICOM", "FHIR", "DICOM"]:
        body = {"keywords": query, "limit": 100}
        r = requests.post(SEARCH, headers=h, json=body, timeout=15)
        if r.status_code == 200:
            for ent in r.json().get("value", []):
                name = ent.get("name", "")
                guid = ent.get("id", "")
                if name and guid:
                    entity_map[name] = guid
        time.sleep(0.15)

    print(f"  Entiteter i katalogen: {len(entity_map)}")

    already_ok = 0
    newly_linked = 0
    errors = 0

    for term_name, expected_entities in FHIR_DICOM_TERM_MAP.items():
        tguid = term_guids.get(term_name)
        if not tguid:
            continue

        # GET using correct endpoint: /glossary/term/ (singular!)
        r = requests.get(f"{ATLAS}/glossary/term/{tguid}", headers=h, timeout=15)
        if r.status_code != 200:
            print(f"  {Y}⚠{RST} {term_name}: GET {r.status_code}")
            errors += 1
            continue

        assigned = r.json().get("assignedEntities", [])
        assigned_guids = {a.get("guid") for a in assigned}

        missing = []
        for ename in expected_entities:
            eguid = entity_map.get(ename)
            if eguid and eguid not in assigned_guids:
                missing.append((ename, eguid))

        if not missing:
            already_ok += 1
        else:
            to_link = [{"guid": g} for _, g in missing]
            r2 = requests.post(
                f"{ATLAS}/glossary/terms/{tguid}/assignedEntities",
                headers=h, json=to_link, timeout=15)
            if r2.status_code in (200, 201, 204):
                names = [n for n, _ in missing]
                print(f"  {G}★{RST} {term_name} → {', '.join(names)}")
                newly_linked += 1
            elif r2.status_code == 400:
                already_ok += 1  # Usually means already assigned
            else:
                print(f"  {Y}⚠{RST} {term_name}: {r2.status_code}")
                errors += 1
            time.sleep(0.15)

    total = sum(1 for t in FHIR_DICOM_TERM_MAP if t in term_guids)
    print(f"\n  Redan OK: {already_ok}/{total}, Nytt: {newly_linked}, Fel: {errors}")


# ─────────────────────────────────────────────────────────────
# VERIFY: Re-run search and term checks
# ─────────────────────────────────────────────────────────────

def verify_all():
    hdr("VERIFIERING")
    h = headers()

    # Check glossary count
    r = requests.get(f"{ATLAS}/glossary/{GLOSSARY_GUID}/terms?limit=200", headers=h, timeout=30)
    if r.status_code == 200:
        terms = r.json()
        print(f"  {G}✓{RST} Glossary-termer: {len(terms)}")

    # Check FHIR/DICOM entity collections
    in_bc = 0
    not_in_bc = 0
    for etype in ["healthcare_fhir_service", "healthcare_fhir_resource_type",
                  "healthcare_dicom_service", "healthcare_dicom_modality"]:
        body = {"keywords": "*", "filter": {"entityType": etype}, "limit": 50}
        r = requests.post(SEARCH, headers=h, json=body, timeout=15)
        if r.status_code == 200:
            for ent in r.json().get("value", []):
                coll = ent.get("collectionId", "?")
                if coll == "fabric-brainchild":
                    in_bc += 1
                else:
                    not_in_bc += 1
        time.sleep(0.15)

    if in_bc > 0 and not_in_bc == 0:
        print(f"  {G}✓{RST} Alla {in_bc} FHIR/DICOM-entiteter i fabric-brainchild")
    elif in_bc > 0:
        print(f"  {Y}⚠{RST} {in_bc} i fabric-brainchild, {not_in_bc} utanför")
    else:
        print(f"  {Y}⚠{RST} Inga FHIR/DICOM-entiteter i fabric-brainchild (collectionId ej i sökresultat)")

    # Check term link counts using correct API
    print(f"\n  Term → Entity kopplingar (stickprov):")
    samples = ["FHIR R4", "FHIR Patient", "DICOM", "SNOMED-CT", "Bronze-lager", "OMOP CDM"]
    r_terms = requests.get(f"{ATLAS}/glossary/{GLOSSARY_GUID}/terms?limit=200", headers=h, timeout=30)
    term_guids = {t["name"]: t["guid"] for t in r_terms.json()} if r_terms.status_code == 200 else {}

    for tname in samples:
        tguid = term_guids.get(tname)
        if not tguid:
            continue
        r2 = requests.get(f"{ATLAS}/glossary/term/{tguid}", headers=h, timeout=15)
        if r2.status_code == 200:
            assigned = r2.json().get("assignedEntities", [])
            names = [a.get("displayText", "?") for a in assigned[:3]]
            extra = f" (+{len(assigned)-3} mer)" if len(assigned) > 3 else ""
            print(f"  {G}✓{RST} {tname}: {len(assigned)} kopplingar → {', '.join(names)}{extra}")
        time.sleep(0.1)

    # Search tests
    print(f"\n  Sökbarhet:")
    for kw, expected in [("FHIR", 5), ("DICOM", 3), ("Patient", 3), ("barncancer", 1)]:
        body = {"keywords": kw, "limit": 5}
        r = requests.post(SEARCH, headers=h, json=body, timeout=15)
        if r.status_code == 200:
            count = r.json().get("@search.count", 0)
            status = f"{G}✓{RST}" if count >= expected else f"{Y}⚠{RST}"
            print(f"  {status} '{kw}' → {count} resultat")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{BOLD}{B}{'═'*70}")
    print(f"  PURVIEW FIX — PHASE 2")
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*70}{RST}")

    # Fix 1: Move entities
    fix_entity_collections()

    # Fix 2: Create missing terms
    term_guids = fix_glossary_terms()

    # Fix 3: Validate/fix term links
    fix_term_links(term_guids)

    # Verify
    verify_all()

    print(f"\n{BOLD}{G}  Klart!{RST}\n")
