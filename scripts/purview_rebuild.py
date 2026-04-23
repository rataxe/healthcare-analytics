"""
Purview REBUILD — Complete setup with governance domains and data products.

Enhanced with patterns from Microsoft-Purview-Unified-Catalog repo:
  - New Purview Unified Catalog API (2025-09-15-preview) for domains/data products
  - Term → Data Product relationship linking
  - PII classifications on sensitive columns
  - Labels/tags for discoverability
  - Owner/expert contacts on key assets
  - Data product status management (Draft → Published)

Creates:
  1. Single unified collection hierarchy under prviewacc
  2. Governance Domains (Klinisk Vård, Barncancerforskning) via new API
  3. Data Products per domain with term links
  4. Business Glossary with terms and categories
  5. Data sources (SQL + Fabric) registered in correct collections
  6. Scans for both data sources
  7. Glossary term → entity mappings
  8. PII classifications on sensitive columns
  9. Labels/tags on key entities
  10. Owner/expert contacts

Prerequisites: Run purview_reset.py first to clean existing data.
"""
import requests, json, time, sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

# ── CONFIG ──
cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

sess = requests.Session()
retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
sess.mount("https://", HTTPAdapter(max_retries=retry))

ACCT = "https://prviewacc.purview.azure.com"
TENANT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
SCAN_EP = f"{TENANT_EP}/scan"
SCAN_API = "2022-07-01-preview"
COLL_API = "2019-11-01-preview"

# New Purview Unified Catalog API (from Microsoft-Purview-Unified-Catalog repo)
DG_API = "2025-09-15-preview"
DG_BASE = f"{TENANT_EP}/datagovernance/catalog"

# Fabric workspace IDs
HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"   # Healthcare-Analytics
BC_WS  = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"    # BrainChild-Demo

# SQL Server
SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DB = "HealthcareAnalyticsDB"
SQL_RG = "rg-healthcare-analytics"
SQL_SUB = "5b44c9f3-bbe7-464c-aa3e-562726a12004"

def sep(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

def api_ok(r, label):
    if r.status_code in (200, 201, 204):
        print(f"  ✅ {label}: {r.status_code}")
        return True
    else:
        print(f"  ❌ {label}: {r.status_code} — {r.text[:200]}")
        return False

def refresh_token():
    """Refresh the auth token (sessions longer than 1 hour)."""
    global token, h
    token = cred.get_token("https://purview.azure.net/.default").token
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


# ══════════════════════════════════════════════════════════════════════
# STEP 1: CLEAN UP OLD COLLECTIONS
# ══════════════════════════════════════════════════════════════════════
sep("1. CLEANING OLD COLLECTIONS")

r = sess.get(f"{ACCT}/account/collections?api-version={COLL_API}", headers=h, timeout=30)
existing = r.json().get("value", []) if r.status_code == 200 else []
root_names = {"prviewacc"}

# Build parent→children map for deletion order
children_map = {}
for c in existing:
    parent = c.get("parentCollection", {}).get("referenceName")
    if parent:
        children_map.setdefault(parent, []).append(c["name"])

def leaf_first_order(colls, cmap):
    order = []
    visited = set()
    def visit(name):
        if name in visited: return
        visited.add(name)
        for child in cmap.get(name, []):
            visit(child)
        if name not in root_names:
            order.append(name)
    for c in colls:
        visit(c["name"])
    return order

delete_order = leaf_first_order(existing, children_map)
for coll_name in delete_order:
    r = sess.delete(f"{ACCT}/account/collections/{coll_name}?api-version={COLL_API}", headers=h, timeout=30)
    status = "✅" if r.status_code in (200, 204) else f"⚠️ {r.status_code}"
    print(f"  DEL {coll_name}: {status}")
    time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════
# STEP 2: CREATE COLLECTION HIERARCHY (under prviewacc root)
# ══════════════════════════════════════════════════════════════════════
sep("2. CREATING COLLECTION HIERARCHY")

collections_to_create = [
    # (name, friendlyName, parent)
    ("halsosjukvard",    "Hälsosjukvård",       "prviewacc"),
    ("sql-databases",    "SQL Databases",        "halsosjukvard"),
    ("fabric-analytics", "Fabric Analytics",     "halsosjukvard"),
    ("barncancer",       "Barncancerforskning",  "prviewacc"),
    ("fabric-brainchild","Fabric BrainChild",    "barncancer"),
]

for name, friendly, parent in collections_to_create:
    body = {
        "name": name,
        "friendlyName": friendly,
        "parentCollection": {"referenceName": parent}
    }
    r = sess.put(
        f"{ACCT}/account/collections/{name}?api-version={COLL_API}",
        headers=h, json=body, timeout=30
    )
    api_ok(r, f"{friendly} ({name}) → {parent}")
    time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════
# STEP 3: REGISTER DATA SOURCES
# ══════════════════════════════════════════════════════════════════════
sep("3. REGISTERING DATA SOURCES")

# SQL Database
sql_body = {
    "kind": "AzureSqlDatabase",
    "name": "sql-hca-demo",
    "properties": {
        "serverEndpoint": SQL_SERVER,
        "resourceGroup": SQL_RG,
        "subscriptionId": SQL_SUB,
        "location": "swedencentral",
        "resourceName": "sql-hca-demo",
        "collection": {"referenceName": "sql-databases", "type": "CollectionReference"}
    }
}
r = sess.put(
    f"{SCAN_EP}/datasources/sql-hca-demo?api-version={SCAN_API}",
    headers=h, json=sql_body, timeout=30
)
api_ok(r, "SQL datasource 'sql-hca-demo' → sql-databases")

# Fabric (Power BI)
fabric_body = {
    "kind": "PowerBI",
    "name": "Fabric",
    "properties": {
        "tenant": "71c4b6d5-0065-4c6c-a125-841a582754eb",
        "collection": {"referenceName": "fabric-analytics", "type": "CollectionReference"}
    }
}
r = sess.put(
    f"{SCAN_EP}/datasources/Fabric?api-version={SCAN_API}",
    headers=h, json=fabric_body, timeout=30
)
api_ok(r, "Fabric datasource 'Fabric' → fabric-analytics")


# ══════════════════════════════════════════════════════════════════════
# STEP 4: CREATE SCANS
# ══════════════════════════════════════════════════════════════════════
sep("4. CREATING SCANS")

# SQL scan — use MSI for AAD-only SQL server
sql_scan_kinds = ["AzureSqlDatabaseMsiScan", "AzureSqlDatabaseCredentialScan"]
sql_scan_ok = False
for scan_kind in sql_scan_kinds:
    sql_scan = {
        "kind": scan_kind,
        "name": "healthcare-scan",
        "properties": {
            "databaseName": SQL_DB,
            "serverEndpoint": SQL_SERVER,
            "scanRulesetName": "AzureSqlDatabase",
            "scanRulesetType": "System",
            "collection": {"referenceName": "sql-databases", "type": "CollectionReference"}
        }
    }
    r = sess.put(
        f"{SCAN_EP}/datasources/sql-hca-demo/scans/healthcare-scan?api-version={SCAN_API}",
        headers=h, json=sql_scan, timeout=30
    )
    if r.status_code in (200, 201):
        api_ok(r, f"SQL scan 'healthcare-scan' ({scan_kind})")
        sql_scan_ok = True
        break
    else:
        print(f"  ⏭️  SQL scan kind '{scan_kind}': {r.status_code} — trying next")
if not sql_scan_ok:
    print(f"  ❌ SQL scan: no valid scan kind accepted")
    print(f"     Create scan manually in Purview portal → sql-hca-demo → New scan")

# Fabric scans — try MSI first, fall back to delegated
fabric_scan_kinds = ["PowerBIMsiScan", "PowerBIDelegatedScan"]
for scan_name, scan_coll, ws_label in [
    ("Scan-HCA", "fabric-analytics", "Healthcare-Analytics"),
    ("Scan-BrainChild", "fabric-brainchild", "BrainChild"),
]:
    scan_ok = False
    for scan_kind in fabric_scan_kinds:
        fb_scan = {
            "kind": scan_kind,
            "name": scan_name,
            "properties": {
                "includePersonalWorkspaces": False,
                "collection": {"referenceName": scan_coll, "type": "CollectionReference"}
            }
        }
        r = sess.put(
            f"{SCAN_EP}/datasources/Fabric/scans/{scan_name}?api-version={SCAN_API}",
            headers=h, json=fb_scan, timeout=30
        )
        if r.status_code in (200, 201):
            api_ok(r, f"Fabric scan '{scan_name}' ({scan_kind}) → {ws_label}")
            scan_ok = True
            break
        else:
            print(f"  ⏭️  Fabric scan '{scan_name}' kind '{scan_kind}': {r.status_code} — trying next")
    if not scan_ok:
        print(f"  ❌ Fabric scan '{scan_name}': no valid scan kind accepted")
        print(f"     Create scan manually in Purview portal → Fabric → New scan → {ws_label}")


# ══════════════════════════════════════════════════════════════════════
# STEP 5: RUN SCANS
# ══════════════════════════════════════════════════════════════════════
sep("5. TRIGGERING SCANS")

for ds, scan in [("sql-hca-demo", "healthcare-scan"), ("Fabric", "Scan-HCA"), ("Fabric", "Scan-BrainChild")]:
    r = sess.post(
        f"{SCAN_EP}/datasources/{ds}/scans/{scan}/runs/run-{int(time.time())}?api-version={SCAN_API}",
        headers=h, json={}, timeout=30
    )
    status = "✅ triggered" if r.status_code in (200, 201, 202) else f"❌ {r.status_code}"
    print(f"  {ds}/{scan}: {status}")
    time.sleep(1)


# ══════════════════════════════════════════════════════════════════════
# STEP 6: CREATE BUSINESS GLOSSARY
# ══════════════════════════════════════════════════════════════════════
sep("6. CREATING BUSINESS GLOSSARY")

glossary_body = {
    "name": "Sjukvårdstermer",
    "qualifiedName": "sjukvardstermer",
    "longDescription": "Komplett affärsordlista för healthcare-analytics och BrainChild-FHIR-plattformarna. Omfattar kliniska standarder (ICD-10, ATC, OMOP, FHIR), dataarkitektur (Medallion, Bronze/Silver/Gold) och barncancerforskningsbegrepp (genomik, DICOM, GMS, SBCR)."
}
r = sess.post(f"{ATLAS}/glossary", headers=h, json=glossary_body, timeout=30)
if r.status_code in (200, 201):
    glossary = r.json()
    GLOSSARY_GUID = glossary["guid"]
    print(f"  ✅ Glossary '{glossary['name']}' created (guid={GLOSSARY_GUID[:12]}...)")
else:
    print(f"  ❌ Glossary create failed: {r.status_code} — {r.text[:200]}")
    # Try to find existing
    r2 = sess.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    if r2.status_code == 200:
        for g in r2.json():
            if "sjukvard" in g["name"].lower() or "kund" in g["name"].lower():
                GLOSSARY_GUID = g["guid"]
                print(f"  ↳ Using existing glossary '{g['name']}' (guid={GLOSSARY_GUID[:12]}...)")
                break
        else:
            GLOSSARY_GUID = r2.json()[0]["guid"] if r2.json() else None
            print(f"  ↳ Using first available glossary")
    if not GLOSSARY_GUID:
        print("  FATAL: No glossary available")
        sys.exit(1)


# ══════════════════════════════════════════════════════════════════════
# STEP 7: CREATE GLOSSARY CATEGORIES (organisering)
# ══════════════════════════════════════════════════════════════════════
sep("7. CREATING GLOSSARY CATEGORIES")

categories = {
    "Kliniska Standarder": "ICD-10, ATC, SNOMED-CT — medicinska klassificeringsystem",
    "Interoperabilitet": "FHIR R4, OMOP CDM — datautbytesstandard",
    "Dataarkitektur": "Medallion, Bronze/Silver/Gold — lagerskikt",
    "Klinisk Data": "Patienter, vårdbesök, diagnoser, labresultat, medicinering",
    "Barncancerforskning": "Genomik, bilddiagnostik, tumörbanken, register",
}
cat_guids = {}

for name, desc in categories.items():
    body = {
        "name": name,
        "shortDescription": desc,
        "anchor": {"glossaryGuid": GLOSSARY_GUID}
    }
    r = sess.post(f"{ATLAS}/glossary/category", headers=h, json=body, timeout=30)
    if r.status_code in (200, 201):
        cat_guids[name] = r.json()["guid"]
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}: {r.status_code}")
    time.sleep(0.2)


# ══════════════════════════════════════════════════════════════════════
# STEP 8: CREATE ALL GLOSSARY TERMS
# ══════════════════════════════════════════════════════════════════════
sep("8. CREATING GLOSSARY TERMS")

# Terms organized by category
term_defs = [
    # ── Kliniska Standarder ──
    ("ICD-10", "Kliniska Standarder",
     "International Classification of Diseases, 10th Revision. WHO:s diagnoskodssystem. Används i diagnoses-tabellen (icd10_code, icd10_description).",
     "ICD-10 är den internationella standarden för att klassificera sjukdomar och hälsorelaterade tillstånd."),

    ("ATC-klassificering", "Kliniska Standarder",
     "Anatomisk Terapeutisk Kemisk klassificering (WHO). Används i medications-tabellen (atc_code, drug_name).",
     "ATC-systemet klassificerar läkemedel i grupper baserat på organ/system, terapeutisk effekt och kemisk substans."),

    ("Skyddad hälsoinformation (PHI)", "Kliniska Standarder",
     "Protected Health Information — personuppgifter som kräver GDPR/patientdatalagen-skydd. Inkluderar patient_id, birth_date, encounter_id.",
     "PHI kräver kryptering, åtkomstkontroll och loggning enligt GDPR och Patientdatalagen (PDL)."),

    ("Svenskt personnummer", "Kliniska Standarder",
     "Unikt 12-siffrigt identifikationsnummer (YYYYMMDD-XXXX). Representeras som postal_code (pseudonymiserat) i patients-tabellen.",
     "Personnummer är det nationella identifikationsnumret i Sverige."),

    # ── Interoperabilitet ──
    ("FHIR R4", "Interoperabilitet",
     "Fast Healthcare Interoperability Resources Release 4. HL7-standarden för hälsodatautbyte. Mappas till alla 5 SQL-tabeller.",
     "FHIR R4 är den dominerande standarden för API-baserat utbyte av hälsodata globalt."),

    ("FHIR Patient", "Interoperabilitet",
     "FHIR-resurs som representerar patientdemografi. Mappas till patients-tabellen.",
     "Patient-resursen innehåller namn, födelsedatum, kön, adress och kontaktuppgifter."),

    ("FHIR Encounter", "Interoperabilitet",
     "FHIR-resurs för vårdkontakter. Mappas till encounters-tabellen.",
     "Encounter beskriver en interaktion mellan patient och vårdgivare."),

    ("FHIR Condition", "Interoperabilitet",
     "FHIR-resurs för diagnoser. Mappas till diagnoses-tabellen.",
     "Condition representerar kliniska tillstånd, problem eller diagnoser."),

    ("FHIR MedicationRequest", "Interoperabilitet",
     "FHIR-resurs för läkemedelsförskrivningar. Mappas till medications-tabellen.",
     "MedicationRequest beskriver ordinerade läkemedel med dosering och tidplan."),

    ("FHIR Observation", "Interoperabilitet",
     "FHIR-resurs för mätvärden och labresultat. Mappas till vitals_labs-tabellen.",
     "Observation används för vitalparametrar, labvärden och andra kliniska mätningar."),

    ("FHIR ImagingStudy", "Interoperabilitet",
     "FHIR-resurs som refererar till DICOM-bildstudier (MR, patologi). Används i BrainChild-plattformen.",
     "ImagingStudy kopplar FHIR-kontext till DICOM-serier och instanser."),

    ("FHIR Specimen", "Interoperabilitet",
     "FHIR-resurs för biologiska prover. Används i BrainChild BTB-modulen.",
     "Specimen beskriver provmaterial, insamlingsmetod och förvaringsvillkor."),

    ("OMOP CDM", "Interoperabilitet",
     "Observational Medical Outcomes Partnership Common Data Model. OHDSI:s standardiserade datamodell. Mappas till alla SQL-tabeller.",
     "OMOP CDM möjliggör storskalig observationell forskning genom standardiserade tabeller och vokabulärer."),

    ("OMOP Person", "Interoperabilitet",
     "OMOP-tabell för patientdemografi. Mappas till patients-tabellen.",
     "Person-tabellen lagrar unika identiteter med demografiska attribut."),

    ("OMOP Visit Occurrence", "Interoperabilitet",
     "OMOP-tabell för vårdbesök. Mappas till encounters-tabellen.",
     "Visit Occurrence fångar tidsperioden för patientens kontakt med vården."),

    ("OMOP Condition Occurrence", "Interoperabilitet",
     "OMOP-tabell för diagnoser. Mappas till diagnoses-tabellen.",
     "Condition Occurrence registrerar diagnostiserade tillstånd med start/slutdatum."),

    ("OMOP Drug Exposure", "Interoperabilitet",
     "OMOP-tabell för läkemedelsexponering. Mappas till medications-tabellen.",
     "Drug Exposure fångar all läkemedelsanvändning inkl. förskrivningar och administreringar."),

    ("OMOP Measurement", "Interoperabilitet",
     "OMOP-tabell för labvärden och mätningar. Mappas till vitals_labs-tabellen.",
     "Measurement lagrar strukturerade mätvärden med enheter och referensintervall."),

    # ── Dataarkitektur ──
    ("Medallion-arkitektur", "Dataarkitektur",
     "Bronze → Silver → Gold datalagerskikt. Implementerad i Fabric Lakehouse med notebooks. Gold-lagret exponeras via vw_ml_encounters.",
     "Medallion-arkitekturen separerar rå, renad och affärsoptimerad data i tre lager."),

    ("Bronze-lager", "Dataarkitektur",
     "Rådatalager — ingestion från SQL utan transformation. Lagras i bronze_lakehouse (Fabric).",
     "Bronze-lagret bevarar originaldata för spårbarhet och möjlighet till omprocessering."),

    ("Silver-lager", "Dataarkitektur",
     "Renat och normaliserat datalager med feature engineering. Lagras i silver_lakehouse (Fabric).",
     "Silver-lagret innehåller validerad, deduplicerad och standardiserad data."),

    ("Gold-lager", "Dataarkitektur",
     "Affärsoptimerat lager med ML-features och aggregeringar. Lagras i gold_lakehouse (Fabric) och vw_ml_encounters (SQL).",
     "Gold-lagret levererar dataprodukter redo för analys, ML och rapportering."),

    # ── Klinisk Data ──
    ("Vårdtid (LOS)", "Klinisk Data",
     "Length of Stay — antal dagar per vårdbesök. Beräknas som DATEDIFF(admission_date, discharge_date). Kolumn: encounters.los_days.",
     "Vårdtid är en central kvalitetsindikator och prediktorvariabel i ML-modeller."),

    ("Återinläggningsrisk", "Klinisk Data",
     "Risk för återinläggning inom 30 dagar. Kolumn: encounters.readmission_30d. Målvariabel i ML-modellen.",
     "30-dagars återinläggning är ett viktigt utfallsmått för vårdkvalitet."),

    ("Vitalparametrar", "Klinisk Data",
     "Fysiologiska mätvärden: systolic_bp, diastolic_bp, heart_rate, oxygen_saturation, temperature. Lagras i vitals_labs.",
     "Vitalparametrar mäts rutinmässigt vid varje vårdkontakt."),

    ("Labresultat", "Klinisk Data",
     "Laboratorieanalyser: hemoglobin_g, wbc_count, glucose_mmol, creatinine_umol, bmi, weight_kg. Lagras i vitals_labs.",
     "Labresultat är avgörande för diagnos, behandlingsbeslut och prediktion."),

    # ── Barncancerforskning ──
    ("DICOM", "Barncancerforskning",
     "Digital Imaging and Communications in Medicine. Standard för medicinsk bildhantering. BrainChild lagrar MR- och patologidata i DICOM-format.",
     "DICOM är den universella standarden för medicinsk bildkommunikation."),

    ("Genomic Medicine Sweden (GMS)", "Barncancerforskning",
     "Nationell infrastruktur för genomisk medicin. GMS-modulen i BrainChild hanterar DiagnosticReport, Observation och Patient (FHIR-format).",
     "GMS levererar precisionsmedicin genom helgenomsekvensering och klinisk genetik."),

    ("VCF (Variant Call Format)", "Barncancerforskning",
     "Standardformat för genomiska varianter. Lagras i BrainChild BTB-modulen med tillhörande metadata.",
     "VCF-filer beskriver genetiska varianter identifierade genom sekvensering."),

    ("BTB (Barntumörbanken)", "Barncancerforskning",
     "Nationell biobank för barncancerprover. BrainChild-modulen hanterar specimen (FHIR), VCF och metadata.",
     "Barntumörbanken tillhandahåller provmaterial för forskning om barncancer."),

    ("SBCR (Svenska Barncancerregistret)", "Barncancerforskning",
     "Nationellt kvalitetsregister för barncancer. BrainChild SBCR-modulen hanterar registrerings-, behandlings- och uppföljningsdata.",
     "SBCR samlar in data om alla barn som diagnostiseras med cancer i Sverige."),

    ("Genomisk variant", "Barncancerforskning",
     "DNA-sekvensavvikelse identifierad genom sekvensering. Klassificeras enligt ACMG-riktlinjer. Lagras i VCF-format.",
     "Genomiska varianter kan vara patogena, benigna eller av okänd signifikans (VUS)."),
]

term_guids = {}
created = 0
for name, category, short_desc, long_desc in term_defs:
    body = {
        "name": name,
        "shortDescription": short_desc,
        "longDescription": long_desc,
        "anchor": {"glossaryGuid": GLOSSARY_GUID},
    }
    # Add category if available
    if category in cat_guids:
        body["categories"] = [{"categoryGuid": cat_guids[category]}]

    r = sess.post(f"{ATLAS}/glossary/term", headers=h, json=body, timeout=30)
    if r.status_code in (200, 201):
        term_guids[name] = r.json()["guid"]
        created += 1
        print(f"  ✅ {name}")
    else:
        print(f"  ❌ {name}: {r.status_code} — {r.text[:100]}")
    time.sleep(0.2)

print(f"\n  Created {created}/{len(term_defs)} terms")


# ══════════════════════════════════════════════════════════════════════
# STEP 9: WAIT FOR SCANS TO COMPLETE
# ══════════════════════════════════════════════════════════════════════
sep("9. WAITING FOR SCANS TO COMPLETE")
print("  Scans usually take 2-5 minutes. Checking every 30s...")

scan_jobs = [
    ("sql-hca-demo", "healthcare-scan"),
    ("Fabric", "Scan-HCA"),
    ("Fabric", "Scan-BrainChild"),
]

max_wait = 600  # 10 minutes
waited = 0
all_done = False

while waited < max_wait:
    all_done = True
    statuses = []
    for ds, scan in scan_jobs:
        r = sess.get(
            f"{SCAN_EP}/datasources/{ds}/scans/{scan}/runs?api-version={SCAN_API}",
            headers=h, timeout=30
        )
        if r.status_code == 200:
            runs = r.json().get("value", [])
            if runs:
                s = runs[0].get("status", "Unknown")
                statuses.append(f"{ds}/{scan}: {s}")
                if s not in ("Succeeded", "Failed", "Canceled"):
                    all_done = False
            else:
                statuses.append(f"{ds}/{scan}: no runs yet")
                all_done = False
        else:
            statuses.append(f"{ds}/{scan}: error {r.status_code}")

    elapsed = f"{waited//60}m{waited%60}s"
    print(f"  [{elapsed}] " + " | ".join(statuses))

    if all_done:
        break

    time.sleep(30)
    waited += 30

if not all_done:
    print("  ⚠️ Not all scans completed within timeout — continuing anyway")
    print("  Glossary term mappings will be attempted (may partially fail)")


# ══════════════════════════════════════════════════════════════════════
# STEP 10: MAP GLOSSARY TERMS TO SQL ENTITIES
# ══════════════════════════════════════════════════════════════════════
sep("10. MAPPING GLOSSARY TERMS TO SQL ENTITIES")

# Find SQL table entities
sql_tables = {}
for table_name in ["patients", "encounters", "diagnoses", "vitals_labs", "medications", "vw_ml_encounters"]:
    search_body = {
        "keywords": table_name,
        "filter": {"and": [
            {"entityType": "azure_sql_table"},
            {"or": [{"entityType": "azure_sql_table"}, {"entityType": "azure_sql_view"}]}
        ]},
        "limit": 5
    }
    r = sess.post(SEARCH, headers=h, json=search_body, timeout=30)
    if r.status_code == 200:
        for asset in r.json().get("value", []):
            qn = asset.get("qualifiedName", "")
            if table_name in qn.lower() and "hca" in qn.lower():
                sql_tables[table_name] = asset["id"]
                break

if sql_tables:
    print(f"  Found {len(sql_tables)} SQL tables")
    for t, g in sql_tables.items():
        print(f"    {t}: {g[:12]}...")
else:
    print("  ⚠️ No SQL tables found yet (scans may still be indexing)")
    print("  Run purview_glossary_map.py later to map terms after scan completes")

# Find SQL column entities
def find_columns(table_name, col_names):
    """Find column GUIDs for a given table."""
    found = {}
    for col in col_names:
        search_body = {
            "keywords": f"{table_name} {col}",
            "filter": {"entityType": "azure_sql_column"},
            "limit": 10
        }
        r = sess.post(SEARCH, headers=h, json=search_body, timeout=30)
        if r.status_code == 200:
            for asset in r.json().get("value", []):
                qn = asset.get("qualifiedName", "")
                if col in qn.lower() and table_name in qn.lower():
                    found[col] = asset["id"]
                    break
        time.sleep(0.1)
    return found

# Define mappings: term_name → list of (table_name, [column_names])
term_entity_map = {
    "ICD-10": [("diagnoses", ["icd10_code", "icd10_description"])],
    "ATC-klassificering": [("medications", ["atc_code", "drug_name"])],
    "Skyddad hälsoinformation (PHI)": [("patients", ["patient_id", "birth_date"]), ("encounters", ["encounter_id"])],
    "Svenskt personnummer": [("patients", ["postal_code"])],
    "FHIR R4": [("patients", []), ("encounters", []), ("diagnoses", []), ("vitals_labs", []), ("medications", [])],
    "FHIR Patient": [("patients", [])],
    "FHIR Encounter": [("encounters", [])],
    "FHIR Condition": [("diagnoses", [])],
    "FHIR MedicationRequest": [("medications", [])],
    "FHIR Observation": [("vitals_labs", [])],
    "OMOP CDM": [("patients", []), ("encounters", []), ("diagnoses", []), ("vitals_labs", []), ("medications", [])],
    "OMOP Person": [("patients", [])],
    "OMOP Visit Occurrence": [("encounters", [])],
    "OMOP Condition Occurrence": [("diagnoses", [])],
    "OMOP Drug Exposure": [("medications", [])],
    "OMOP Measurement": [("vitals_labs", [])],
    "Medallion-arkitektur": [("vw_ml_encounters", [])],
    "Gold-lager": [("vw_ml_encounters", [])],
    "Vårdtid (LOS)": [("encounters", ["los_days"]), ("vw_ml_encounters", [])],
    "Återinläggningsrisk": [("encounters", ["readmission_30d"]), ("vw_ml_encounters", [])],
    "Vitalparametrar": [("vitals_labs", ["systolic_bp", "diastolic_bp", "heart_rate", "oxygen_saturation", "temperature"])],
    "Labresultat": [("vitals_labs", ["hemoglobin_g", "wbc_count", "glucose_mmol", "creatinine_umol", "bmi", "weight_kg"])],
}

mapped_count = 0

for term_name, targets in term_entity_map.items():
    if term_name not in term_guids:
        print(f"  ⚠️ Term '{term_name}' not found in created terms")
        continue

    term_guid = term_guids[term_name]
    entity_guids = []

    for table_name, columns in targets:
        if table_name in sql_tables:
            entity_guids.append({"guid": sql_tables[table_name]})
        if columns:
            col_guids = find_columns(table_name, columns)
            for col, guid in col_guids.items():
                entity_guids.append({"guid": guid})

    if entity_guids:
        r = sess.post(
            f"{ATLAS}/glossary/terms/{term_guid}/assignedEntities",
            headers=h, json=entity_guids, timeout=30
        )
        if r.status_code in (200, 201, 204):
            mapped_count += 1
            entities_str = ", ".join([e["guid"][:8] for e in entity_guids[:3]])
            if len(entity_guids) > 3:
                entities_str += f"... +{len(entity_guids)-3} more"
            print(f"  ✅ {term_name} → {len(entity_guids)} entities ({entities_str})")
        else:
            print(f"  ❌ {term_name}: {r.status_code} — {r.text[:150]}")
        time.sleep(0.2)

print(f"\n  Mapped {mapped_count}/{len(term_entity_map)} terms to SQL entities")


# ══════════════════════════════════════════════════════════════════════
# STEP 11: ENRICH FABRIC LAKEHOUSES WITH DESCRIPTIONS
# ══════════════════════════════════════════════════════════════════════
sep("11. ENRICHING FABRIC LAKEHOUSE DESCRIPTIONS")

fabric_descriptions = {
    "bronze_lakehouse": "🔶 Bronze Layer | Rådatalager — direkt ingestion från Azure SQL. Relaterade termer: Medallion-arkitektur, Bronze-lager, FHIR R4, OMOP CDM",
    "silver_lakehouse": "⬜ Silver Layer | Renat & normaliserat datalager med feature engineering. Relaterade termer: Medallion-arkitektur, Silver-lager, Vitalparametrar, Labresultat",
    "gold_lakehouse": "🟡 Gold Layer | ML-ready features för LOS-prediktion och återinläggningsrisk. Relaterade termer: Medallion-arkitektur, Gold-lager, Vårdtid (LOS), Återinläggningsrisk",
    "gold_omop": "🟡 Gold OMOP | OMOP CDM-standardiserad data. Relaterade termer: OMOP CDM, OMOP Person, OMOP Visit Occurrence, OMOP Condition Occurrence",
    "lh_brainchild": "🧬 BrainChild Lakehouse | Barncancerforskningsdata. Relaterade termer: BTB, DICOM, GMS, SBCR, VCF, Genomisk variant, FHIR ImagingStudy, FHIR Specimen",
}

# Search for Fabric lakehouses
for lh_name, description in fabric_descriptions.items():
    search_body = {
        "keywords": lh_name,
        "filter": {"or": [
            {"entityType": "powerbi_dataset"},
            {"entityType": "powerbi_table"},
        ]},
        "limit": 10
    }
    r = sess.post(SEARCH, headers=h, json=search_body, timeout=30)
    if r.status_code == 200:
        for asset in r.json().get("value", []):
            if lh_name.lower() in asset.get("name", "").lower() or lh_name.lower() in asset.get("qualifiedName", "").lower():
                guid = asset["id"]
                # Update description
                r2 = sess.put(
                    f"{ATLAS}/entity/guid/{guid}?name=userDescription",
                    headers=h, data=json.dumps(description), timeout=30
                )
                if r2.status_code == 200:
                    print(f"  ✅ {lh_name}: description updated")
                else:
                    print(f"  ⚠️ {lh_name}: {r2.status_code}")
                break
        else:
            print(f"  ⏳ {lh_name}: not found yet (scan indexing)")
    time.sleep(0.2)


# ══════════════════════════════════════════════════════════════════════
# STEP 12: CREATE GOVERNANCE DOMAINS (Unified Catalog API)
# ══════════════════════════════════════════════════════════════════════
sep("12. CREATING GOVERNANCE DOMAINS")
refresh_token()

# Using the new Purview Unified Catalog API pattern from
# MarcoOesterlin/Microsoft-Purview-Unified-Catalog repo
# Endpoint: /datagovernance/catalog/ with api-version=2025-09-15-preview

domains_to_create = [
    {
        "name": "Klinisk Vård",
        "description": "Domän för klinisk patientdata — vårdbesök, diagnoser, medicinering, labresultat och ML-prediktioner. Källa: Azure SQL + Fabric Lakehouse (Healthcare-Analytics). Standarder: ICD-10, ATC, FHIR R4, OMOP CDM.",
        "type": "governance-domain",
    },
    {
        "name": "Barncancerforskning",
        "description": "Domän för barncancerforskningsdata — FHIR-resurser, DICOM-bilder, genomik (VCF), biobanksdata (BTB), GMS och SBCR-register. Källa: Fabric Lakehouse (BrainChild-Demo). Standarder: FHIR R4, DICOM, VCF, GMS.",
        "type": "governance-domain",
    },
]

domain_guids = {}

# Try multiple API endpoints in order of preference (new Unified Catalog API first)
for domain in domains_to_create:
    created = False

    # 1. Try new Unified Catalog API (multiple base URLs)
    for base_url in [DG_BASE, f"{TENANT_EP}/datagovernance/catalog", f"{ACCT}/datagovernance/catalog"]:
        if created:
            break
        r = sess.post(
            f"{base_url}/domains?api-version={DG_API}",
            headers=h, json=domain, timeout=30
        )
        if r.status_code in (200, 201):
            resp = r.json()
            domain_guids[domain["name"]] = resp.get("id") or resp.get("guid")
            print(f"  ✅ Domain '{domain['name']}' created (Unified Catalog API)")
            created = True
        elif r.status_code == 409:
            print(f"  ↳ Domain '{domain['name']}' already exists")
            # Try to fetch existing
            r2 = sess.get(f"{base_url}/domains?api-version={DG_API}", headers=h, timeout=30)
            if r2.status_code == 200:
                for d in r2.json().get("value", []):
                    if d.get("name", "").lower() == domain["name"].lower():
                        domain_guids[domain["name"]] = d.get("id") or d.get("guid")
                        break
            created = True

    # 2. Try datamap governance-domains API
    if not created:
        for api_ver in ["2023-10-01-preview", "2023-02-01-preview"]:
            r = sess.post(
                f"{ACCT}/datamap/api/governance-domains?api-version={api_ver}",
                headers=h, json=domain, timeout=30
            )
            if r.status_code in (200, 201):
                domain_guids[domain["name"]] = r.json().get("id") or r.json().get("guid")
                print(f"  ✅ Domain '{domain['name']}' created (datamap API, v={api_ver})")
                created = True
                break
            elif r.status_code == 409:
                print(f"  ↳ Domain '{domain['name']}' already exists")
                created = True
                break

    if not created:
        print(f"  ⚠️ Domain '{domain['name']}': Could not create via API")
        print(f"     Last response: {r.status_code} — {r.text[:200]}")

if domain_guids:
    print(f"\n  Created/found {len(domain_guids)} governance domains")
else:
    print(f"\n  ⚠️ Governance domain API not available — create manually in Purview portal:")
    print(f"     1. Go to https://purview.microsoft.com → Data Catalog → Governance domains")
    print(f"     2. Create 'Klinisk Vård' and 'Barncancerforskning'")
    print(f"     3. Add data products under each domain")


# ══════════════════════════════════════════════════════════════════════
# STEP 13: CREATE DATA PRODUCTS (Unified Catalog API)
# ══════════════════════════════════════════════════════════════════════
sep("13. CREATING DATA PRODUCTS")

data_products = [
    {
        "domain": "Klinisk Vård",
        "products": [
            {
                "name": "Patientdemografi",
                "description": "Demografisk patientdata — ålder, kön, postnummer. Källa: hca.patients (SQL). Standard: FHIR Patient, OMOP Person.",
                "businessUse": "Används för patientidentifiering, kohortanalys och demografisk stratifiering i kliniska studier.",
                "updateFrequency": "Dagligen via SQL-sync",
                "terms": ["FHIR Patient", "OMOP Person", "Skyddad hälsoinformation (PHI)", "Svenskt personnummer"],
            },
            {
                "name": "Vårdbesök & utfall",
                "description": "Vårdbesöksdata med LOS och återinläggningsrisk. Källa: hca.encounters + vw_ml_encounters (SQL/Gold). Standard: FHIR Encounter, OMOP Visit Occurrence.",
                "businessUse": "Prediktion av vårdtid och återinläggningsrisk. Underlag för kapacitetsplanering och kvalitetsuppföljning.",
                "updateFrequency": "Dagligen via SQL-sync + Gold-lager",
                "terms": ["FHIR Encounter", "OMOP Visit Occurrence", "Vårdtid (LOS)", "Återinläggningsrisk"],
            },
            {
                "name": "Diagnoser (ICD-10)",
                "description": "Diagnosinformation klassificerad med ICD-10. Källa: hca.diagnoses (SQL). Standard: FHIR Condition, OMOP Condition Occurrence.",
                "businessUse": "Epidemiologisk analys, DRG-klassificering och sjukdomsmönsteridentifiering.",
                "updateFrequency": "Dagligen via SQL-sync",
                "terms": ["ICD-10", "FHIR Condition", "OMOP Condition Occurrence"],
            },
            {
                "name": "Medicinering (ATC)",
                "description": "Läkemedelsdata klassificerad med ATC. Källa: hca.medications (SQL). Standard: FHIR MedicationRequest, OMOP Drug Exposure.",
                "businessUse": "Läkemedelsinteraktionsanalys, förskrivningsmönster och farmakovigilans.",
                "updateFrequency": "Dagligen via SQL-sync",
                "terms": ["ATC-klassificering", "FHIR MedicationRequest", "OMOP Drug Exposure"],
            },
            {
                "name": "Vitalparametrar & labb",
                "description": "Vitalparametrar och labresultat. Källa: hca.vitals_labs (SQL). Standard: FHIR Observation, OMOP Measurement.",
                "businessUse": "Tidig varning (Early Warning Score), labvärdesövervakning och ML-features.",
                "updateFrequency": "Realtid via SQL-sync",
                "terms": ["Vitalparametrar", "Labresultat", "FHIR Observation", "OMOP Measurement"],
            },
            {
                "name": "ML-prediktion (LOS & readmission)",
                "description": "ML-modell för vårdtid och återinläggningsprediktion. Feature store i Gold Lakehouse. Målvariabler: los_days, readmission_30d.",
                "businessUse": "Kliniskt beslutsstöd, resursoptimering och vårdkvalitetsuppföljning.",
                "updateFrequency": "Dagligen via Medallion-pipeline",
                "terms": ["Medallion-arkitektur", "Gold-lager", "Vårdtid (LOS)", "Återinläggningsrisk"],
            },
        ]
    },
    {
        "domain": "Barncancerforskning",
        "products": [
            {
                "name": "FHIR Patientresurser",
                "description": "BrainChild FHIR R4-resurser: Patient, Encounter, Condition, MedicationRequest, Observation, ImagingStudy, Specimen.",
                "businessUse": "Interoperabel patientdata för multicenter-forskning och nationella kvalitetsregister.",
                "updateFrequency": "Veckovis via FHIR-sync",
                "terms": ["FHIR R4", "FHIR Patient", "FHIR Encounter", "FHIR Specimen", "FHIR ImagingStudy"],
            },
            {
                "name": "Medicinsk bilddiagnostik (DICOM)",
                "description": "MR-hjärna och patologidata i DICOM-format. 42 MRI-serier + 79 patologiprover. Källa: BrainChild DICOM service.",
                "businessUse": "AI-baserad bildanalys, tumörklassificering och behandlingsplanering.",
                "updateFrequency": "Veckovis vid nya studier",
                "terms": ["DICOM", "FHIR ImagingStudy"],
            },
            {
                "name": "Genomik (GMS/VCF)",
                "description": "Genomiska varianter i VCF-format och GMS DiagnosticReports. Sekvensering: WGS/WES. Klassificering: ACMG.",
                "businessUse": "Precisionsmedicin, variantklassificering och farmakogenomik.",
                "updateFrequency": "Per sekvenseringskörning",
                "terms": ["Genomic Medicine Sweden (GMS)", "VCF (Variant Call Format)", "Genomisk variant"],
            },
            {
                "name": "Biobanksdata (BTB)",
                "description": "Barntumörbankens provdata — FHIR Specimen med VCF-koppling och metadata.",
                "businessUse": "Provspårbarhet, forskningssamarbeten och biobanksinventeringar.",
                "updateFrequency": "Veckovis vid nya prover",
                "terms": ["BTB (Barntumörbanken)", "FHIR Specimen", "VCF (Variant Call Format)"],
            },
            {
                "name": "Kvalitetsregister (SBCR)",
                "description": "Svenska Barncancerregistret — registrering, behandling och uppföljning av alla barncancerfall.",
                "businessUse": "Nationell kvalitetsuppföljning, överlevnadsstatistik och forskningskohorter.",
                "updateFrequency": "Månadsvis via SBCR-sync",
                "terms": ["SBCR (Svenska Barncancerregistret)"],
            },
        ]
    }
]

# Track created data products for term linking
created_data_products = {}  # {product_name: product_id}

for dp_config in data_products:
    domain_name = dp_config["domain"]
    domain_id = domain_guids.get(domain_name)

    for product in dp_config["products"]:
        created = False
        product_body = {
            "name": product["name"],
            "description": product["description"],
        }
        if "businessUse" in product:
            product_body["businessUse"] = product["businessUse"]
        if "updateFrequency" in product:
            product_body["updateFrequency"] = product["updateFrequency"]
        if domain_id:
            product_body["domainId"] = domain_id

        # 1. Try new Unified Catalog API (from repo pattern: /datagovernance/catalog/dataProducts)
        if domain_id:
            r = sess.post(
                f"{DG_BASE}/dataProducts?api-version={DG_API}",
                headers=h, json=product_body, timeout=30
            )
            if r.status_code in (200, 201):
                resp = r.json()
                dp_id = resp.get("id") or resp.get("guid")
                created_data_products[product["name"]] = dp_id
                print(f"  ✅ [{domain_name}] {product['name']} (Unified Catalog API)")
                created = True
            elif r.status_code == 409:
                print(f"  ↳ [{domain_name}] {product['name']} — already exists")
                created = True

        # 2. Fallback to old datamap API
        if not created and domain_id:
            for api_ver in ["2023-10-01-preview", "2023-02-01-preview"]:
                r = sess.post(
                    f"{ACCT}/datamap/api/data-products?api-version={api_ver}",
                    headers=h, json=product_body, timeout=30
                )
                if r.status_code in (200, 201):
                    resp = r.json()
                    dp_id = resp.get("id") or resp.get("guid")
                    created_data_products[product["name"]] = dp_id
                    print(f"  ✅ [{domain_name}] {product['name']} (datamap API)")
                    created = True
                    break
                elif r.status_code == 409:
                    print(f"  ↳ [{domain_name}] {product['name']} — already exists")
                    created = True
                    break

        if not created:
            if domain_id:
                print(f"  ⚠️ [{domain_name}] {product['name']}: {r.status_code}")
            else:
                print(f"  ⏭️  [{domain_name}] {product['name']} — skipped (no domain)")

        time.sleep(0.2)

if not domain_guids:
    print(f"\n  💡 Data products skipped — create governance domains first in portal")


# ══════════════════════════════════════════════════════════════════════
# STEP 14: LINK GLOSSARY TERMS TO DATA PRODUCTS
# ══════════════════════════════════════════════════════════════════════
sep("14. LINKING GLOSSARY TERMS TO DATA PRODUCTS")

# Pattern from Microsoft-Purview-Unified-Catalog: add_term_to_data_product.py
# POST /datagovernance/catalog/dataProducts/{id}/relationships
# with entityType=TERM and payload containing term linkage

linked_count = 0
for dp_config in data_products:
    for product in dp_config["products"]:
        dp_id = created_data_products.get(product["name"])
        if not dp_id:
            continue

        for term_name in product.get("terms", []):
            term_guid = term_guids.get(term_name)
            if not term_guid:
                continue

            # Try new Unified Catalog relationships API
            rel_body = {
                "description": f"Automatiskt länkad: {term_name} → {product['name']}",
                "relationshipType": "Related",
                "assetId": dp_id,
                "entityId": term_guid,
            }
            r = sess.post(
                f"{DG_BASE}/dataProducts/{dp_id}/relationships?api-version={DG_API}&entityType=TERM",
                headers=h, json=rel_body, timeout=30
            )
            if r.status_code in (200, 201):
                linked_count += 1
            elif r.status_code == 409:
                linked_count += 1  # Already linked
            else:
                # Fallback: try Atlas API term assignment
                assign_body = [{"guid": dp_id}]
                r2 = sess.post(
                    f"{ATLAS}/glossary/terms/{term_guid}/assignedEntities",
                    headers=h, json=assign_body, timeout=30
                )
                if r2.status_code in (200, 201, 204):
                    linked_count += 1

            time.sleep(0.1)

print(f"  Linked {linked_count} term-to-data-product relationships")


# ══════════════════════════════════════════════════════════════════════
# STEP 15: ADD PII CLASSIFICATIONS TO SENSITIVE COLUMNS
# ══════════════════════════════════════════════════════════════════════
sep("15. ADDING PII CLASSIFICATIONS TO SENSITIVE COLUMNS")

# Pattern from Microsoft-Purview-Unified-Catalog: Add_PII_Label.py
# POST /datamap/api/atlas/v2/entity/guid/{guid}/classifications
# with [{"typeName": "MICROSOFT.PERSONAL.*"}]

# Define which columns should get PII classifications
pii_columns = {
    ("patients", "patient_id"):     ["MICROSOFT.PERSONAL.NAME"],
    ("patients", "birth_date"):     ["MICROSOFT.PERSONAL.DATEOFBIRTH"],
    ("patients", "postal_code"):    ["MICROSOFT.PERSONAL.ZIPCODE"],
    ("patients", "gender"):         ["MICROSOFT.PERSONAL.GENDER"],
    ("patients", "smoking_status"): ["MICROSOFT.PERSONAL.HEALTH"],
    ("encounters", "patient_id"):   ["MICROSOFT.PERSONAL.NAME"],
    ("encounters", "admission_date"): ["MICROSOFT.PERSONAL.DATE"],
    ("encounters", "discharge_date"): ["MICROSOFT.PERSONAL.DATE"],
    ("vitals_labs", "systolic_bp"):   ["MICROSOFT.PERSONAL.HEALTH"],
    ("vitals_labs", "heart_rate"):    ["MICROSOFT.PERSONAL.HEALTH"],
    ("vitals_labs", "glucose_mmol"):  ["MICROSOFT.PERSONAL.HEALTH"],
    ("vitals_labs", "hemoglobin_g"):  ["MICROSOFT.PERSONAL.HEALTH"],
    ("medications", "drug_name"):     ["MICROSOFT.PERSONAL.HEALTH"],
    ("diagnoses", "icd10_code"):      ["MICROSOFT.PERSONAL.HEALTH"],
}

classified_count = 0
for (table_name, col_name), class_types in pii_columns.items():
    # Search for the column entity
    search_body = {
        "keywords": f"{table_name} {col_name}",
        "filter": {"entityType": "azure_sql_column"},
        "limit": 10
    }
    r = sess.post(SEARCH, headers=h, json=search_body, timeout=30)
    if r.status_code != 200:
        continue

    col_guid = None
    for asset in r.json().get("value", []):
        qn = asset.get("qualifiedName", "")
        if col_name in qn.lower() and table_name in qn.lower() and "hca" in qn.lower():
            col_guid = asset["id"]
            break

    if not col_guid:
        print(f"  ⏳ {table_name}.{col_name}: not found yet (scan indexing)")
        continue

    # Add classifications
    classifications = [{"typeName": ct} for ct in class_types]
    r = sess.post(
        f"{DATAMAP}/entity/guid/{col_guid}/classifications?api-version=2023-09-01",
        headers=h, json=classifications, timeout=30
    )
    if r.status_code == 204:
        classified_count += 1
        print(f"  ✅ {table_name}.{col_name} ← {', '.join(class_types)}")
    elif r.status_code == 409:
        classified_count += 1
        print(f"  ↳ {table_name}.{col_name}: already classified")
    else:
        print(f"  ⚠️ {table_name}.{col_name}: {r.status_code} — {r.text[:100]}")
    time.sleep(0.1)

print(f"\n  Classified {classified_count}/{len(pii_columns)} sensitive columns")


# ══════════════════════════════════════════════════════════════════════
# STEP 16: ADD LABELS/TAGS TO KEY ENTITIES
# ══════════════════════════════════════════════════════════════════════
sep("16. ADDING LABELS/TAGS TO KEY ENTITIES")

# Pattern from Microsoft-Purview-Unified-Catalog: add_tag.py
# PUT /datamap/api/atlas/v2/entity/guid/{guid}/labels
# with ["tag_name"]

# Define labels for SQL tables
table_labels = {
    "patients":        ["PHI", "FHIR-Patient", "OMOP-Person", "Medallion-Source"],
    "encounters":      ["PHI", "FHIR-Encounter", "OMOP-Visit", "Medallion-Source", "ML-Target"],
    "diagnoses":       ["ICD-10", "FHIR-Condition", "OMOP-Condition", "Medallion-Source"],
    "medications":     ["ATC", "FHIR-MedicationRequest", "OMOP-DrugExposure", "Medallion-Source"],
    "vitals_labs":     ["FHIR-Observation", "OMOP-Measurement", "Medallion-Source", "ML-Feature"],
    "vw_ml_encounters":["Gold-Layer", "ML-Ready", "Medallion-Gold"],
}

labeled_count = 0
for table_name, labels in table_labels.items():
    guid = sql_tables.get(table_name)
    if not guid:
        print(f"  ⏳ {table_name}: not found yet (scan indexing)")
        continue

    r = sess.put(
        f"{DATAMAP}/entity/guid/{guid}/labels",
        headers=h, json=labels, timeout=30
    )
    if r.status_code == 204:
        labeled_count += 1
        print(f"  ✅ {table_name} ← {', '.join(labels)}")
    else:
        print(f"  ⚠️ {table_name}: {r.status_code} — {r.text[:100]}")
    time.sleep(0.1)

print(f"\n  Labeled {labeled_count}/{len(table_labels)} entities")


# ══════════════════════════════════════════════════════════════════════
# STEP 17: PUBLISH DATA PRODUCTS (Draft → Published)
# ══════════════════════════════════════════════════════════════════════
sep("17. PUBLISHING DATA PRODUCTS")

# Pattern from Microsoft-Purview-Unified-Catalog: update_data_product_status
# PUT /datagovernance/catalog/dataProducts/{id}
# with {...product, "status": "Published"}

published_count = 0
for product_name, dp_id in created_data_products.items():
    # First GET the product to have full body for PUT
    r = sess.get(
        f"{DG_BASE}/dataProducts/{dp_id}?api-version={DG_API}",
        headers=h, timeout=30
    )
    if r.status_code == 200:
        dp_body = r.json()
        dp_body["status"] = "Published"
        r2 = sess.put(
            f"{DG_BASE}/dataProducts/{dp_id}?api-version={DG_API}",
            headers=h, json=dp_body, timeout=30
        )
        if r2.status_code in (200, 201):
            published_count += 1
            print(f"  ✅ {product_name}: Published")
        else:
            print(f"  ⚠️ {product_name}: publish {r2.status_code}")
    else:
        print(f"  ⏭️  {product_name}: could not fetch (status API may not be available)")
    time.sleep(0.2)

print(f"\n  Published {published_count}/{len(created_data_products)} data products")


# ══════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════════════
sep("FINAL SUMMARY")

# Count assets
r = sess.post(SEARCH, headers=h, json={"keywords": "*", "limit": 1}, timeout=30)
asset_count = r.json().get("@search.count", "?") if r.status_code == 200 else "?"

# Count glossary terms
r = sess.get(f"{ATLAS}/glossary/{GLOSSARY_GUID}/terms?limit=500", headers=h, timeout=30)
term_count = len(r.json()) if r.status_code == 200 else "?"

# Count collections
r = sess.get(f"{ACCT}/account/collections?api-version={COLL_API}", headers=h, timeout=30)
coll_count = len(r.json().get("value", [])) if r.status_code == 200 else "?"

# Count data sources
r = sess.get(f"{SCAN_EP}/datasources?api-version={SCAN_API}", headers=h, timeout=30)
ds_count = len(r.json().get("value", [])) if r.status_code == 200 else "?"

print(f"""
  ╔══════════════════════════════════════════════╗
  ║       PURVIEW REBUILD — RESULTAT             ║
  ╠══════════════════════════════════════════════╣
  ║  Collections:        {str(coll_count):>6}                 ║
  ║  Data sources:       {str(ds_count):>6}                 ║
  ║  Assets discovered:  {str(asset_count):>6}                 ║
  ║  Glossary terms:     {str(term_count):>6}                 ║
  ║  Terms mapped (SQL): {str(mapped_count):>6}                 ║
  ║  Gov. domains:       {str(len(domain_guids)):>6}                 ║
  ║  Data products:      {str(len(created_data_products)):>6}                 ║
  ║  Term→DP links:      {str(linked_count):>6}                 ║
  ║  PII classified:     {str(classified_count):>6}                 ║
  ║  Labels applied:     {str(labeled_count):>6}                 ║
  ║  Products published: {str(published_count):>6}                 ║
  ╚══════════════════════════════════════════════╝
""")

if asset_count == 0 or asset_count == "?":
    print("  ⏳ Assets will appear after scans complete (2-5 min)")
    print("  Run 'python scripts/purview_rebuild.py' again to retry mappings")

print("\n  ✅ REBUILD COMPLETE!")
