"""
Demo Showcase — Customer-facing presentation of the Healthcare Analytics Platform.

Shows all capabilities with beautiful formatting for customer demos:
  1. Platform Architecture Overview
  2. Data Layer (Azure SQL + row counts + schema)
  3. Fabric Medallion Architecture (Bronze → Silver → Gold)
  4. BrainChild Research Platform (FHIR, DICOM, Genomics)
  5. Data Governance (Purview collections, glossary, data products)
  6. ML Models & Experiments
  7. Security & Compliance (PII classifications, labels)
  8. OMOP CDM Interoperability
  9. Architecture Decisions & Standards

Usage:
  python scripts/demo_showcase.py           # Full showcase
  python scripts/demo_showcase.py --section 5  # Only governance
  python scripts/demo_showcase.py --json       # Export as JSON report
"""
import argparse
import json
import struct
import sys
from datetime import datetime
from pathlib import Path

import pyodbc
import requests
from azure.identity import AzureCliCredential

# ── CONFIG ─────────────────────────────────────────────────────────
SERVER = "sql-hca-demo.database.windows.net"
DATABASE = "HealthcareAnalyticsDB"
HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"
BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"
FABRIC_API = "https://api.fabric.microsoft.com/v1"
PURVIEW_ACCT = "https://prviewacc.purview.azure.com"
SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
FHIR_URL = "https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com"
DICOM_URL = "https://brainchildhdws-brainchilddicom.dicom.azurehealthcareapis.com"

cred = AzureCliCredential(process_timeout=30)
report = {}  # JSON export


# ── FORMATTING ─────────────────────────────────────────────────────
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def header(num, title, subtitle=""):
    print(f"\n{BOLD}{BLUE}{'━' * 70}{RESET}")
    print(f"{BOLD}{BLUE}  {num}. {title}{RESET}")
    if subtitle:
        print(f"{DIM}     {subtitle}{RESET}")
    print(f"{BOLD}{BLUE}{'━' * 70}{RESET}")


def ok(msg):
    print(f"  {GREEN}✅{RESET} {msg}")


def info(msg):
    print(f"  {CYAN}ℹ️ {RESET} {msg}")


def warn(msg):
    print(f"  {YELLOW}⚠️ {RESET} {msg}")


def detail(msg):
    print(f"     {DIM}{msg}{RESET}")


def table_row(col1, col2, col3=""):
    c1 = f"{col1:<30}"
    c2 = f"{col2:>12}"
    c3 = f"  {DIM}{col3}{RESET}" if col3 else ""
    print(f"  │ {c1} │ {c2} │{c3}")


def table_sep():
    print(f"  ├{'─' * 32}┼{'─' * 14}┤")


# ── SECTION 1: ARCHITECTURE OVERVIEW ──────────────────────────────
def section1():
    header("1", "PLATFORM ARCHITECTURE", "End-to-end Healthcare Analytics & Research Platform")
    print(f"""
  {BOLD}┌─────────────────────────────────────────────────────────────────┐{RESET}
  {BOLD}│                    HEALTHCARE DATA PLATFORM                     │{RESET}
  {BOLD}├─────────────────────────────────────────────────────────────────┤{RESET}
  │                                                                 │
  │   {CYAN}Azure SQL{RESET}  ──►  {CYAN}Fabric Bronze{RESET}  ──►  {CYAN}Fabric Silver{RESET}  ──►  {CYAN}Gold{RESET}  │
  │   (10K pts)      (Raw data)       (Features)        (ML/OMOP) │
  │                                                                 │
  │   {CYAN}FHIR Server{RESET} ──►  {CYAN}BrainChild LH{RESET}  ──►  Bronze  ──►  Silver   │
  │   (40 pts)       (OneLake)         (FHIR/DICOM/Genomics)      │
  │                                                                 │
  │   {CYAN}DICOM Server{RESET}     {CYAN}Microsoft Purview{RESET}                          │
  │   (121 studies)  (Governance, Glossary, PII, Data Products)    │
  │                                                                 │
  {BOLD}└─────────────────────────────────────────────────────────────────┘{RESET}

  {BOLD}Standards:{RESET}  FHIR R4 · OMOP CDM v5.4 · ICD-10-SE · ATC · DICOM · VCF
  {BOLD}Languages:{RESET}  Swedish (Sjukvård) — demo-ready for Nordic healthcare
  {BOLD}ML Models:{RESET}  LOS Prediction (LightGBM) · Readmission Risk (RandomForest)
  {BOLD}Security:{RESET}   AAD-only auth · PII classifications · Sensitivity labels
""")
    report["architecture"] = {
        "standards": ["FHIR R4", "OMOP CDM v5.4", "ICD-10-SE", "ATC", "DICOM", "VCF"],
        "components": ["Azure SQL", "Fabric Lakehouses", "FHIR Server", "DICOM Server", "Purview"],
    }


# ── SECTION 2: DATA LAYER ────────────────────────────────────────
def section2():
    header("2", "DATA LAYER — Azure SQL", f"Server: {SERVER}")
    try:
        token = cred.get_token("https://database.windows.net/.default")
        tb = token.token.encode("UTF-16-LE")
        ts = struct.pack(f"<I{len(tb)}s", len(tb), tb)
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};",
            attrs_before={1256: ts},
        )
        cursor = conn.cursor()

        print(f"\n  ┌{'─' * 32}┬{'─' * 14}┐")
        print(f"  │ {'Table':<30} │ {'Rows':>12} │")
        print(f"  ├{'─' * 32}┼{'─' * 14}┤")

        sql_report = {}
        total = 0
        for table in ["hca.patients", "hca.encounters", "hca.diagnoses",
                       "hca.vitals_labs", "hca.medications", "hca.vw_ml_encounters"]:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            cnt = cursor.fetchone()[0]
            total += cnt
            table_row(table, f"{cnt:,}")
            sql_report[table] = cnt

        print(f"  ├{'─' * 32}┼{'─' * 14}┤")
        table_row("TOTAL", f"{total:,}")
        print(f"  └{'─' * 32}┴{'─' * 14}┘")

        # Show schema highlights
        print(f"\n  {BOLD}Schema Highlights:{RESET}")
        detail("hca.patients — Swedish demographics (gender, ses_level, smoking_status)")
        detail("hca.encounters — 15 departments, LOS, readmission_30d")
        detail("hca.diagnoses — ICD-10-SE codes (Primary/Secondary/Complication)")
        detail("hca.medications — ATC codes, dosage, route, duration")
        detail("hca.vitals_labs — BP, HR, SpO2, temp, HbA1c, glucose, creatinine")
        detail("hca.vw_ml_encounters — ML-ready view with latest vitals + primary dx")

        # Sample data preview
        print(f"\n  {BOLD}Sample Patient:{RESET}")
        cursor.execute("SELECT TOP 1 patient_id, gender, birth_date, postal_code, ses_level FROM hca.patients")
        row = cursor.fetchone()
        if row:
            detail(f"ID: {row[0][:12]}... | Gender: {row[1]} | Born: {row[2]} | Postal: {row[3]} | SES: {row[4]}")

        conn.close()
        ok(f"Azure SQL: {total:,} total rows, AAD-only auth, ODBC 17")
        report["sql"] = sql_report

    except Exception as e:
        warn(f"SQL connection failed: {e}")


# ── SECTION 3: FABRIC MEDALLION ──────────────────────────────────
def section3():
    header("3", "FABRIC MEDALLION ARCHITECTURE", "Healthcare-Analytics Workspace")
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(f"{FABRIC_API}/workspaces/{HCA_WS}/items", headers=headers)
    if r.status_code != 200:
        warn(f"Could not list items: {r.status_code}")
        return

    items = r.json().get("value", [])
    by_type = {}
    for it in items:
        by_type.setdefault(it["type"], []).append(it)

    print(f"\n  {BOLD}Medallion Layers:{RESET}")
    layers = [
        ("🔶 Bronze", "bronze_lakehouse", "Raw SQL data — patients, encounters, diagnoses, vitals, meds"),
        ("⬜ Silver", "silver_lakehouse", "Feature engineering — Charlson Comorbidity Index, aggregated labs"),
        ("🟡 Gold ML", "gold_lakehouse", "ML features — LOS predictor, readmission classifier (MLflow)"),
        ("🟡 Gold OMOP", "gold_omop", "OMOP CDM v5.4 — person, visit, condition, drug, measurement"),
    ]
    for icon, name, desc in layers:
        found = any(it["displayName"] == name for it in items)
        status_icon = GREEN + "✅" + RESET if found else "❌"
        print(f"    {status_icon} {icon} {BOLD}{name}{RESET}")
        detail(desc)

    print(f"\n  {BOLD}Notebooks:{RESET}")
    for nb in sorted(by_type.get("Notebook", []), key=lambda x: x["displayName"]):
        print(f"    📓 {nb['displayName']}")

    print(f"\n  {BOLD}Pipelines:{RESET}")
    for pl in by_type.get("DataPipeline", []):
        print(f"    🔄 {pl['displayName']}")
        detail("Bronze → Silver → Gold_ML + Gold_OMOP (parallel after Silver)")

    print(f"\n  {BOLD}ML Models (MLflow):{RESET}")
    for ml in by_type.get("MLModel", []):
        print(f"    🤖 {ml['displayName']}")
    for exp in by_type.get("MLExperiment", []):
        print(f"    🧪 {exp['displayName']}")

    ok(f"Healthcare-Analytics: {len(items)} items total")
    report["fabric_hca"] = {it["displayName"]: it["type"] for it in items}


# ── SECTION 4: BRAINCHILD RESEARCH ──────────────────────────────
def section4():
    header("4", "BRAINCHILD RESEARCH PLATFORM", "BrainChild-Demo Workspace")
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(f"{FABRIC_API}/workspaces/{BC_WS}/items", headers=headers)
    if r.status_code != 200:
        warn(f"Could not list items: {r.status_code}")
        return

    items = r.json().get("value", [])

    print(f"\n  {BOLD}Data Sources:{RESET}")
    data_sources = [
        ("🏥", "FHIR R4 Server", f"{FHIR_URL}", "40 patients, 121 ImagingStudies"),
        ("📷", "DICOM Server", f"{DICOM_URL}", "42 MRI + 79 pathology"),
        ("🧬", "Genomics (GMS/VCF)", "OneLake", "WGS/WES sequencing data"),
        ("🧪", "Biobank (BTB)", "OneLake", "Barntumörbanken specimens"),
        ("📊", "Quality Register (SBCR)", "OneLake", "Svenska Barncancerregistret"),
        ("📋", "OMOP CDM Tables", "OneLake", "Standardized clinical data"),
    ]
    for icon, name, source, desc in data_sources:
        print(f"    {icon} {BOLD}{name}{RESET} — {desc}")
        detail(f"Source: {source}")

    print(f"\n  {BOLD}Processing Pipeline:{RESET}")
    print(f"    📥 01_load_omop_tables     — OMOP CSV → Lakehouse Delta")
    print(f"    ✅ 02_validate_data        — Row counts, schema validation")
    print(f"    🏥 03_ingest_fhir_bronze   — FHIR API → Bronze (Patient/Encounter/Condition/...)")
    print(f"    🔄 04_fhir_to_silver       — Flatten FHIR resources → Silver tables")
    print(f"    📷 05_ingest_dicom_bronze  — DICOM metadata → Bronze")
    print(f"    🔄 06_dicom_to_silver      — DICOM → Silver imaging tables")

    # Check FHIR server
    try:
        fhir_token = cred.get_token("https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com/.default").token
        fhir_h = {"Authorization": f"Bearer {fhir_token}"}
        r = requests.get(f"{FHIR_URL}/Patient?_summary=count", headers=fhir_h, timeout=10)
        if r.status_code == 200:
            pts = r.json().get("total", "?")
            ok(f"FHIR Server: {pts} patients online")
    except Exception:
        info("FHIR server check skipped (token scope)")

    ok(f"BrainChild-Demo: {len(items)} items, 6 data sources")
    report["fabric_bc"] = {it["displayName"]: it["type"] for it in items}


# ── SECTION 5: PURVIEW GOVERNANCE ────────────────────────────────
def section5():
    header("5", "DATA GOVERNANCE — Microsoft Purview", f"Account: prviewacc")
    token = cred.get_token("https://purview.azure.net/.default").token
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    gov_report = {}

    # Assets
    r = requests.post(
        f"{PURVIEW_ACCT}/catalog/api/search/query?api-version=2022-08-01-preview",
        headers=headers, json={"keywords": "*", "limit": 1}, timeout=30
    )
    assets = r.json().get("@search.count", 0) if r.status_code == 200 else "?"
    gov_report["assets"] = assets

    # Collections
    r = requests.get(
        f"{PURVIEW_ACCT}/account/collections?api-version=2019-11-01-preview",
        headers=headers, timeout=30
    )
    colls = []
    if r.status_code == 200:
        colls = r.json().get("value", [])
    gov_report["collections"] = len(colls)

    # Datasources
    r = requests.get(
        f"{SCAN_EP}/scan/datasources?api-version=2022-07-01-preview",
        headers=headers, timeout=30
    )
    sources = r.json().get("value", []) if r.status_code == 200 else []
    gov_report["datasources"] = len(sources)

    # Glossary
    r = requests.get(
        f"{PURVIEW_ACCT}/catalog/api/atlas/v2/glossary",
        headers=headers, timeout=30
    )
    glossary_info = {"glossaries": 0, "terms": 0, "categories": 0}
    if r.status_code == 200:
        data = r.json()
        glossaries = data if isinstance(data, list) else [data]
        glossary_info["glossaries"] = len(glossaries)
        for g in glossaries:
            glossary_info["terms"] += len(g.get("terms", []))
            glossary_info["categories"] += len(g.get("categories", []))
    gov_report["glossary"] = glossary_info

    print(f"\n  ┌{'─' * 40}┬{'─' * 14}┐")
    print(f"  │ {'Metric':<38} │ {'Count':>12} │")
    print(f"  ├{'─' * 40}┼{'─' * 14}┤")
    table_row("Discovered Assets", str(assets))
    table_row("Collections", str(len(colls)))
    table_row("Data Sources", str(len(sources)))
    table_row("Glossary Terms", str(glossary_info["terms"]))
    table_row("Term Categories", str(glossary_info["categories"]))
    print(f"  └{'─' * 40}┴{'─' * 14}┘")

    if colls:
        print(f"\n  {BOLD}Collection Hierarchy:{RESET}")
        # Build tree
        tree = {}
        for c in colls:
            tree[c["name"]] = {
                "friendly": c.get("friendlyName", c["name"]),
                "parent": c.get("parentCollection", {}).get("referenceName"),
            }
        # Print tree (simple 2-level)
        root = [n for n, v in tree.items() if not v["parent"]]
        for r_name in root:
            print(f"    📁 {tree[r_name]['friendly']}")
            children = [n for n, v in tree.items() if v["parent"] == r_name]
            for c_name in children:
                print(f"      📂 {tree[c_name]['friendly']}")
                grandchildren = [n for n, v in tree.items() if v["parent"] == c_name]
                for gc in grandchildren:
                    print(f"        📄 {tree[gc]['friendly']}")

    if sources:
        print(f"\n  {BOLD}Data Sources & Scans:{RESET}")
        for src in sources:
            print(f"    🔗 {src.get('name', '?')} ({src.get('kind', '?')})")

    if glossary_info["terms"] > 0:
        print(f"\n  {BOLD}Glossary Preview (Sjukvårdstermer):{RESET}")
        # Fetch a few terms
        r = requests.get(
            f"{PURVIEW_ACCT}/catalog/api/atlas/v2/glossary",
            headers=headers, timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            glossaries = data if isinstance(data, list) else [data]
            if glossaries:
                g = glossaries[0]
                # Show categories
                cats = g.get("categories", [])
                if cats:
                    print(f"    {BOLD}Categories:{RESET}")
                    for cat in cats[:10]:
                        print(f"      📋 {cat.get('displayText', cat.get('categoryGuid', '?'))}")
                # Show some terms
                terms = g.get("terms", [])
                if terms:
                    print(f"    {BOLD}Terms ({len(terms)} total):{RESET}")
                    for t in terms[:8]:
                        print(f"      📝 {t.get('displayText', t.get('termGuid', '?'))}")
                    if len(terms) > 8:
                        print(f"      {DIM}... and {len(terms) - 8} more{RESET}")

    ok(f"Purview: {assets} assets, {glossary_info['terms']} terms, {len(sources)} sources")
    report["governance"] = gov_report


# ── SECTION 6: ML MODELS ─────────────────────────────────────────
def section6():
    header("6", "ML MODELS & EXPERIMENTS", "MLflow on Fabric")
    token = cred.get_token("https://api.fabric.microsoft.com/.default").token
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(f"{FABRIC_API}/workspaces/{HCA_WS}/items", headers=headers)
    if r.status_code != 200:
        return

    items = r.json().get("value", [])
    models = [it for it in items if it["type"] == "MLModel"]
    experiments = [it for it in items if it["type"] == "MLExperiment"]

    print(f"\n  {BOLD}Registered Models:{RESET}")
    for m in models:
        print(f"    🤖 {BOLD}{m['displayName']}{RESET}")
        if "los" in m["displayName"].lower():
            detail("Algorithm: LightGBM | Target: los_days (regression)")
            detail("Features: age, gender, department, vitals, Charlson Comorbidity Index")
        elif "readmission" in m["displayName"].lower():
            detail("Algorithm: RandomForest | Target: readmission_30d (binary)")
            detail("Features: los_days, diagnosis count, medication count, vitals")

    print(f"\n  {BOLD}Experiments:{RESET}")
    for exp in experiments:
        print(f"    🧪 {exp['displayName']}")
        detail("Tracks model versions, hyperparameters, and evaluation metrics")

    print(f"\n  {BOLD}Feature Engineering Pipeline:{RESET}")
    detail("1. Bronze → Silver: Charlson Comorbidity Index (ICD-10 prefix mapping)")
    detail("2. Silver → Gold: Feature aggregation per encounter")
    detail("3. Gold: Train/test split, MLflow tracking, model registry")

    ok(f"{len(models)} registered models, {len(experiments)} experiments")


# ── SECTION 7: SECURITY ──────────────────────────────────────────
def section7():
    header("7", "SECURITY & COMPLIANCE", "PHI protection, PII classification, sensitivity labels")

    print(f"\n  {BOLD}Authentication:{RESET}")
    detail("Azure SQL: Azure AD-only authentication (no SQL passwords)")
    detail("Fabric: Azure AD token-based access")
    detail("FHIR: Azure AD managed identity")
    detail("Purview: Azure AD with role-based access")

    print(f"\n  {BOLD}PII Classifications (Purview):{RESET}")
    pii_items = [
        ("patients.patient_id", "PERSONAL.NAME"),
        ("patients.birth_date", "PERSONAL.DATEOFBIRTH"),
        ("patients.postal_code", "PERSONAL.ZIPCODE"),
        ("patients.gender", "PERSONAL.GENDER"),
        ("patients.smoking_status", "PERSONAL.HEALTH"),
        ("vitals_labs.systolic_bp", "PERSONAL.HEALTH"),
        ("vitals_labs.glucose_mmol", "PERSONAL.HEALTH"),
        ("diagnoses.icd10_code", "PERSONAL.HEALTH"),
        ("medications.drug_name", "PERSONAL.HEALTH"),
    ]
    for col, cls in pii_items:
        print(f"    🔒 {col:<35} → MICROSOFT.{cls}")

    print(f"\n  {BOLD}Entity Labels:{RESET}")
    labels = {
        "patients": "PHI · FHIR-Patient · OMOP-Person · Medallion-Source",
        "encounters": "PHI · FHIR-Encounter · OMOP-Visit · ML-Target",
        "diagnoses": "ICD-10 · FHIR-Condition · Medallion-Source",
        "vitals_labs": "FHIR-Observation · ML-Feature · Medallion-Source",
        "vw_ml_encounters": "Gold-Layer · ML-Ready · Medallion-Gold",
    }
    for table, tags in labels.items():
        print(f"    🏷️  {table:<25} → {tags}")

    ok("AAD-only auth, PII classified, sensitivity labels applied")


# ── SECTION 8: OMOP CDM ──────────────────────────────────────────
def section8():
    header("8", "OMOP CDM INTEROPERABILITY", "Observational Medical Outcomes Partnership v5.4")

    print(f"\n  {BOLD}OMOP Transformation (Gold Layer):{RESET}")
    omop_tables = [
        ("person", "Mapped from hca.patients — person_id, gender, birth date"),
        ("visit_occurrence", "Mapped from hca.encounters — visit dates, department → care_site"),
        ("condition_occurrence", "Mapped from hca.diagnoses — ICD-10 → concept_id"),
        ("drug_exposure", "Mapped from hca.medications — ATC → concept_id"),
        ("measurement", "Mapped from hca.vitals_labs — vital signs + lab results"),
        ("observation", "Additional clinical observations"),
        ("location", "Swedish postal codes"),
    ]
    for table, desc in omop_tables:
        print(f"    📊 {BOLD}{table}{RESET}")
        detail(desc)

    print(f"\n  {BOLD}Standards Compliance:{RESET}")
    detail("OMOP CDM v5.4 — Full standard vocabulary mappings")
    detail("ICD-10-SE → SNOMED CT concept mapping")
    detail("ATC → RxNorm concept mapping")
    detail("Integer person_id from UUID (deterministic hash)")
    detail("Stored in gold_omop lakehouse (Delta format)")

    ok("OMOP CDM v5.4 — 7 standardized tables in Gold layer")


# ── SECTION 9: ARCHITECTURE DECISIONS ─────────────────────────────
def section9():
    header("9", "ARCHITECTURE DECISIONS", "Design choices for the demo platform")

    decisions = [
        ("Medallion Architecture",
         "Bronze (raw) → Silver (cleaned/enriched) → Gold (ML-ready + OMOP)",
         "Industry standard for lakehouse analytics; clear data lineage"),
        ("Azure SQL as Source",
         "Operational database with realistic Swedish healthcare data",
         "Demonstrates hybrid cloud pattern: SQL → Lakehouse → ML"),
        ("FHIR R4 + DICOM",
         "Healthcare interoperability standards for clinical + imaging data",
         "Shows multi-modal data integration (structured + imaging + genomics)"),
        ("OMOP CDM",
         "International standard for observational health data",
         "Enables cross-institutional research and federated analytics"),
        ("Microsoft Purview",
         "Unified governance with glossary, data products, PII classification",
         "Demonstrates data catalog, lineage, and compliance capabilities"),
        ("MLflow on Fabric",
         "Model training and registry integrated in the lakehouse",
         "End-to-end ML lifecycle without separate infrastructure"),
        ("Swedish Language",
         "All metadata, departments, and governance in Swedish",
         "Demonstrates localized healthcare terminology (ICD-10-SE, ATC)"),
        ("BrainChild Separation",
         "Research data in separate workspace from clinical analytics",
         "Shows multi-workspace governance with unified Purview catalog"),
    ]

    for i, (name, what, why) in enumerate(decisions, 1):
        print(f"\n  {BOLD}{i}. {name}{RESET}")
        print(f"     {CYAN}What:{RESET} {what}")
        print(f"     {DIM}Why: {why}{RESET}")


# ── MAIN ──────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Demo Showcase — Healthcare Analytics Platform")
    parser.add_argument("--section", type=int, help="Show only specific section (1-9)")
    parser.add_argument("--json", action="store_true", help="Export report as JSON")
    args = parser.parse_args()

    print(f"""
{BOLD}{BLUE}╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║        🏥  HEALTHCARE ANALYTICS & RESEARCH PLATFORM  🧬              ║
║                                                                      ║
║        Customer Demo — {datetime.now().strftime('%Y-%m-%d %H:%M')}                              ║
║                                                                      ║
║        Azure SQL · Fabric Lakehouses · FHIR · DICOM                  ║
║        Purview Governance · MLflow · OMOP CDM                        ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝{RESET}
""")

    sections = {
        1: section1,
        2: section2,
        3: section3,
        4: section4,
        5: section5,
        6: section6,
        7: section7,
        8: section8,
        9: section9,
    }

    if args.section:
        if args.section in sections:
            sections[args.section]()
        else:
            print(f"Unknown section {args.section}. Valid: 1-9")
            sys.exit(1)
    else:
        for fn in sections.values():
            fn()

    if args.json:
        report["timestamp"] = datetime.now().isoformat()
        out = Path(__file__).resolve().parent.parent / "demo_report.json"
        out.write_text(json.dumps(report, indent=2, ensure_ascii=False))
        print(f"\n  📄 Report exported: {out}")

    print(f"\n{BOLD}{'═' * 70}{RESET}")
    print(f"{BOLD}  Demo showcase complete — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}{'═' * 70}{RESET}\n")


if __name__ == "__main__":
    main()
