"""
COMPREHENSIVE VALIDATION — Tests ALL systems end-to-end.

Checks:
 1. Azure SQL — tables, row counts, view
 2. Fabric Healthcare-Analytics workspace — lakehouses, notebooks, pipelines
 3. Fabric BrainChild-Demo workspace — lakehouses, notebooks, pipelines
 4. Purview — data sources, collections, glossary, Atlas entities
 5. Purview — classifications, labels, term mappings
 6. Purview — data products, OKRs, data quality
 7. FHIR server — patients, ImagingStudy
 8. DICOM server — studies
 9. OMOP CDM data in Fabric
10. End-to-end connectivity summary

Usage:
  python scripts/validate_all.py
  python scripts/validate_all.py --json   # export JSON report
"""
import json
import struct
import sys
import time
from datetime import datetime
from pathlib import Path

import pyodbc
import requests
from azure.identity import AzureCliCredential

# ── CONFIG ──
SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DB = "HealthcareAnalyticsDB"

FABRIC_HCA_WS = "afda4639-34ce-4ee9-a82f-ab7b5cfd7334"  # Healthcare-Analytics
FABRIC_BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"    # BrainChild-Demo

PURVIEW_ACCT = "https://prviewacc.purview.azure.com"
PURVIEW_SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ATLAS = f"{PURVIEW_ACCT}/catalog/api/atlas/v2"

FHIR_URL = "https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com"
DICOM_URL = "https://brainchildhdws-brainchilddicom.dicom.azurehealthcareapis.com"

# ── STYLING ──
G = "\033[92m"
R = "\033[91m"
Y = "\033[93m"
C = "\033[96m"
B = "\033[1m"
D = "\033[2m"
Z = "\033[0m"

# ── STATE ──
results = {}
total_pass = 0
total_fail = 0
total_warn = 0

cred = AzureCliCredential(process_timeout=30)


def get_token(scope):
    return cred.get_token(scope).token


def header(num, title):
    print(f"\n{'━' * 70}")
    print(f"  {B}{num}. {title}{Z}")
    print(f"{'━' * 70}")


def ok(msg, detail=""):
    global total_pass
    total_pass += 1
    d = f" {D}({detail}){Z}" if detail else ""
    print(f"  {G}✅{Z} {msg}{d}")


def fail(msg, detail=""):
    global total_fail
    total_fail += 1
    d = f" {D}({detail}){Z}" if detail else ""
    print(f"  {R}❌{Z} {msg}{d}")


def warn(msg, detail=""):
    global total_warn
    total_warn += 1
    d = f" {D}({detail}){Z}" if detail else ""
    print(f"  {Y}⚠️ {Z} {msg}{d}")


def info(msg):
    print(f"  {C}ℹ️ {Z} {msg}")


# ══════════════════════════════════════════════════════════════════
#  1. AZURE SQL
# ══════════════════════════════════════════════════════════════════
EXPECTED_TABLES = {
    "hca.patients": 10000,
    "hca.encounters": 17292,
    "hca.diagnoses": 30297,
    "hca.vitals_labs": 48131,
    "hca.medications": 60563,
}


def check_sql():
    header("1", "AZURE SQL DATABASE")
    section = {"status": "UNKNOWN", "checks": []}

    try:
        tok = get_token("https://database.windows.net/.default")
        tb = tok.encode("UTF-16-LE")
        ts = struct.pack(f"<I{len(tb)}s", len(tb), tb)
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DB};",
            attrs_before={1256: ts},
        )
        ok("SQL connection established", f"{SQL_SERVER}/{SQL_DB}")
    except Exception as e:
        fail(f"SQL connection failed: {e}")
        section["status"] = "FAIL"
        results["sql"] = section
        return

    cursor = conn.cursor()
    all_ok = True
    total_rows = 0

    for table, expected in EXPECTED_TABLES.items():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            total_rows += count
            if count >= expected:
                ok(f"{table}: {count:,} rows", f"expected ≥{expected:,}")
                section["checks"].append({"table": table, "rows": count, "expected": expected, "status": "PASS"})
            elif count > 0:
                warn(f"{table}: {count:,} rows", f"expected ≥{expected:,}")
                section["checks"].append({"table": table, "rows": count, "expected": expected, "status": "WARN"})
            else:
                fail(f"{table}: EMPTY")
                all_ok = False
                section["checks"].append({"table": table, "rows": 0, "expected": expected, "status": "FAIL"})
        except Exception as e:
            fail(f"{table}: {e}")
            all_ok = False

    # Check view
    try:
        cursor.execute("SELECT COUNT(*) FROM hca.vw_ml_encounters")
        vc = cursor.fetchone()[0]
        total_rows += vc
        if vc > 0:
            ok(f"hca.vw_ml_encounters (view): {vc:,} rows")
        else:
            fail("hca.vw_ml_encounters: EMPTY")
            all_ok = False
    except Exception as e:
        fail(f"View error: {e}")
        all_ok = False

    # Check schema
    try:
        cursor.execute("""
            SELECT TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'hca'
            ORDER BY TABLE_NAME
        """)
        tables = cursor.fetchall()
        info(f"Schema hca: {len(tables)} objects — {', '.join(t[0] for t in tables)}")
    except Exception:
        pass

    conn.close()
    info(f"Total rows across all tables: {total_rows:,}")
    section["status"] = "PASS" if all_ok else "FAIL"
    section["total_rows"] = total_rows
    results["sql"] = section


# ══════════════════════════════════════════════════════════════════
#  2. FABRIC HEALTHCARE-ANALYTICS WORKSPACE
# ══════════════════════════════════════════════════════════════════
def check_fabric_workspace(ws_id, ws_name, expected_items):
    section = {"status": "UNKNOWN", "items": []}

    try:
        tok = get_token("https://api.fabric.microsoft.com/.default")
        h = {"Authorization": f"Bearer {tok}"}
    except Exception as e:
        fail(f"Fabric token failed: {e}")
        section["status"] = "FAIL"
        return section

    r = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items",
        headers=h, timeout=30
    )
    if r.status_code != 200:
        fail(f"Cannot list items in {ws_name}: {r.status_code}")
        section["status"] = "FAIL"
        return section

    items = r.json().get("value", [])
    item_map = {}
    for item in items:
        t = item.get("type", "")
        n = item.get("displayName", "")
        item_map.setdefault(t, []).append(n)

    ok(f"Workspace accessible: {ws_name}", f"{len(items)} items total")

    found = 0
    missing = 0
    for item_type, names in expected_items.items():
        actual = item_map.get(item_type, [])
        for name in names:
            if name in actual:
                ok(f"{item_type}/{name}")
                found += 1
                section["items"].append({"type": item_type, "name": name, "status": "FOUND"})
            else:
                # Partial match
                matches = [a for a in actual if name.lower() in a.lower()]
                if matches:
                    ok(f"{item_type}/{name}", f"matched: {matches[0]}")
                    found += 1
                    section["items"].append({"type": item_type, "name": name, "status": "FOUND", "match": matches[0]})
                else:
                    warn(f"{item_type}/{name} — not found")
                    missing += 1
                    section["items"].append({"type": item_type, "name": name, "status": "MISSING"})

    # Show any extra items not in expected list
    expected_names = set()
    for names in expected_items.values():
        expected_names.update(n.lower() for n in names)

    extras = []
    for item in items:
        if item["displayName"].lower() not in expected_names:
            extras.append(f"{item['type']}/{item['displayName']}")
    if extras:
        info(f"Additional items: {', '.join(extras[:10])}")

    section["status"] = "PASS" if missing == 0 else "WARN"
    section["found"] = found
    section["missing"] = missing
    return section


def check_fabric_hca():
    header("2", "FABRIC — HEALTHCARE-ANALYTICS WORKSPACE")
    expected = {
        "Lakehouse": ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse", "gold_omop"],
        "Notebook": ["01_bronze_ingestion", "02_silver_features", "03_ml_training", "04_omop_transformation"],
        "DataPipeline": ["healthcare_etl_pipeline"],
    }
    results["fabric_hca"] = check_fabric_workspace(FABRIC_HCA_WS, "Healthcare-Analytics", expected)


def check_fabric_bc():
    header("3", "FABRIC — BRAINCHILD-DEMO WORKSPACE")
    expected = {
        "Lakehouse": ["lh_brainchild"],
        "Notebook": ["01_load_omop_tables", "03_ingest_fhir_bronze", "05_ingest_dicom_bronze"],
        "DataPipeline": ["pl_brainchild_ingestion", "pl_fhir_ingestion"],
    }
    results["fabric_bc"] = check_fabric_workspace(FABRIC_BC_WS, "BrainChild-Demo", expected)


# ══════════════════════════════════════════════════════════════════
#  4. PURVIEW — DATA SOURCES & COLLECTIONS
# ══════════════════════════════════════════════════════════════════
def check_purview_infra():
    header("4", "PURVIEW — DATA SOURCES & COLLECTIONS")
    section = {"status": "UNKNOWN"}

    try:
        tok = get_token("https://purview.azure.net/.default")
        h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}
    except Exception as e:
        fail(f"Purview token failed: {e}")
        section["status"] = "FAIL"
        results["purview_infra"] = section
        return h

    # Data sources
    r = requests.get(
        f"{PURVIEW_SCAN_EP}/scan/datasources?api-version=2023-09-01",
        headers=h, timeout=30
    )
    if r.status_code == 200:
        sources = r.json().get("value", [])
        names = [s["name"] for s in sources]
        ok(f"Data sources: {len(sources)}", ", ".join(names))
        for expected in ["sql-hca-demo", "Fabric"]:
            if expected in names:
                ok(f"  Source: {expected}")
            else:
                warn(f"  Source: {expected} — not found")
    else:
        fail(f"Data sources API: {r.status_code}")

    # Collections
    r = requests.get(
        f"{PURVIEW_ACCT}/account/collections?api-version=2019-11-01-preview",
        headers=h, timeout=30
    )
    if r.status_code == 200:
        cols = r.json().get("value", [])
        col_names = [c.get("friendlyName", c["name"]) for c in cols]
        ok(f"Collections: {len(cols)}", ", ".join(col_names))
    else:
        warn(f"Collections API: {r.status_code}")

    section["status"] = "PASS"
    results["purview_infra"] = section
    return h


# ══════════════════════════════════════════════════════════════════
#  5. PURVIEW — GLOSSARY & TERMS
# ══════════════════════════════════════════════════════════════════
def check_purview_glossary(h):
    header("5", "PURVIEW — GLOSSARY, TERMS & CATEGORIES")
    section = {"status": "UNKNOWN", "terms": 0, "categories": 0}

    r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    if r.status_code != 200:
        fail(f"Glossary API: {r.status_code}")
        section["status"] = "FAIL"
        results["purview_glossary"] = section
        return

    data = r.json()
    glossaries = data if isinstance(data, list) else [data]
    g = glossaries[0]
    g_guid = g["guid"]
    ok(f"Glossary: {g.get('name', 'default')}", f"guid={g_guid}")

    # Get full glossary with categories
    r2 = requests.get(f"{ATLAS}/glossary/{g_guid}", headers=h, timeout=30)
    if r2.status_code == 200:
        cats = r2.json().get("categories", [])
        cat_names = [c.get("displayText", "") for c in cats]
        section["categories"] = len(cats)
        if len(cats) >= 5:
            ok(f"Categories: {len(cats)}", ", ".join(cat_names))
        else:
            warn(f"Categories: {len(cats)} (expected ≥5)", ", ".join(cat_names))

    # Get terms
    r3 = requests.get(f"{ATLAS}/glossary/{g_guid}/terms?limit=500", headers=h, timeout=30)
    if r3.status_code == 200:
        terms = r3.json()
        section["terms"] = len(terms)

        # Count by category
        cat_counts = {}
        no_cat = 0
        for t in terms:
            tcats = t.get("categories", [])
            if tcats:
                for tc in tcats:
                    cn = tc.get("displayText", "?")
                    cat_counts[cn] = cat_counts.get(cn, 0) + 1
            else:
                no_cat += 1

        if len(terms) >= 32:
            ok(f"Terms: {len(terms)} total")
        else:
            warn(f"Terms: {len(terms)} (expected ≥32)")

        for cat, count in sorted(cat_counts.items()):
            info(f"  {cat}: {count} terms")
        if no_cat > 0:
            info(f"  (uncategorized): {no_cat} terms")

        # Check for data product terms
        dp_terms = [t for t in terms if t["name"].startswith("DP")]
        okr_terms = [t for t in terms if "OKR" in t["name"]]
        dq_terms = [t for t in terms if t["name"].startswith("DQ-")]

        if dp_terms:
            ok(f"Data Product terms: {len(dp_terms)}")
        else:
            warn("No Data Product terms found (run purview_data_products.py)")

        if okr_terms:
            ok(f"OKR terms: {len(okr_terms)}")
        else:
            warn("No OKR terms found (run purview_data_products.py)")

        if dq_terms:
            ok(f"Data Quality terms: {len(dq_terms)}")
        else:
            warn("No Data Quality terms found (run purview_data_products.py)")

    section["status"] = "PASS" if section["terms"] >= 32 else "WARN"
    results["purview_glossary"] = section


# ══════════════════════════════════════════════════════════════════
#  6. PURVIEW — ATLAS ENTITIES (SQL ASSETS)
# ══════════════════════════════════════════════════════════════════
def check_purview_entities(h):
    header("6", "PURVIEW — ATLAS ENTITIES & CLASSIFICATIONS")
    section = {"status": "UNKNOWN", "entities": 0, "classifications": 0, "labels": 0}

    # Search for all SQL entities
    search_payload = {
        "keywords": "sql-hca-demo",
        "limit": 100,
    }
    r = requests.post(
        f"{PURVIEW_ACCT}/catalog/api/search/query?api-version=2022-08-01-preview",
        headers=h, json=search_payload, timeout=30
    )
    if r.status_code != 200:
        fail(f"Search API: {r.status_code}")
        section["status"] = "FAIL"
        results["purview_entities"] = section
        return

    data = r.json()
    count = data.get("@search.count", 0)
    entities = data.get("value", [])

    if count >= 10:
        ok(f"Total entities found: {count}")
    elif count > 0:
        warn(f"Entities found: {count} (expected ≥10)")
    else:
        fail("No entities found!")
        section["status"] = "FAIL"
        results["purview_entities"] = section
        return

    # Count by type
    type_counts = {}
    classified = 0
    labeled = 0
    with_terms = 0

    for e in entities:
        etype = e.get("entityType", "unknown")
        type_counts[etype] = type_counts.get(etype, 0) + 1

        if e.get("classification"):
            classified += 1
        if e.get("label"):
            labeled += 1
        if e.get("term"):
            with_terms += 1

    section["entities"] = count

    for etype, cnt in sorted(type_counts.items()):
        info(f"  {etype}: {cnt}")

    # Check specific tables
    expected_tables = ["patients", "encounters", "diagnoses", "vitals_labs", "medications", "vw_ml_encounters"]
    found_tables = set()
    for e in entities:
        name = e.get("name", "")
        if name in expected_tables:
            found_tables.add(name)

    for t in expected_tables:
        if t in found_tables:
            ok(f"Table entity: {t}")
        else:
            warn(f"Table entity: {t} — not in search results")

    # Classifications
    info(f"Entities with classifications: {classified}")
    info(f"Entities with labels: {labeled}")
    info(f"Entities with glossary terms: {with_terms}")

    # Check specific entity for classifications
    for e in entities:
        if e.get("name") == "patients" and e.get("entityType") == "azure_sql_table":
            guid = e.get("id")
            if guid:
                r2 = requests.get(f"{ATLAS}/entity/guid/{guid}", headers=h, timeout=30)
                if r2.status_code == 200:
                    ent = r2.json().get("entity", {})
                    cls_list = ent.get("classifications", [])
                    labels = ent.get("labels", [])
                    meanings = ent.get("relationshipAttributes", {}).get("meanings", [])
                    if cls_list:
                        ok(f"patients classifications: {len(cls_list)}", ", ".join(c.get("typeName", "") for c in cls_list))
                        section["classifications"] = len(cls_list)
                    if labels:
                        ok(f"patients labels: {labels}")
                        section["labels"] = len(labels)
                    if meanings:
                        ok(f"patients glossary terms: {len(meanings)}")
            break

    # Check custom data product entities
    dp_search = {"keywords": "healthcare_data_product", "limit": 10}
    r3 = requests.post(
        f"{PURVIEW_ACCT}/catalog/api/search/query?api-version=2022-08-01-preview",
        headers=h, json=dp_search, timeout=30
    )
    if r3.status_code == 200:
        dp_count = r3.json().get("@search.count", 0)
        if dp_count > 0:
            ok(f"Data Product entities: {dp_count}")
        else:
            info("No Data Product entities yet")

    section["status"] = "PASS" if count >= 10 else "WARN"
    results["purview_entities"] = section


# ══════════════════════════════════════════════════════════════════
#  7. FHIR SERVER
# ══════════════════════════════════════════════════════════════════
def check_fhir():
    header("7", "FHIR SERVER — BRAINCHILD")
    section = {"status": "UNKNOWN"}

    try:
        tok = get_token(FHIR_URL)
        h = {"Authorization": f"Bearer {tok}"}
    except Exception as e:
        fail(f"FHIR token failed: {e}")
        section["status"] = "FAIL"
        results["fhir"] = section
        return

    # Patients
    r = requests.get(f"{FHIR_URL}/Patient?_summary=count", headers=h, timeout=30)
    if r.status_code == 200:
        total = r.json().get("total", 0)
        if total >= 40:
            ok(f"FHIR Patients: {total}")
        elif total > 0:
            warn(f"FHIR Patients: {total} (expected ≥40)")
        else:
            fail("FHIR Patients: 0")
    else:
        fail(f"FHIR Patient query: {r.status_code}")

    # ImagingStudy
    r2 = requests.get(f"{FHIR_URL}/ImagingStudy?_summary=count", headers=h, timeout=30)
    if r2.status_code == 200:
        total = r2.json().get("total", 0)
        if total >= 100:
            ok(f"FHIR ImagingStudy: {total}", "MRI + Pathology")
        elif total > 0:
            warn(f"FHIR ImagingStudy: {total} (expected ≥121)")
        else:
            fail("FHIR ImagingStudy: 0")
    else:
        warn(f"FHIR ImagingStudy query: {r2.status_code}")

    # Specimen (BTB)
    r3 = requests.get(f"{FHIR_URL}/Specimen?_summary=count", headers=h, timeout=30)
    if r3.status_code == 200:
        total = r3.json().get("total", 0)
        if total > 0:
            ok(f"FHIR Specimen: {total}")
        else:
            info("FHIR Specimen: 0 (may not be uploaded)")
    else:
        info(f"FHIR Specimen: {r3.status_code}")

    # Condition
    r4 = requests.get(f"{FHIR_URL}/Condition?_summary=count", headers=h, timeout=30)
    if r4.status_code == 200:
        total = r4.json().get("total", 0)
        if total > 0:
            ok(f"FHIR Condition: {total}")
    else:
        info(f"FHIR Condition: {r4.status_code}")

    section["status"] = "PASS"
    results["fhir"] = section


# ══════════════════════════════════════════════════════════════════
#  8. DICOM SERVER
# ══════════════════════════════════════════════════════════════════
def check_dicom():
    header("8", "DICOM SERVER — BRAINCHILD")
    section = {"status": "UNKNOWN"}

    try:
        tok = get_token("https://dicom.healthcareapis.azure.com")
        h = {"Authorization": f"Bearer {tok}", "Accept": "application/dicom+json"}
    except Exception as e:
        fail(f"DICOM token failed: {e}")
        section["status"] = "FAIL"
        results["dicom"] = section
        return

    # Get studies
    r = requests.get(f"{DICOM_URL}/v1/studies?limit=200&includefield=all", headers=h, timeout=30)
    if r.status_code == 200:
        studies = r.json()
        if isinstance(studies, list) and len(studies) > 0:
            # Count by modality
            modalities = {}
            for s in studies:
                mod = "Unknown"
                for tag in s.get("00080061", {}).get("Value", []):
                    mod = tag
                modalities[mod] = modalities.get(mod, 0) + 1

            ok(f"DICOM Studies: {len(studies)}")
            for mod, cnt in sorted(modalities.items()):
                info(f"  Modality {mod}: {cnt} studies")
        else:
            warn("DICOM: no studies returned")
    else:
        fail(f"DICOM query: {r.status_code}")

    section["status"] = "PASS"
    results["dicom"] = section


# ══════════════════════════════════════════════════════════════════
#  9. DATA QUALITY (quick SQL check)
# ══════════════════════════════════════════════════════════════════
def check_data_quality():
    header("9", "DATA QUALITY — QUICK CHECKS")
    section = {"status": "UNKNOWN", "checks": []}

    try:
        tok = get_token("https://database.windows.net/.default")
        tb = tok.encode("UTF-16-LE")
        ts = struct.pack(f"<I{len(tb)}s", len(tb), tb)
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DB};",
            attrs_before={1256: ts},
        )
    except Exception as e:
        fail(f"SQL connection for DQ: {e}")
        section["status"] = "FAIL"
        results["data_quality"] = section
        return

    cursor = conn.cursor()
    checks = [
        ("Orphan encounters", """
            SELECT COUNT(*) FROM hca.encounters e
            LEFT JOIN hca.patients p ON e.patient_id = p.patient_id
            WHERE p.patient_id IS NULL
        """, 0, "eq"),
        ("Orphan diagnoses", """
            SELECT COUNT(*) FROM hca.diagnoses d
            LEFT JOIN hca.encounters e ON d.encounter_id = e.encounter_id
            WHERE e.encounter_id IS NULL
        """, 0, "eq"),
        ("Duplicate patient_ids", """
            SELECT COUNT(*) - COUNT(DISTINCT patient_id) FROM hca.patients
        """, 0, "eq"),
        ("Duplicate encounter_ids", """
            SELECT COUNT(*) - COUNT(DISTINCT encounter_id) FROM hca.encounters
        """, 0, "eq"),
        ("Null patient_id in patients", """
            SELECT COUNT(*) FROM hca.patients WHERE patient_id IS NULL
        """, 0, "eq"),
        ("Null admission_date", """
            SELECT COUNT(*) FROM hca.encounters WHERE admission_date IS NULL
        """, 0, "eq"),
        ("ICD-10 format valid", """
            SELECT COUNT(*) FROM hca.diagnoses
            WHERE icd10_code NOT LIKE '[A-Z][0-9][0-9]%'
        """, 0, "eq"),
        ("Negative LOS", """
            SELECT COUNT(*) FROM hca.encounters WHERE los_days < 0
        """, 0, "eq"),
    ]

    passed = 0
    for name, sql, expected, op in checks:
        try:
            cursor.execute(sql)
            val = cursor.fetchone()[0]
            if op == "eq" and val == expected:
                ok(f"{name}: {val}")
                passed += 1
                section["checks"].append({"name": name, "value": val, "status": "PASS"})
            elif op == "eq":
                fail(f"{name}: {val} (expected {expected})")
                section["checks"].append({"name": name, "value": val, "status": "FAIL"})
            else:
                ok(f"{name}: {val}")
                passed += 1
        except Exception as e:
            warn(f"{name}: {e}")

    conn.close()
    info(f"Quick DQ: {passed}/{len(checks)} passed")
    section["status"] = "PASS" if passed == len(checks) else "WARN"
    results["data_quality"] = section


# ══════════════════════════════════════════════════════════════════
#  10. PURVIEW DATA PRODUCTS API
# ══════════════════════════════════════════════════════════════════
def check_purview_data_products(h):
    header("10", "PURVIEW — DATA PRODUCTS & UNIFIED CATALOG")
    section = {"status": "UNKNOWN"}

    # Data products API
    r = requests.get(
        f"{PURVIEW_ACCT}/datagovernance/catalog/dataproducts?api-version=2025-09-15-preview",
        headers=h, timeout=15
    )
    if r.status_code == 200:
        dps = r.json().get("value", [])
        if dps:
            ok(f"Data Products: {len(dps)}")
            for dp in dps:
                info(f"  {dp.get('name', '?')}")
        else:
            info("Data Products API works but empty (products stored as glossary terms)")
    else:
        warn(f"Data Products API: {r.status_code}")

    # Custom type check
    r2 = requests.get(f"{ATLAS}/types/typedef/name/healthcare_data_product", headers=h, timeout=15)
    if r2.status_code == 200:
        ok("Custom type 'healthcare_data_product' registered")
    else:
        info("Custom type 'healthcare_data_product' not yet created")

    section["status"] = "PASS"
    results["purview_data_products"] = section


# ══════════════════════════════════════════════════════════════════
#  SUMMARY
# ══════════════════════════════════════════════════════════════════
def print_summary(export_json=False):
    print(f"\n{'━' * 70}")
    print(f"  {B}VALIDATION SUMMARY — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{Z}")
    print(f"{'━' * 70}")

    component_status = {
        "sql": ("Azure SQL Database", results.get("sql", {}).get("status", "SKIP")),
        "fabric_hca": ("Fabric Healthcare-Analytics", results.get("fabric_hca", {}).get("status", "SKIP")),
        "fabric_bc": ("Fabric BrainChild-Demo", results.get("fabric_bc", {}).get("status", "SKIP")),
        "purview_infra": ("Purview Data Sources", results.get("purview_infra", {}).get("status", "SKIP")),
        "purview_glossary": ("Purview Glossary & Terms", results.get("purview_glossary", {}).get("status", "SKIP")),
        "purview_entities": ("Purview Atlas Entities", results.get("purview_entities", {}).get("status", "SKIP")),
        "purview_data_products": ("Purview Data Products", results.get("purview_data_products", {}).get("status", "SKIP")),
        "fhir": ("FHIR Server (BrainChild)", results.get("fhir", {}).get("status", "SKIP")),
        "dicom": ("DICOM Server (BrainChild)", results.get("dicom", {}).get("status", "SKIP")),
        "data_quality": ("Data Quality Checks", results.get("data_quality", {}).get("status", "SKIP")),
    }

    print(f"\n  {'Component':<35} {'Status':>10}")
    print(f"  {'─' * 35} {'─' * 10}")

    for key, (name, status) in component_status.items():
        if status == "PASS":
            icon = f"{G}✅ PASS{Z}"
        elif status == "WARN":
            icon = f"{Y}⚠️  WARN{Z}"
        elif status == "FAIL":
            icon = f"{R}❌ FAIL{Z}"
        else:
            icon = f"{D}⏭️  SKIP{Z}"
        print(f"  {name:<35} {icon}")

    print(f"\n  {B}Checks:{Z} {G}{total_pass} passed{Z}, {Y}{total_warn} warnings{Z}, {R}{total_fail} failed{Z}")

    overall = "PASS" if total_fail == 0 else "FAIL"
    if overall == "PASS":
        print(f"\n  {G}{B}🎉 ALL VALIDATIONS PASSED — Platform ready for demo!{Z}")
    else:
        print(f"\n  {Y}{B}⚠️  Some issues found — review above for details{Z}")

    # Portal links
    print(f"\n  {B}Portal Links:{Z}")
    print(f"  {C}Purview:{Z}  https://purview.microsoft.com")
    print(f"  {C}Fabric:{Z}   https://app.fabric.microsoft.com/groups/{FABRIC_HCA_WS}")
    print(f"  {C}Fabric:{Z}   https://app.fabric.microsoft.com/groups/{FABRIC_BC_WS}")

    if export_json:
        report = {
            "timestamp": datetime.now().isoformat(),
            "overall": overall,
            "checks": {"passed": total_pass, "warnings": total_warn, "failed": total_fail},
            "components": {k: v for k, v in results.items()},
        }
        out = Path(__file__).resolve().parent.parent / "validation_report.json"
        out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str))
        info(f"Report: {out}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Comprehensive Validation")
    parser.add_argument("--json", action="store_true", help="Export JSON report")
    args = parser.parse_args()

    print(f"""
{B}{C}╔══════════════════════════════════════════════════════════════════╗
║  COMPREHENSIVE PLATFORM VALIDATION                               ║
║  Healthcare Analytics + BrainChild + Purview + FHIR + DICOM      ║
║  {datetime.now().strftime('%Y-%m-%d %H:%M')}                                               ║
╚══════════════════════════════════════════════════════════════════╝{Z}
""")

    # 1. Azure SQL
    check_sql()

    # 2-3. Fabric workspaces
    check_fabric_hca()
    check_fabric_bc()

    # 4. Purview infrastructure (returns headers for reuse)
    h = check_purview_infra()

    # 5. Purview glossary
    if isinstance(h, dict):
        check_purview_glossary(h)

        # 6. Purview entities
        check_purview_entities(h)

        # 10. Purview data products
        check_purview_data_products(h)

    # 7. FHIR
    check_fhir()

    # 8. DICOM
    check_dicom()

    # 9. Data Quality
    check_data_quality()

    # Summary
    print_summary(export_json=args.json)


if __name__ == "__main__":
    main()
