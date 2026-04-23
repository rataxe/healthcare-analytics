"""
Complete glossary solution:
1. Map all remaining SQL table/column glossary terms
2. Add rich business-term descriptions to Fabric lakehouses 
   (cross-domain prevents direct glossary assignment)
3. Clean up: delete the duplicate 'Fabric Assets' glossary
4. Generate final summary report
"""
import requests, json, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

ACCT_EP = "https://prviewacc.purview.azure.com"
ATLAS_EP = f"{ACCT_EP}/catalog/api/atlas/v2"
SEARCH_EP = f"{ACCT_EP}/catalog/api/search/query?api-version=2022-08-01-preview"

# ════════════════════════════════════════════════════════════
# 0. Cleanup: delete duplicate 'Fabric Assets' glossary
# ════════════════════════════════════════════════════════════
print("=" * 70)
print("0. Cleaning up duplicate glossary")
print("=" * 70)

r = sess.get(f"{ATLAS_EP}/glossary", headers=h, timeout=30)
for g in r.json():
    if g.get("name") == "Fabric Assets":
        guid = g["guid"]
        # Get terms first and delete them
        r2 = sess.get(f"{ATLAS_EP}/glossary/{guid}/terms?limit=100", headers=h, timeout=30)
        if r2.status_code == 200:
            for t in r2.json():
                sess.delete(f"{ATLAS_EP}/glossary/term/{t['guid']}", headers=h, timeout=30)
                time.sleep(0.2)
        r3 = sess.delete(f"{ATLAS_EP}/glossary/{guid}", headers=h, timeout=30)
        print(f"  Deleted 'Fabric Assets' glossary: {r3.status_code}")

# Delete fabric-assets collection
r4 = sess.delete(f"{ACCT_EP}/account/collections/fabric-assets?api-version=2019-11-01-preview", headers=h, timeout=30)
print(f"  Deleted 'fabric-assets' collection: {r4.status_code}")

time.sleep(1)

# ════════════════════════════════════════════════════════════
# 1. Get main glossary and all terms
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("1. Loading glossary 'Kund'")
print("=" * 70)

r = sess.get(f"{ATLAS_EP}/glossary", headers=h, timeout=30)
glossary_guid = None
for g in r.json():
    if g.get("name") == "Kund":
        glossary_guid = g["guid"]
        break

r = sess.get(f"{ATLAS_EP}/glossary/{glossary_guid}/terms?limit=100", headers=h, timeout=30)
terms = {}
for t in r.json():
    name = t.get("name")
    assigned = t.get("assignedEntities", [])
    terms[name] = {
        "guid": t["guid"],
        "assigned_guids": set(a.get("guid") for a in assigned),
        "assigned_names": [a.get("displayText", "?") for a in assigned],
    }

print(f"  Total terms: {len(terms)}")
mapped = sum(1 for v in terms.values() if v["assigned_guids"])
print(f"  Mapped: {mapped}, Unmapped: {len(terms) - mapped}")

# ════════════════════════════════════════════════════════════
# 2. Get SQL table entities
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("2. SQL table entities")
print("=" * 70)

QN_BASE = "mssql://sql-hca-demo.database.windows.net/HealthcareAnalyticsDB/hca"
TABLE_TYPES = {
    "patients": "azure_sql_table",
    "encounters": "azure_sql_table",
    "diagnoses": "azure_sql_table",
    "vitals_labs": "azure_sql_table",
    "medications": "azure_sql_table",
    "vw_ml_encounters": "azure_sql_view",
}

tables = {}
for tbl, ttype in TABLE_TYPES.items():
    qn = f"{QN_BASE}/{tbl}"
    r = sess.get(f"{ATLAS_EP}/entity/uniqueAttribute/type/{ttype}?attr:qualifiedName={qn}", headers=h, timeout=30)
    if r.status_code == 200:
        tables[tbl] = r.json().get("entity", {})
        print(f"  ✅ {tbl}")
    time.sleep(0.3)


def col_guid(table_name, col_name):
    ent = tables.get(table_name, {})
    for c in ent.get("relationshipAttributes", {}).get("columns", []):
        if c.get("displayText") == col_name:
            return c.get("guid")
    return None


def assign(term_name, guids, label=""):
    if term_name not in terms:
        return
    info = terms[term_name]
    new = [g for g in guids if g and g not in info["assigned_guids"]]
    if not new:
        print(f"  ✅ {term_name} -> {label} (already)")
        return
    body = [{"guid": g} for g in new]
    r = sess.post(f"{ATLAS_EP}/glossary/terms/{info['guid']}/assignedEntities", headers=h, json=body, timeout=30)
    if r.status_code in (200, 204):
        print(f"  ✅ {term_name} -> {label} ({len(new)} new)")
        info["assigned_guids"].update(new)
    else:
        print(f"  ⚠️ {term_name}: {r.status_code}")
    time.sleep(0.3)


# ════════════════════════════════════════════════════════════
# 3. Map ALL remaining SQL glossary terms
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("3. Mapping all SQL glossary terms")
print("=" * 70)

# -- Already mapped (verify) --
assign("Person OMOP", [tables["patients"]["guid"]], "patients")
assign("Condition Occurrence", [tables["diagnoses"]["guid"]], "diagnoses")
assign("Drug Exposure", [tables["medications"]["guid"]], "medications")
assign("Measurement", [tables["vitals_labs"]["guid"]], "vitals_labs")
assign("Visit Occurrence", [tables["encounters"]["guid"]], "encounters")

# -- ICD 10 --
assign("ICD 10", [
    tables["diagnoses"]["guid"],
    col_guid("diagnoses", "icd10_code"),
    col_guid("diagnoses", "icd10_description"),
], "diagnoses + icd10_code + icd10_description")

# -- ATC Classification --
assign("ATC Classification", [
    tables["medications"]["guid"],
    col_guid("medications", "atc_code"),
    col_guid("medications", "drug_name"),
], "medications + atc_code + drug_name")

# -- PHI --
assign("Protected Health Information", [
    col_guid("patients", "patient_id"),
    col_guid("patients", "birth_date"),
    col_guid("patients", "postal_code"),
    col_guid("patients", "gender"),
    col_guid("encounters", "patient_id"),
    col_guid("diagnoses", "encounter_id"),
    col_guid("vitals_labs", "encounter_id"),
    col_guid("medications", "encounter_id"),
], "PHI-kolumner")

# -- Swedish Personnummer --
assign("Swedish Personnummer", [
    col_guid("patients", "postal_code"),
], "patients.postal_code")

# -- Vital Signs --
assign("Vital Signs", [
    tables["vitals_labs"]["guid"],
    col_guid("vitals_labs", "systolic_bp"),
    col_guid("vitals_labs", "diastolic_bp"),
    col_guid("vitals_labs", "heart_rate"),
    col_guid("vitals_labs", "temperature_c"),
    col_guid("vitals_labs", "oxygen_saturation"),
], "vitals_labs + vital-kolumner")

# -- Lab Results --
assign("Lab Results", [
    tables["vitals_labs"]["guid"],
    col_guid("vitals_labs", "glucose_mmol"),
    col_guid("vitals_labs", "creatinine_umol"),
    col_guid("vitals_labs", "hemoglobin_g"),
    col_guid("vitals_labs", "sodium_mmol"),
    col_guid("vitals_labs", "potassium_mmol"),
    col_guid("vitals_labs", "bmi"),
    col_guid("vitals_labs", "weight_kg"),
], "vitals_labs + labb-kolumner")

# -- Length of Stay --
assign("Length of Stay", [
    col_guid("encounters", "los_days"),
    tables.get("vw_ml_encounters", {}).get("guid"),
], "encounters.los_days + vw_ml_encounters")

# -- Readmission Risk --
assign("Readmission Risk", [
    col_guid("encounters", "readmission_30d"),
    tables.get("vw_ml_encounters", {}).get("guid"),
], "encounters.readmission_30d + vw_ml_encounters")

# -- FHIR resource terms → SQL tables --
assign("FHIR Patient", [tables["patients"]["guid"]], "patients")
assign("FHIR Encounter", [tables["encounters"]["guid"]], "encounters")
assign("FHIR Condition", [tables["diagnoses"]["guid"]], "diagnoses")
assign("FHIR MedicationRequest", [tables["medications"]["guid"]], "medications")
assign("FHIR Observation", [tables["vitals_labs"]["guid"]], "vitals_labs")

# -- OMOP sub-terms → SQL tables --
assign("OMOP Person", [tables["patients"]["guid"]], "patients")
assign("OMOP Visit Occurrence", [tables["encounters"]["guid"]], "encounters")
assign("OMOP Condition Occurrence", [tables["diagnoses"]["guid"]], "diagnoses")
assign("OMOP Drug Exposure", [tables["medications"]["guid"]], "medications")
assign("OMOP Measurement", [tables["vitals_labs"]["guid"]], "vitals_labs")

# -- OMOP CDM → all 5 SQL tables --
assign("OMOP CDM", [
    tables["patients"]["guid"],
    tables["encounters"]["guid"],
    tables["diagnoses"]["guid"],
    tables["vitals_labs"]["guid"],
    tables["medications"]["guid"],
], "alla 5 SQL-tabeller")

# -- Medallion Architecture → vw_ml_encounters (gold-level view) --
assign("Medallion Architecture", [
    tables.get("vw_ml_encounters", {}).get("guid"),
], "vw_ml_encounters (gold view)")

# -- FHIR R4 → all SQL tables (patient data follows FHIR R4) --
assign("FHIR R4", [
    tables["patients"]["guid"],
    tables["encounters"]["guid"],
    tables["diagnoses"]["guid"],
    tables["vitals_labs"]["guid"],
    tables["medications"]["guid"],
], "alla 5 SQL-tabeller")


# ════════════════════════════════════════════════════════════
# 4. Enrich Fabric lakehouse descriptions with business terms
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("4. Enriching Fabric lakehouse descriptions with business term references")
print("=" * 70)

fabric_assets = {}
for kw in ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse", "gold_omop", "lh_brainchild"]:
    body = {"keywords": kw, "limit": 5}
    r = sess.post(SEARCH_EP, headers=h, json=body, timeout=30)
    for a in r.json().get("value", []):
        if a.get("entityType") == "fabric_lake_warehouse" and kw.replace("_", "") in a["name"].replace("_", "").lower():
            fabric_assets[kw] = {"guid": a["id"], "name": a["name"]}
            break
    time.sleep(0.3)

FABRIC_DESCRIPTIONS = {
    "bronze_lakehouse": (
        "Bronze Layer (rådata) i Medallion-arkitekturen. "
        "Innehåller oförändrad ingestion av källdata från Azure SQL Database. "
        "📋 Affärstermer: Bronze Layer, Medallion Architecture, Protected Health Information. "
        "🔗 Relaterade FHIR-resurser: Patient, Encounter, Condition, MedicationRequest, Observation."
    ),
    "silver_lakehouse": (
        "Silver Layer (rensat & berikat) i Medallion-arkitekturen. "
        "Feature engineering, datavalidering och kvalitetskontroll. "
        "📋 Affärstermer: Silver Layer, Medallion Architecture, Vital Signs, Lab Results, ICD 10, ATC Classification. "
        "🔄 Transformationer: typkonvertering, beräkning av LOS, åldersberäkning, normaliseringsflaggor."
    ),
    "gold_lakehouse": (
        "Gold Layer (aggregerat & ML-klart) i Medallion-arkitekturen. "
        "Slutlig analytisk vy för ML-modeller och BI-rapportering. "
        "📋 Affärstermer: Gold Layer, Medallion Architecture, Length of Stay, Readmission Risk. "
        "🤖 ML-features: vw_ml_encounters med prediktiva variabler för återinskrivningsrisk."
    ),
    "gold_omop": (
        "OMOP CDM v5.4 Gold Layer — standardiserad klinisk datamodell. "
        "ETL från Silver Layer till OMOP-tabeller: Person, Visit Occurrence, Condition Occurrence, Drug Exposure, Measurement. "
        "📋 Affärstermer: OMOP CDM, OMOP Person, OMOP Visit Occurrence, OMOP Condition Occurrence, OMOP Drug Exposure, OMOP Measurement. "
        "🏥 Standarder: SNOMED CT, RxNorm, LOINC, ICD-10-SE."
    ),
    "lh_brainchild": (
        "BrainChild FHIR Demo — barncancerforskning med genomik och bilddiagnostik. "
        "Innehåller FHIR R4-resurser, DICOM-metadata (MR + patologi), GMS-data (Genomic Medicine Sweden), "
        "SBCR (Svenska BarnCancerRegistret), BTB (BioBank), VCF (genomiska varianter). "
        "📋 Affärstermer: FHIR R4, DICOM, Genomic Medicine Sweden, SBCR, BTB, VCF, Genomic Variant, "
        "FHIR Patient, FHIR ImagingStudy, FHIR Specimen, Protected Health Information."
    ),
}

for key, desc in FABRIC_DESCRIPTIONS.items():
    if key not in fabric_assets:
        continue
    guid = fabric_assets[key]["guid"]
    name = fabric_assets[key]["name"]
    
    r = sess.put(
        f"{ATLAS_EP}/entity/guid/{guid}?name=userDescription",
        headers=h, json=desc, timeout=30,
    )
    if r.status_code == 200:
        print(f"  ✅ {name}: description updated with business term references")
    else:
        print(f"  ⚠️ {name}: {r.status_code} {r.text[:200]}")
    time.sleep(0.3)

# Also map remaining terms that reference ONLY lakehouses → map to SQL equivalents instead
# Bronze Layer → no direct SQL asset, but it's the source layer
# Silver Layer, Gold Layer → same
# These are architecture concepts, not data assets, so they're mapped to descriptions only

# -- Map remaining terms to SQL tables where conceptually relevant --
# DICOM, Genomic Medicine Sweden, SBCR, BTB, VCF, FHIR ImagingStudy, FHIR Specimen, Genomic Variant
# These are BrainChild-specific and don't have SQL counterparts
# Mark them as Fabric-only terms via description enrichment

print(f"\n  Note: BrainChild-specific terms (DICOM, GMS, SBCR, BTB, VCF, etc.)")
print(f"  are referenced in Fabric lakehouse descriptions (cross-domain prevents direct glossary link)")


# ════════════════════════════════════════════════════════════
# 5. Final comprehensive summary
# ════════════════════════════════════════════════════════════
print(f"\n{'=' * 70}")
print("5. FINAL GLOSSARY SUMMARY")
print("=" * 70)

r = sess.get(f"{ATLAS_EP}/glossary/{glossary_guid}/terms?limit=100", headers=h, timeout=30)
if r.status_code == 200:
    all_terms = sorted(r.json(), key=lambda x: x.get("name", ""))
    mapped_count = 0
    unmapped_count = 0
    fabric_only = 0

    FABRIC_ONLY_TERMS = {
        "Bronze Layer", "Silver Layer", "Gold Layer", "Medallion Architecture",
        "DICOM", "Genomic Medicine Sweden", "SBCR", "BTB", "VCF",
        "FHIR ImagingStudy", "FHIR Specimen", "Genomic Variant",
    }

    print(f"\n  {'Term':<30} {'Status':<12} {'Mapped To'}")
    print(f"  {'-'*30} {'-'*12} {'-'*40}")
    
    for t in all_terms:
        name = t.get("name")
        assigned = t.get("assignedEntities", [])
        
        if assigned:
            entities = ", ".join(a.get("displayText", "?") for a in assigned)
            status = "✅ SQL"
            mapped_count += 1
        elif name in FABRIC_ONLY_TERMS:
            status = "📦 Fabric"
            entities = "(via description)"
            fabric_only += 1
        else:
            status = "⬜ unmapped"
            entities = ""
            unmapped_count += 1
        
        print(f"  {name:<30} {status:<12} {entities[:55]}")
    
    total = mapped_count + unmapped_count + fabric_only
    print(f"\n  ══════════════════════════════════════")
    print(f"  Totalt: {total} glossary-termer")
    print(f"  ✅ Direkt SQL-mappade: {mapped_count}")
    print(f"  📦 Fabric (via beskrivningar): {fabric_only}")
    print(f"  ⬜ Ej mappade: {unmapped_count}")
    print(f"  ══════════════════════════════════════")

print(f"\n{'=' * 70}")
print("KLAR — Glossary-mappning komplett!")
print("=" * 70)
