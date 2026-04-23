"""Assign all 145 glossary terms to the correct categories."""
import requests, sys, os, time
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
ACCT = "https://prviewacc.purview.azure.com"
ATLAS = ACCT + "/catalog/api/atlas/v2"
GLOSSARY_GUID = "d939ea20-9c67-48af-98d9-b66965f7cde1"

# Category GUIDs
CAT = {
    "Barncancerforskning": "a4b7c43f-b028-4132-a8fe-745f4254234e",
    "Klinisk Data":        "b971f80a-dad9-4742-8626-aa5a07216708",
    "Interoperabilitet":   "7ddea2c9-fb2b-4883-9096-ce3c8dcd1d81",
    "Kliniska Standarder": "716df4e0-9ae5-46c3-90ee-b01c7b5b08d8",
    "Dataarkitektur":      "0363c301-7938-4622-9f27-21a3559b0581",
}

# ── Term → Category mapping ──────────────────────────────────────────────
TERM_MAP = {
    # ── Barncancerforskning (48) ──
    "ACMG-klassificering":                          "Barncancerforskning",
    "ALL (Akut Lymfatisk Leukemi)":                 "Barncancerforskning",
    "AML (Akut Myeloisk Leukemi)":                  "Barncancerforskning",
    "BTB (Barntumörbanken)":                        "Barncancerforskning",
    "Barncancerfonden":                             "Barncancerforskning",
    "Biobank":                                      "Barncancerforskning",
    "Biobankslagen":                                "Barncancerforskning",
    "CAR-T cellterapi":                             "Barncancerforskning",
    "CNS-tumör":                                    "Barncancerforskning",
    "Ewing sarkom":                                 "Barncancerforskning",
    "FFPE (Formalinfixerat paraffin)":              "Barncancerforskning",
    "Flödescytometri":                              "Barncancerforskning",
    "Genomic Medicine Sweden (GMS)":                "Barncancerforskning",
    "Genomisk variant":                             "Barncancerforskning",
    "HGVS-nomenklatur":                             "Barncancerforskning",
    "Histopatologi":                                "Barncancerforskning",
    "Hodgkins lymfom":                              "Barncancerforskning",
    "Immunhistokemi (IHK)":                         "Barncancerforskning",
    "Immunterapi":                                  "Barncancerforskning",
    "Kemoterapi":                                   "Barncancerforskning",
    "Ki-67 Proliferationsindex":                    "Barncancerforskning",
    "Liquid Biopsy (Flytande biopsi)":              "Barncancerforskning",
    "MYCN-amplifiering":                            "Barncancerforskning",
    "Medulloblastom":                               "Barncancerforskning",
    "Minimal Residual Disease (MRD)":               "Barncancerforskning",
    "Molekylär tumörboard":                         "Barncancerforskning",
    "NOPHO (Nordic Society of Paediatric Haematology and Oncology)": "Barncancerforskning",
    "Neuroblastom":                                 "Barncancerforskning",
    "Non-Hodgkins lymfom (barn)":                   "Barncancerforskning",
    "Osteosarkom":                                  "Barncancerforskning",
    "Protonterapi":                                 "Barncancerforskning",
    "RNA-sekvensering":                             "Barncancerforskning",
    "Retinoblastom":                                "Barncancerforskning",
    "Rhabdomyosarkom":                              "Barncancerforskning",
    "SBCR (Svenska Barncancerregistret)":           "Barncancerforskning",
    "SIOP (International Society of Paediatric Oncology)": "Barncancerforskning",
    "Seneffekter":                                  "Barncancerforskning",
    "Seneffektsmottagning":                         "Barncancerforskning",
    "Stamcellstransplantation":                     "Barncancerforskning",
    "Strålbehandling":                              "Barncancerforskning",
    "Tumörmutationsbörda (TMB)":                    "Barncancerforskning",
    "Tumörsite":                                    "Barncancerforskning",
    "Tumörstadium":                                 "Barncancerforskning",
    "VCF (Variant Call Format)":                    "Barncancerforskning",
    "Whole Exome Sequencing (WES)":                 "Barncancerforskning",
    "Whole Genome Sequencing (WGS)":                "Barncancerforskning",
    "Wilms tumör (Nefroblastom)":                   "Barncancerforskning",
    "Överlevnadsanalys":                            "Barncancerforskning",

    # ── Klinisk Data (36) ──
    "ALAT/ASAT (Levertransaminaser)":               "Klinisk Data",
    "Akutmottagning":                               "Klinisk Data",
    "Behandlingsprotokoll":                         "Klinisk Data",
    "Blodstatus (Hb, LPK, TPK)":                   "Klinisk Data",
    "Brytpunktssamtal":                             "Klinisk Data",
    "CRP (C-reaktivt protein)":                     "Klinisk Data",
    "Charlson Comorbidity Index":                   "Klinisk Data",
    "Epikris":                                      "Klinisk Data",
    "FLAIR":                                        "Klinisk Data",
    "GCS (Glasgow Coma Scale)":                     "Klinisk Data",
    "Informerat samtycke (kliniskt)":               "Klinisk Data",
    "Intensivvård (IVA)":                           "Klinisk Data",
    "Kontaktsjuksköterska":                         "Klinisk Data",
    "Kreatinin":                                    "Klinisk Data",
    "Kvalitetsregister":                            "Klinisk Data",
    "Labresultat":                                  "Klinisk Data",
    "MR (Magnetresonanstomografi)":                 "Klinisk Data",
    "Multidisciplinär konferens (MDK)":             "Klinisk Data",
    "NEWS (National Early Warning Score)":          "Klinisk Data",
    "Palliativ vård":                               "Klinisk Data",
    "Patientdemografi":                             "Klinisk Data",
    "Personnummer":                                 "Klinisk Data",
    "Rehabilitering (barn)":                        "Klinisk Data",
    "Remiss":                                       "Klinisk Data",
    "Svenskt personnummer":                         "Klinisk Data",
    "T1-viktad MR":                                 "Klinisk Data",
    "T2-viktad MR":                                 "Klinisk Data",
    "Triage":                                       "Klinisk Data",
    "Troponin":                                     "Klinisk Data",
    "VAS/NRS Smärtskattning":                       "Klinisk Data",
    "Vitalparametrar":                              "Klinisk Data",
    "Vårdkontakt":                                  "Klinisk Data",
    "Vårdplan":                                     "Klinisk Data",
    "Vårdtid (LOS)":                                "Klinisk Data",
    "eGFR (Estimerad Glomerulär Filtration)":       "Klinisk Data",
    "Återinläggningsrisk":                          "Klinisk Data",

    # ── Interoperabilitet (19) ──
    "CDA (Clinical Document Architecture)":         "Interoperabilitet",
    "DICOM":                                        "Interoperabilitet",
    "DICOMweb":                                     "Interoperabilitet",
    "FHIR Condition":                               "Interoperabilitet",
    "FHIR DiagnosticReport":                        "Interoperabilitet",
    "FHIR Encounter":                               "Interoperabilitet",
    "FHIR ImagingStudy":                            "Interoperabilitet",
    "FHIR MedicationRequest":                       "Interoperabilitet",
    "FHIR Observation":                             "Interoperabilitet",
    "FHIR Patient":                                 "Interoperabilitet",
    "FHIR R4":                                      "Interoperabilitet",
    "FHIR Specimen":                                "Interoperabilitet",
    "HL7 v2":                                       "Interoperabilitet",
    "IHE-profiler":                                 "Interoperabilitet",
    "Inera":                                        "Interoperabilitet",
    "Nationell Patientöversikt (NPÖ)":              "Interoperabilitet",
    "SITHS-kort":                                   "Interoperabilitet",
    "Terminologitjänst":                            "Interoperabilitet",
    "openEHR":                                      "Interoperabilitet",

    # ── Kliniska Standarder (18) ──
    "ATC (Anatomical Therapeutic Chemical Classification)": "Kliniska Standarder",
    "ATC-klassificering":                           "Kliniska Standarder",
    "DRG-klassificering":                           "Kliniska Standarder",
    "Etikprövning":                                 "Kliniska Standarder",
    "Etikprövningslagen":                           "Kliniska Standarder",
    "GCP (Good Clinical Practice)":                 "Kliniska Standarder",
    "GDPR i vården":                                "Kliniska Standarder",
    "ICD-10":                                       "Kliniska Standarder",
    "ICD-O-3":                                      "Kliniska Standarder",
    "ICF (International Classification of Functioning)": "Kliniska Standarder",
    "Informerat samtycke":                          "Kliniska Standarder",
    "KVÅ (Klassifikation av vårdåtgärder)":         "Kliniska Standarder",
    "LOINC":                                        "Kliniska Standarder",
    "NordDRG":                                      "Kliniska Standarder",
    "Patientdatalagen (PDL)":                       "Kliniska Standarder",
    "Pseudonymisering":                             "Kliniska Standarder",
    "SNOMED-CT":                                    "Kliniska Standarder",
    "Skyddad hälsoinformation (PHI)":               "Kliniska Standarder",

    # ── Dataarkitektur (24) ──
    "Apache Spark":                                 "Dataarkitektur",
    "Bronze-lager":                                 "Dataarkitektur",
    "Data Lakehouse":                               "Dataarkitektur",
    "Data Lineage":                                 "Dataarkitektur",
    "Data Mesh":                                    "Dataarkitektur",
    "Data Quality Score":                           "Dataarkitektur",
    "Delta Lake":                                   "Dataarkitektur",
    "ETL-pipeline":                                 "Dataarkitektur",
    "Feature Engineering":                          "Dataarkitektur",
    "Feature Store":                                "Dataarkitektur",
    "Gold-lager":                                   "Dataarkitektur",
    "ML-prediktion":                                "Dataarkitektur",
    "Master Data Management (MDM)":                 "Dataarkitektur",
    "Medallion-arkitektur":                         "Dataarkitektur",
    "OMOP CDM":                                     "Dataarkitektur",
    "OMOP Condition Occurrence":                    "Dataarkitektur",
    "OMOP Drug Exposure":                           "Dataarkitektur",
    "OMOP Genomics":                                "Dataarkitektur",
    "OMOP Measurement":                             "Dataarkitektur",
    "OMOP Person":                                  "Dataarkitektur",
    "OMOP Specimen":                                "Dataarkitektur",
    "OMOP Visit Occurrence":                        "Dataarkitektur",
    "Schema Evolution":                             "Dataarkitektur",
    "Silver-lager":                                 "Dataarkitektur",
}

# ── Sanity check ──
assert len(TERM_MAP) == 145, f"Expected 145 terms, got {len(TERM_MAP)}"

# ── Fetch all terms ──
print("Fetching all glossary terms...")
all_terms = []
offset = 0
while True:
    r = requests.get(f"{ATLAS}/glossary/{GLOSSARY_GUID}/terms?limit=100&offset={offset}",
                     headers=h, timeout=15)
    if r.status_code != 200:
        print(f"  ERROR fetching terms: {r.status_code}")
        sys.exit(1)
    batch = r.json()
    if not batch:
        break
    all_terms.extend(batch)
    offset += len(batch)
    if len(batch) < 100:
        break
print(f"  Found {len(all_terms)} terms")

# Build name→term lookup
name_to_term = {t["name"]: t for t in all_terms}

# ── Assign categories ──
stats = {cat: 0 for cat in CAT}
errors = []
skipped = []

for i, (term_name, cat_name) in enumerate(sorted(TERM_MAP.items())):
    term = name_to_term.get(term_name)
    if not term:
        errors.append(f"Term not found: {term_name}")
        continue

    existing_cats = term.get("categories", [])
    if existing_cats:
        skipped.append(term_name)
        stats[cat_name] += 1
        continue

    term_guid = term["guid"]
    cat_guid = CAT[cat_name]

    # GET full term
    r = requests.get(f"{ATLAS}/glossary/term/{term_guid}", headers=h, timeout=15)
    if r.status_code != 200:
        errors.append(f"GET {term_name}: {r.status_code}")
        continue

    full_term = r.json()
    full_term["categories"] = [{"categoryGuid": cat_guid}]

    # PUT updated term
    r2 = requests.put(f"{ATLAS}/glossary/term/{term_guid}",
                      headers=h, json=full_term, timeout=15)
    if r2.status_code in (200, 204):
        stats[cat_name] += 1
        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/145] Assigned {term_name} -> {cat_name}")
    else:
        errors.append(f"PUT {term_name}: {r2.status_code} - {r2.text[:200]}")

    # Small delay to avoid throttling
    if (i + 1) % 20 == 0:
        time.sleep(0.5)

# ── Report ──
print("\n" + "=" * 60)
print("CATEGORY ASSIGNMENT RESULTS")
print("=" * 60)
total_ok = sum(stats.values())
for cat, cnt in sorted(stats.items()):
    print(f"  {cat}: {cnt} terms")
print(f"\n  TOTAL assigned: {total_ok}/145")

if skipped:
    print(f"\n  Already categorized (skipped): {len(skipped)}")
if errors:
    print(f"\n  ERRORS ({len(errors)}):")
    for e in errors:
        print(f"    {e}")

print("\nDone!")
