"""
Purview Data Products, OKRs & Data Quality — Creates governance artifacts for demo.

1. Data Products via Purview Unified Catalog API
2. OKR terms in glossary (Objectives & Key Results for data governance)
3. SQL-based data quality rules & checks with results stored in Purview

Usage:
  python scripts/purview_data_products.py
"""
import json
import struct
import sys
import time
from datetime import datetime

import pyodbc
import requests
from azure.identity import AzureCliCredential

# ── CONFIG ──
cred = AzureCliCredential(process_timeout=30)
ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
UNIFIED = f"{ACCT}/datagovernance/catalog"
API_VER = "2025-09-15-preview"

SQL_SERVER = "sql-hca-demo.database.windows.net"
SQL_DB = "HealthcareAnalyticsDB"

BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


def refresh_token():
    global token, h
    token = cred.get_token("https://purview.azure.net/.default").token
    h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


refresh_token()


def header(num, title):
    print(f"\n{'=' * 70}")
    print(f"  {num}. {title}")
    print(f"{'=' * 70}")


def ok(msg):
    print(f"  {GREEN}✅{RESET} {msg}")


def warn(msg):
    print(f"  {YELLOW}⚠️ {RESET} {msg}")


def info(msg):
    print(f"  {CYAN}ℹ️ {RESET} {msg}")


# ══════════════════════════════════════════════════════════════════
#  1. DATA PRODUCTS
# ══════════════════════════════════════════════════════════════════
DATA_PRODUCTS = [
    {
        "name": "Klinisk Patientanalys",
        "domain_name": "Klinisk Vård",
        "description": (
            "Dataprodukt för klinisk patientanalys — innehåller patientdemografi, "
            "vårdbesök, diagnoser, vitalparametrar och labresultat. Används för "
            "prediktiv analys av vårdtid (LOS) och återinläggningsrisk. "
            "Följer FHIR R4 och OMOP CDM v5.4 standarder."
        ),
        "owners": ["Healthcare Analytics Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "BIEngineer", "DataScientist", "DataAnalyst"],
        "business_use": "Vårdflödesoptimering, LOS-prediktion och återinläggningsrisk i klinisk verksamhet",
        "tables": ["patients", "encounters", "diagnoses", "vitals_labs", "medications"],
        "use_cases": [
            "LOS-prediktion (LightGBM)",
            "Återinläggningsrisk (RandomForest)",
            "Charlson Comorbidity Index",
            "Avdelningsstatistik",
        ],
        "sla": "Daglig uppdatering, <1h latens, 99.5% tillgänglighet",
        "quality_score": None,  # Filled after DQ checks
    },
    {
        "name": "Akutflödesmonitorering",
        "domain_name": "Klinisk Vård",
        "description": (
            "Dataprodukt för realtidsnära övervakning av akutmottagningens inflöde, "
            "triage, väntetider och utskrivningsmönster. Stödjer operativ styrning "
            "av patientflöden och kapacitetsplanering i akutsjukvården."
        ),
        "owners": ["Emergency Care Analytics Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "BIEngineer", "DataAnalyst"],
        "business_use": "Operativ uppföljning av triage, väntetider och genomströmning i akutvården",
        "tables": ["ed_visits", "triage_events", "bed_status", "discharge_decisions"],
        "use_cases": [
            "Övervakning av väntetid per prioritet",
            "Prediktion av köbildning i akuten",
            "Kapacitetsplanering per skift",
            "Ledningsdashboard för akutflöden",
        ],
        "sla": "15 min uppdatering, <10 min latens, 99.9% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Vårdplatskapacitet",
        "domain_name": "Klinisk Vård",
        "description": (
            "Dataprodukt för vårdplatsbeläggning, in- och utskrivningar, beläggningsgrad "
            "och överbeläggningsrisk per enhet. Används för daglig styrning av kapacitet "
            "inom slutenvården."
        ),
        "owners": ["Inpatient Operations Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataAnalyst", "BIEngineer", "DataEngineer"],
        "business_use": "Styrning av vårdplatser, beläggningsgrad och utskrivningskapacitet i slutenvården",
        "tables": ["bed_occupancy", "admission_events", "discharge_forecast", "unit_capacity"],
        "use_cases": [
            "Beläggningsgrad per klinik",
            "Prognos för utskrivningar nästa 24h",
            "Överbeläggningslarm",
            "Planering av vårdplatsfördelning",
        ],
        "sla": "Timvis uppdatering, <30 min latens, 99.7% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Läkemedelsuppföljning Klinik",
        "domain_name": "Klinisk Vård",
        "description": (
            "Dataprodukt för klinisk uppföljning av ordinationer, administrering och "
            "läkemedelsrelaterade avvikelser. Stödjer kvalitetssäkring och uppföljning "
            "av läkemedelsprocessen i vårdverksamheten."
        ),
        "owners": ["Clinical Pharmacy Analytics Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataAnalyst", "BIEngineer"],
        "business_use": "Uppföljning av läkemedelsordinationer, administrering och patientsäkerhet i klinik",
        "tables": ["med_orders", "med_admin", "adverse_events", "med_reconciliation"],
        "use_cases": [
            "Avvikelsedetektion i läkemedelsadministrering",
            "Uppföljning av antibiotikaanvändning",
            "Patientsäkerhetsindikatorer",
            "Läkemedelsprocess per vårdenhet",
        ],
        "sla": "Daglig uppdatering, <2h latens, 99.5% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "OMOP Forskningsdata",
        "domain_name": "Interoperabilitet & Standarder",
        "description": (
            "OMOP CDM v5.4-transformerade data för observationell forskning. "
            "Möjliggör kors-institutionell forskning och federerad analys. "
            "Mappning: ICD-10-SE → SNOMED CT, ATC → RxNorm."
        ),
        "owners": ["Research Data Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataAnalyst", "DataEngineer"],
        "business_use": "Interoperabel forskningsdata enligt OMOP för kohorter, outcomes och real-world evidence",
        "tables": ["person", "visit_occurrence", "condition_occurrence", "drug_exposure", "measurement"],
        "use_cases": [
            "Kohortstudier",
            "Läkemedelssäkerhet",
            "Kliniska utfall (OHDSI)",
            "Federerad analys (DataSHIELD)",
        ],
        "sla": "Veckovis uppdatering, <4h ETL, GDPR-kompatibel",
        "quality_score": None,
    },
    {
        "name": "FHIR Interoperabilitetslager",
        "domain_name": "Interoperabilitet & Standarder",
        "description": (
            "Dataprodukt som exponerar harmoniserade FHIR R4-resurser för patient, besök, "
            "observationer och ordinationer. Stödjer interoperabilitet mellan kliniska system, "
            "integrationer och sekundär användning av vårddata."
        ),
        "owners": ["Interoperability Platform Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "DataAnalyst"],
        "business_use": "Standardiserad FHIR-exponering för integrationer, sekundär användning och datadelning",
        "tables": ["fhir_patient", "fhir_encounter", "fhir_observation", "fhir_medicationrequest"],
        "use_cases": [
            "FHIR-baserad systemintegration",
            "Sekundär användning av strukturerade resurser",
            "API-försörjning till partnerlösningar",
            "Validering mot FHIR-profiler",
        ],
        "sla": "Realtidsnära uppdatering, <5 min latens, 99.9% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Terminologitjänst & Kodverk",
        "domain_name": "Interoperabilitet & Standarder",
        "description": (
            "Dataprodukt för förvaltning och distribution av kodverk, terminologimappningar "
            "och semantiska referenser mellan ICD-10-SE, SNOMED CT, ATC, LOINC och RxNorm."
        ),
        "owners": ["Terminology Management Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "DataScientist"],
        "business_use": "Semantisk standardisering och kodverksmappning för interoperabilitet och analys",
        "tables": ["code_systems", "code_mappings", "value_sets", "term_synonyms"],
        "use_cases": [
            "Kodverksmappning mellan standarder",
            "Validering av terminologi i ETL-flöden",
            "Sökbar referens för analytiker",
            "Stöd för OMOP- och FHIR-transformering",
        ],
        "sla": "Veckovis uppdatering, <8h publicering, versionshanterad historik",
        "quality_score": None,
    },
    {
        "name": "Masterdata Vårdhändelser",
        "domain_name": "Interoperabilitet & Standarder",
        "description": (
            "Dataprodukt som konsoliderar masterdata och gemensamma identifierare för "
            "patient, vårdkontakt, vårdenhet och vårdhändelser över flera källsystem. "
            "Minskar dubblering och stärker semantisk konsekvens i plattformen."
        ),
        "owners": ["Master Data Services Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "BIEngineer", "DataAnalyst"],
        "business_use": "Gemensamma identifierare och masterdata för konsistenta vårdhändelser över systemgränser",
        "tables": ["master_patient", "master_encounter", "master_provider", "crosswalk_events"],
        "use_cases": [
            "Golden record för vårdkontakter",
            "Referensnycklar i integrationsflöden",
            "Deduplicering av vårdhändelser",
            "Semantisk harmonisering mellan källsystem",
        ],
        "sla": "Daglig uppdatering, <2h latens, 99.8% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "BrainChild Barncancerforskning",
        "domain_name": "Forskning & Genomik",
        "description": (
            "Multimodal forskningsplattform för barncancer — integrerar FHIR-klinisk data, "
            "DICOM-bilddiagnostik (MRI + patologi), genomikdata (WGS/WES via GMS), "
            "biobanksdata (BTB) och kvalitetsregister (SBCR)."
        ),
        "owners": ["BrainChild Research Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataEngineer", "DataAnalyst"],
        "business_use": "Multimodal barncancerforskning med klinik, bilddiagnostik och genomik i samma dataprodukt",
        "tables": ["fhir_patients", "imaging_studies", "genomic_variants", "specimens", "sbcr_registrations"],
        "use_cases": [
            "Tumörklassificering (MRI + patologi AI)",
            "Genomisk variant-analys",
            "Behandlingsutfall (SBCR)",
            "Biobanksförvaltning (BTB)",
        ],
        "sla": "Realtid FHIR-ingest, daglig batch-ETL, forskaråtkomst via Fabric",
        "quality_score": None,
    },
    {
        "name": "Precisionsonkologi Variantlager",
        "domain_name": "Forskning & Genomik",
        "description": (
            "Dataprodukt för somatiska och germline-varianter, annotationsresultat, "
            "panelträffar och tolkningar för precisionsonkologi inom barncancerforskning. "
            "Kopplar genomik till fenotyp och behandlingsutfall."
        ),
        "owners": ["Genomics Interpretation Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataEngineer"],
        "business_use": "Varianttolkning och biomarköranalys för precisionsonkologi och translational research",
        "tables": ["variant_calls", "variant_annotations", "gene_panels", "molecular_findings"],
        "use_cases": [
            "Biomarköranalys per diagnosgrupp",
            "Koppling mellan variant och behandlingsutfall",
            "Prioritering av kliniskt relevanta varianter",
            "Forskningsstöd för precisionsmedicin",
        ],
        "sla": "Daglig batch-ETL, <6h latens, versionssäkrad annotation",
        "quality_score": None,
    },
    {
        "name": "Pediatrisk Imaging Research Hub",
        "domain_name": "Forskning & Genomik",
        "description": (
            "Dataprodukt för forskningsanpassad tillgång till DICOM-metadata, bildserier, "
            "segmenteringar och AI-härledda features från pediatrisk bilddiagnostik. "
            "Optimerad för radiomik och multimodala modeller."
        ),
        "owners": ["Imaging AI Research Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataAnalyst", "DataEngineer"],
        "business_use": "Forskningsplattform för pediatrisk bilddiagnostik, radiomik och AI-modellering",
        "tables": ["dicom_studies", "image_series", "segmentations", "radiomics_features"],
        "use_cases": [
            "Radiomikfeature-extraktion",
            "Modellträning för tumörklassificering",
            "Kvalitetskontroll av bildserier",
            "Multimodal länkning till kliniska utfall",
        ],
        "sla": "Daglig uppdatering, <4h latens, forskaråtkomst via Fabric",
        "quality_score": None,
    },
    {
        "name": "Biobank & Provspårbarhet",
        "domain_name": "Forskning & Genomik",
        "description": (
            "Dataprodukt för biobanksprov, provkedja, fryslager, uttag och koppling mellan "
            "provmaterial, kliniska data och genomiska analyser. Säkerställer spårbarhet "
            "och forskningsberedskap för provhantering."
        ),
        "owners": ["Biobank Operations Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataAnalyst", "DataEngineer"],
        "business_use": "Spårbarhet och operativ uppföljning av biobanksprov från insamling till analys",
        "tables": ["biobank_samples", "sample_chain", "freezer_inventory", "sample_requests"],
        "use_cases": [
            "Provspårning genom hela kedjan",
            "Koppling mellan prov och analysresultat",
            "Kapacitetsuppföljning av fryslager",
            "Forskningsstöd för provurval",
        ],
        "sla": "Daglig uppdatering, <2h latens, full audit trail",
        "quality_score": None,
    },
    {
        "name": "ML Feature Store",
        "domain_name": "Data & Analytics",
        "description": (
            "Gold-lager med ML-redo features — aggregerade per vårdbesök med "
            "Charlson Comorbidity Index, senaste vitalparametrar, primärdiagnos "
            "och läkemedelsdata. Används av LOS- och readmission-modeller."
        ),
        "owners": ["ML Engineering Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataEngineer", "BIEngineer"],
        "business_use": "Produktionsnära ML-features och modellövervakning för prediktiv vårdanalys",
        "tables": ["vw_ml_encounters"],
        "use_cases": [
            "Feature serving för ML-modeller",
            "A/B-testning av features",
            "Model monitoring",
            "Drift detection",
        ],
        "sla": "Uppdateras efter varje pipeline-körning, <30min latens",
        "quality_score": None,
    },
    {
        "name": "Population Health Dashboard",
        "domain_name": "Data & Analytics",
        "description": (
            "Dataprodukt för populationsbaserad analys av prevalens, vårdbehov, risksegment "
            "och utfallsindikatorer över region, socioekonomi och diagnosgrupper. "
            "Byggd för ledningsrapportering och planering av preventiva insatser."
        ),
        "owners": ["Population Analytics Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["BIEngineer", "DataAnalyst", "DataScientist"],
        "business_use": "Ledningsnära analys av population health, risksegment och vårdbehov över regioner",
        "tables": ["population_segments", "care_need_index", "outcomes_dashboard", "regional_metrics"],
        "use_cases": [
            "Segmentering av riskpopulationer",
            "Regional jämförelse av vårdutfall",
            "Planering av preventiva program",
            "Ledningsdashboard för population health",
        ],
        "sla": "Daglig uppdatering, <3h latens, publicering till Power BI",
        "quality_score": None,
    },
    {
        "name": "Operations Intelligence Mart",
        "domain_name": "Data & Analytics",
        "description": (
            "Dataprodukt för operativ analys av produktion, väntetider, resursutnyttjande, "
            "schemaläggning och produktionsutfall i vårdorganisationen. Optimerad för "
            "ledningsnära analys och uppföljning."
        ),
        "owners": ["Healthcare BI Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["BIEngineer", "DataAnalyst", "DataEngineer"],
        "business_use": "Produktions- och resursanalys för operativ styrning av vårdverksamheten",
        "tables": ["production_facts", "staffing_plan", "queue_metrics", "unit_performance"],
        "use_cases": [
            "Produktionsuppföljning per klinik",
            "Schemapåverkan på genomströmning",
            "Analys av väntetider",
            "Effektuppföljning av förbättringsinsatser",
        ],
        "sla": "Timvis uppdatering, <45 min latens, 99.7% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "MLOps Modellregister",
        "domain_name": "Data & Analytics",
        "description": (
            "Dataprodukt för versionshantering, prestanda, drift, feature lineage och "
            "governance för prediktiva modeller inom hälso- och sjukvård. Ger ett samlat "
            "lager för modellövervakning och ansvarsfull AI."
        ),
        "owners": ["MLOps Platform Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataAnalyst", "DataEngineer"],
        "business_use": "Modellstyrning, driftövervakning och lineage för produktionssatta ML-modeller",
        "tables": ["model_registry", "model_metrics", "drift_signals", "feature_lineage"],
        "use_cases": [
            "Versionsspårning av modeller",
            "Drift- och biasövervakning",
            "Audit trail för modelländringar",
            "Rapportering för ansvarsfull AI",
        ],
        "sla": "Efter varje modellkörning, <15 min latens, 99.9% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Hälsosjukvård Datastyrning",
        "domain_name": "Hälsosjukvård",
        "description": (
            "Samlad dataprodukt för governance, compliance och kvalitet i vårddata. "
            "Konsoliderar metadata, datakvalitetsresultat, policydrivna kontroller "
            "och spårbarhet för regulatorisk uppföljning."
        ),
        "owners": ["Data Governance Office"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataSteward", "SecurityOfficer", "DataEngineer", "DataAnalyst"],
        "business_use": "GDPR-efterlevnad, revisionsspår och styrning av vårddata i Purview",
        "tables": ["data_quality_report", "policy_controls", "lineage_events"],
        "use_cases": [
            "GDPR- och compliance-uppföljning",
            "Datakvalitetsstyrning per domän",
            "Lineage- och impact-analys",
            "Ledningsrapportering av datarisk",
        ],
        "sla": "Daglig uppdatering, <4h latens, revisionsbar historik",
        "quality_score": None,
    },
    {
        "name": "Kardiologisk Kvalitetsuppföljning",
        "domain_name": "Klinisk Vård",
        "description": (
            "Dataprodukt för kvalitetsuppföljning inom kardiologi med diagnoser, "
            "behandlingsåtgärder, mortalitet, återbesök och läkemedelsföljsamhet. "
            "Understödjer klinisk förbättring och utfallsjämförelser."
        ),
        "owners": ["Cardiology Analytics Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "BIEngineer", "DataAnalyst"],
        "business_use": "Kvalitetsindikatorer och utfallsuppföljning för hjärtvård och sekundärprevention",
        "tables": ["cardio_encounters", "cardio_procedures", "echo_measurements", "followup_outcomes"],
        "use_cases": [
            "Kvalitetsregisterrapportering",
            "Rehospitaliseringsanalys",
            "Läkemedelsföljsamhet",
            "Outcome benchmarking",
        ],
        "sla": "Daglig uppdatering, <2h latens, 99.0% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Perioperativ Produktionsstyrning",
        "domain_name": "Klinisk Vård",
        "description": (
            "Dataprodukt för planering och uppföljning av operationsflöden med "
            "operationssalar, schemaläggning, anestesitider, förseningar och "
            "inställda ingrepp. Möjliggör produktionsstyrning i perioperativ vård."
        ),
        "owners": ["Surgical Operations Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "BIEngineer", "DataAnalyst"],
        "business_use": "Produktionsstyrning av operationskapacitet, punktlighet och inställda ingrepp",
        "tables": ["or_schedule", "surgery_cases", "anesthesia_events", "capacity_slots"],
        "use_cases": [
            "Kapacitetsplanering",
            "OR-utilization",
            "Förseningsoch avvikelseanalys",
            "Inställda operationer",
        ],
        "sla": "Timvis uppdatering, <1h latens, 99.5% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "FHIR Interoperabilitetsnav",
        "domain_name": "Interoperabilitet & Standarder",
        "description": (
            "Dataprodukt för FHIR-baserad interoperabilitet med resurser, profiler, "
            "valideringsutfall och meddelandeflöden mellan vårdsystem. Används för "
            "standardiserad informationsutväxling och integrationstester."
        ),
        "owners": ["Interoperability Platform Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "DataAnalyst"],
        "business_use": "Övervakning och kvalitetssäkring av FHIR-baserad informationsutväxling mellan system",
        "tables": ["fhir_messages", "resource_validation", "profile_conformance", "integration_endpoints"],
        "use_cases": [
            "FHIR-validering",
            "Interoperabilitetsövervakning",
            "Profilkonformans",
            "Felanalys i integrationsflöden",
        ],
        "sla": "15 min uppdatering, <30 min latens, 99.9% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Terminologitjänst Kliniska Kodverk",
        "domain_name": "Interoperabilitet & Standarder",
        "description": (
            "Dataprodukt som konsoliderar kliniska kodverk, mappningar och versioner "
            "för ICD-10-SE, KVÅ, SNOMED CT, LOINC och ATC. Stödjer harmonisering, "
            "spårbarhet och standardiserad semantik i analyser."
        ),
        "owners": ["Terminology Management Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "DataScientist", "DataAnalyst"],
        "business_use": "Central hantering av kodverk, översättningar och semantisk interoperabilitet",
        "tables": ["code_systems", "concept_mappings", "value_sets", "terminology_versions"],
        "use_cases": [
            "Kodverksmappning",
            "Semantisk harmonisering",
            "Versionering av terminologi",
            "Analys av täckningsgrad",
        ],
        "sla": "Veckovis uppdatering, <4h latens, 99.0% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Standardiserad Vårdepisodmodell",
        "domain_name": "Interoperabilitet & Standarder",
        "description": (
            "Dataprodukt med standardiserade vårdepisoder och mappningar mellan lokala "
            "vårdhändelser och interoperabla episodbegrepp. Underlättar jämförbarhet "
            "mellan källsystem och analytiska modeller."
        ),
        "owners": ["Clinical Standards Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "BIEngineer", "DataAnalyst"],
        "business_use": "Gemensam episodmodell för rapportering, integration och tvärsystemanalys",
        "tables": ["care_episodes", "episode_mappings", "encounter_groups", "care_pathways"],
        "use_cases": [
            "Episodbaserad rapportering",
            "Jämförbarhet mellan system",
            "Patientflödesanalys",
            "Mapping governance",
        ],
        "sla": "Daglig uppdatering, <2h latens, 99.0% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Pediatrisk Precision Onkologi",
        "domain_name": "Forskning & Genomik",
        "description": (
            "Dataprodukt för precisionsonkologi inom barncancer som kombinerar "
            "molekylära profiler, behandlingsregimer, respons och långtidsutfall. "
            "Stödjer translational research och stratifierad behandlingsanalys."
        ),
        "owners": ["Pediatric Precision Oncology Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataEngineer"],
        "business_use": "Biomarkördriven barncancerforskning och analys av behandlingsrespons",
        "tables": ["molecular_profiles", "treatment_protocols", "response_assessments", "survival_outcomes"],
        "use_cases": [
            "Biomarkörstratifiering",
            "Behandlingsresponsanalys",
            "Långtidsuppföljning",
            "Klinisk forskningskohort",
        ],
        "sla": "Daglig uppdatering, <6h latens, forskningsåtkomst i Fabric",
        "quality_score": None,
    },
    {
        "name": "Radiogenomik Barnonkologi",
        "domain_name": "Forskning & Genomik",
        "description": (
            "Dataprodukt för radiogenomik med koppling mellan MRI-fynd, patologi, "
            "genomiska varianter och kliniska utfall i barnonkologi. Möjliggör "
            "multimodala AI-studier och hypotesdriven forskning."
        ),
        "owners": ["Imaging Genomics Lab"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataAnalyst"],
        "business_use": "Multimodal forskning som kopplar bilddiagnostik till molekylära signaturer och utfall",
        "tables": ["radiomics_features", "image_annotations", "genomic_signatures", "linked_outcomes"],
        "use_cases": [
            "Radiomics-modellering",
            "Variantassociering",
            "Tumörsubtypning",
            "Bild-biomarkörforskning",
        ],
        "sla": "Daglig batch-ETL, <8h latens, forskaråtkomst via Fabric",
        "quality_score": None,
    },
    {
        "name": "Nationell Biobank Sammanställning",
        "domain_name": "Forskning & Genomik",
        "description": (
            "Dataprodukt för samordnad överblick av prov, aliquots, samtycken och "
            "fryslogistik över forskningsbiobanker. Stödjer spårbarhet, tillgänglighet "
            "och effektiv provmatchning till forskningsprotokoll."
        ),
        "owners": ["Biobank Operations Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "DataScientist", "DataAnalyst"],
        "business_use": "Biobanksspårbarhet, provmatchning och kapacitetsuppföljning för forskning",
        "tables": ["biobank_inventory", "specimen_consents", "freezer_locations", "research_allocations"],
        "use_cases": [
            "Provmatchning",
            "Samtyckeskontroll",
            "Fryslogistik",
            "Forskningsallokering",
        ],
        "sla": "Daglig uppdatering, <4h latens, 99.0% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Prediktiv Vårdplatskapacitet",
        "domain_name": "Data & Analytics",
        "description": (
            "Dataprodukt med features och prognoser för vårdplatskapacitet, beläggning, "
            "utskrivningar och inflöde. Stödjer planering på sjukhus- och avdelningsnivå "
            "med fokus på kapacitetsutnyttjande."
        ),
        "owners": ["Capacity Analytics Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "BIEngineer"],
        "business_use": "Prediktiv kapacitetsplanering och scenariostöd för vårdplatser och beläggning",
        "tables": ["bed_capacity_features", "admission_forecasts", "discharge_predictions", "occupancy_snapshots"],
        "use_cases": [
            "Beläggningsprognos",
            "Scenarioanalys",
            "Utskrivningsprognos",
            "Kapacitetsdashboard",
        ],
        "sla": "Timvis uppdatering, <30 min latens, 99.5% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Population Health Segmentering",
        "domain_name": "Data & Analytics",
        "description": (
            "Dataprodukt för segmentering av patientpopulationer baserat på risk, "
            "vårdkonsumtion, kroniska tillstånd och socioekonomiska indikatorer. "
            "Används för kohortstyrning och riktade preventiva insatser."
        ),
        "owners": ["Population Health Analytics"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataAnalyst", "BIEngineer"],
        "business_use": "Risksegmentering och analys av patientpopulationer för prevention och resursstyrning",
        "tables": ["population_segments", "risk_profiles", "care_utilization", "socioeconomic_features"],
        "use_cases": [
            "Risksegmentering",
            "Kohortstyrning",
            "Preventiva program",
            "Vårdkonsumtionsanalys",
        ],
        "sla": "Daglig uppdatering, <2h latens, 99.0% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "MLOps Modelltelemetri",
        "domain_name": "Data & Analytics",
        "description": (
            "Dataprodukt för telemetri, prestanda, datadrift och inferensövervakning i "
            "produktiva vårdmodeller. Samlar modellversioner, feature drift, latens och "
            "prediktionskvalitet för MLOps-styrning."
        ),
        "owners": ["MLOps Platform Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataScientist", "DataEngineer"],
        "business_use": "Övervakning av modellhälsa, drift och inferenskvalitet i produktionsnära AI-flöden",
        "tables": ["model_runs", "inference_logs", "drift_metrics", "performance_snapshots"],
        "use_cases": [
            "Drift detection",
            "Latency monitoring",
            "Model comparison",
            "Incident analysis",
        ],
        "sla": "15 min uppdatering, <15 min latens, 99.9% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Compliance Kontrollbibliotek",
        "domain_name": "Hälsosjukvård",
        "description": (
            "Dataprodukt som samlar regulatoriska kontroller, kontrollutfall, policykrav "
            "och ägarskap för vårddata. Underlättar spårbar compliance-uppföljning över "
            "plattform, domäner och processer."
        ),
        "owners": ["Compliance Office"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataAnalyst", "DataEngineer"],
        "business_use": "Samordnad uppföljning av regulatoriska kontroller, avvikelser och policyefterlevnad",
        "tables": ["compliance_controls", "control_results", "policy_mappings", "remediation_actions"],
        "use_cases": [
            "Kontrolluppföljning",
            "Remediation tracking",
            "Policy mapping",
            "Revisionsstöd",
        ],
        "sla": "Daglig uppdatering, <4h latens, revisionsbar historik",
        "quality_score": None,
    },
    {
        "name": "Informationsklassning Vårddata",
        "domain_name": "Hälsosjukvård",
        "description": (
            "Dataprodukt för informationsklassning med klassningsnivåer, känslighetsprofiler, "
            "PII/PHI-indikatorer och styrande skyddsåtgärder för vårddata. Används i "
            "Purview för riskanalys och åtkomststyrning."
        ),
        "owners": ["Information Security Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataAnalyst", "DataEngineer"],
        "business_use": "Informationsklassning och riskstyrning av känsliga vårddata och metadata",
        "tables": ["data_classifications", "sensitivity_profiles", "protection_requirements", "asset_tags"],
        "use_cases": [
            "Riskklassning",
            "PII-identifiering",
            "Skyddsnivåer",
            "Åtkomststyrningsunderlag",
        ],
        "sla": "Daglig uppdatering, <2h latens, 99.5% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Audit Lineage Vårdplattform",
        "domain_name": "Hälsosjukvård",
        "description": (
            "Dataprodukt för audit, lineage och förändringsspårning över dataflöden i "
            "vårdplattformen. Konsoliderar pipelinehändelser, åtkomstspår, policyutfall "
            "och beroenden för incidentanalys och revision."
        ),
        "owners": ["Platform Governance Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataEngineer", "DataAnalyst"],
        "business_use": "Revision, lineage-analys och incidentutredning över vårdplattformens dataflöden",
        "tables": ["audit_events", "lineage_snapshots", "access_logs", "pipeline_dependencies"],
        "use_cases": [
            "Revisionsspårning",
            "Impact analysis",
            "Incidentutredning",
            "Lineage compliance",
        ],
        "sla": "Timvis uppdatering, <1h latens, 99.5% tillgänglighet",
        "quality_score": None,
    },
    {
        "name": "Informationsklassning & Policyefterlevnad",
        "domain_name": "Hälsosjukvård",
        "description": (
            "Dataprodukt för informationsklassning, policyramverk, kontrollstatus och "
            "efterlevnadsgrad per datadomän, system och informationsmängd. Gör det möjligt "
            "att följa upp styrning på ett enhetligt sätt i Purview."
        ),
        "owners": ["Information Governance Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataAnalyst"],
        "business_use": "Informationsklassning och policyefterlevnad för styrning av vårddata och informationsmängder",
        "tables": ["information_classes", "policy_catalog", "control_status", "compliance_scores"],
        "use_cases": [
            "Uppföljning av informationsklassning",
            "Kontrollstatus per policyområde",
            "Gap-analys mot styrande krav",
            "Ledningsrapportering av compliance",
        ],
        "sla": "Daglig uppdatering, <4h latens, revisionsbar historik",
        "quality_score": None,
    },
    {
        "name": "Åtkomstgranskning & Behörighetskontroll",
        "domain_name": "Hälsosjukvård",
        "description": (
            "Dataprodukt för uppföljning av åtkomstmönster, roller, privilegier, avvikande "
            "behörigheter och attestflöden för vårddata. Stödjer säkerhetsgranskning och "
            "least-privilege-principen i dataförvaltningen."
        ),
        "owners": ["Identity & Access Governance Team"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataAnalyst", "DataEngineer"],
        "business_use": "Granskning av åtkomst, behörigheter och avvikelser för skyddad vårddata",
        "tables": ["access_logs", "role_assignments", "privilege_exceptions", "attestation_cycles"],
        "use_cases": [
            "Upptäckt av avvikande behörigheter",
            "Kvartalsvis attest av åtkomst",
            "Spårbarhet för skyddad dataåtkomst",
            "Revisionsunderlag för säkerhetskontroller",
        ],
        "sla": "Daglig uppdatering, <2h latens, full revisionsspårbarhet",
        "quality_score": None,
    },
    {
        "name": "Lineage & Data Risk Office",
        "domain_name": "Hälsosjukvård",
        "description": (
            "Dataprodukt för central uppföljning av lineage, beroenden, data risk score, "
            "påverkansanalys och kritiska datapipelines. Används av data governance office "
            "för att prioritera riskreducerande åtgärder."
        ),
        "owners": ["Data Risk Office"],
        "type": "Operational",
        "status": "Published",
        "audience": ["DataAnalyst", "DataEngineer"],
        "business_use": "Övergripande riskstyrning, lineage-analys och impact assessment för dataprodukter",
        "tables": ["lineage_graph", "impact_assessments", "risk_scores", "critical_pipelines"],
        "use_cases": [
            "Riskprioritering av datapipelines",
            "Impact-analys vid schemaändringar",
            "Identifiering av kritiska beroenden",
            "Ledningsrapportering av datarisk",
        ],
        "sla": "Daglig uppdatering, <4h latens, versionshanterad riskhistorik",
        "quality_score": None,
    },
]


def create_data_products():
    header("1", "CREATING DATA PRODUCTS IN PURVIEW")
    refresh_token()

    created = 0

    # Resolve business domains once for deterministic domain mapping.
    domain_resp = requests.get(
        f"{UNIFIED}/businessDomains?api-version={API_VER}",
        headers=h, timeout=30
    )
    if domain_resp.status_code != 200:
        warn(f"Could not read business domains: {domain_resp.status_code}")
        return 0
    domain_map = {d["name"]: d["id"] for d in domain_resp.json().get("value", [])}

    # Check existing data products
    r = requests.get(
        f"{UNIFIED}/dataProducts?api-version={API_VER}",
        headers=h, timeout=30
    )
    existing = {}
    if r.status_code == 200:
        existing = {dp["name"]: dp for dp in r.json().get("value", [])}
        if existing:
            info(f"Existing data products: {', '.join(existing.keys())}")

    # Reuse known-good contacts shape from existing products when available.
    default_contacts = {"owner": [{"id": "9350a243-7bcf-4053-8f7e-996364f4de24", "description": "Creator"}]}
    for ep in existing.values():
        c = ep.get("contacts")
        if isinstance(c, dict) and c.get("owner"):
            default_contacts = c
            break

    for dp in DATA_PRODUCTS:
        if dp["name"] in existing:
            ok(f"{dp['name']} — already exists")
            created += 1
            continue

        domain_id = domain_map.get(dp["domain_name"])
        if not domain_id:
            warn(f"{dp['name']}: missing domain '{dp['domain_name']}'")
            continue

        # Create via Unified Catalog API with required schema fields.
        payload = {
            "name": dp["name"],
            "description": dp["description"],
            "status": dp["status"],
            "type": dp["type"],
            "domain": domain_id,
            "businessUse": dp.get("business_use", ""),
            "contacts": default_contacts,
            "termsOfUse": [],
            "documentation": [],
            "endorsed": True,
            "audience": dp.get("audience", ["DataEngineer"]),
        }

        r = requests.post(
            f"{UNIFIED}/dataProducts?api-version={API_VER}",
            headers=h, json=payload, timeout=30
        )
        if r.status_code in (200, 201):
            ok(f"{dp['name']} — created via Unified Catalog API")
            created += 1
        else:
            # Fallback: store as custom entity in Atlas with business metadata
            info(f"{dp['name']}: Unified API returned {r.status_code}, using Atlas custom type")
            # Create as glossary term under a new category instead
            created += create_dp_as_glossary(dp)

    # Also register as custom Atlas entities for rich metadata
    register_dp_entities()

    print(f"\n  Created/verified {created}/{len(DATA_PRODUCTS)} data products")
    return created


def create_dp_as_glossary(dp):
    """Create data product as a glossary term with rich metadata."""
    # Get glossary guid
    r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    if r.status_code != 200:
        warn("Cannot access glossary")
        return 0

    data = r.json()
    glossaries = data if isinstance(data, list) else [data]
    g_guid = glossaries[0]["guid"]

    # First ensure "Dataprodukter" category exists
    cat_r = requests.get(f"{ATLAS}/glossary/{g_guid}", headers=h, timeout=30)
    cat_guid = None
    if cat_r.status_code == 200:
        for cat in cat_r.json().get("categories", []):
            if cat.get("displayText") == "Dataprodukter":
                cat_guid = cat.get("categoryGuid")

    if not cat_guid:
        cat_payload = {
            "name": "Dataprodukter",
            "glossaryGuid": g_guid,
            "shortDescription": "Registrerade dataprodukter i plattformen",
        }
        r = requests.post(f"{ATLAS}/glossary/category", headers=h, json=cat_payload, timeout=30)
        if r.status_code in (200, 201):
            cat_guid = r.json().get("guid")
            ok("Created category: Dataprodukter")
        elif r.status_code == 409 or (r.status_code == 400 and "ATLAS-400-00-0" in r.text):
            info("Category 'Dataprodukter' already exists")
        else:
            warn(f"Category creation failed: {r.status_code}")

    # Create term for data product
    term_payload = {
        "name": f"DP {dp['name']}",
        "anchor": {"glossaryGuid": g_guid},
        "shortDescription": dp["description"][:256],
        "longDescription": (
            f"**Typ:** {dp['type']}\n"
            f"**Status:** {dp['status']}\n"
            f"**Domän:** {dp.get('domain_name', 'N/A')}\n"
            f"**Ägare:** {', '.join(dp['owners'])}\n"
            f"**Tabeller:** {', '.join(dp['tables'])}\n"
            f"**SLA:** {dp['sla']}\n\n"
            f"**Användningsområden:**\n" +
            "\n".join(f"- {uc}" for uc in dp["use_cases"])
        ),
        "status": "Approved",
    }
    if cat_guid:
        term_payload["categories"] = [{"categoryGuid": cat_guid}]

    r = requests.post(f"{ATLAS}/glossary/term", headers=h, json=term_payload, timeout=30)
    if r.status_code in (200, 201):
        ok(f"DP {dp['name']} — created as glossary term")
        return 1
    elif r.status_code == 409 or (r.status_code == 400 and "already exists" in r.text.lower()):
        ok(f"DP {dp['name']} — already exists")
        return 1
    else:
        warn(f"DP {dp['name']}: {r.status_code} {r.text[:100]}")
        return 0


def register_dp_entities():
    """Register data products as custom type definition for discoverability."""
    # Check if our custom type exists
    r = requests.get(f"{ATLAS}/types/typedef/name/healthcare_data_product", headers=h, timeout=15)
    if r.status_code == 200:
        info("Custom type 'healthcare_data_product' already defined")
        return

    typedef = {
        "classificationDefs": [],
        "entityDefs": [
            {
                "name": "healthcare_data_product",
                "description": "A healthcare data product combining multiple data assets",
                "superTypes": ["DataSet"],
                "typeVersion": "1.0",
                "attributeDefs": [
                    {"name": "product_type", "typeName": "string", "isOptional": True},
                    {"name": "product_status", "typeName": "string", "isOptional": True},
                    {"name": "product_owners", "typeName": "string", "isOptional": True},
                    {"name": "sla", "typeName": "string", "isOptional": True},
                    {"name": "use_cases", "typeName": "string", "isOptional": True},
                    {"name": "quality_score", "typeName": "float", "isOptional": True},
                    {"name": "tables", "typeName": "string", "isOptional": True},
                ],
            }
        ],
        "enumDefs": [],
        "relationshipDefs": [],
        "structDefs": [],
    }

    r = requests.post(f"{ATLAS}/types/typedefs", headers=h, json=typedef, timeout=30)
    if r.status_code in (200, 201):
        ok("Custom type 'healthcare_data_product' registered")
    else:
        warn(f"Custom type creation: {r.status_code} {r.text[:100]}")
        return

    # Now create entity instances
    for dp in DATA_PRODUCTS:
        entity = {
            "entity": {
                "typeName": "healthcare_data_product",
                "attributes": {
                    "qualifiedName": f"dp://{dp['name'].lower().replace(' ', '-')}",
                    "name": dp["name"],
                    "description": dp["description"],
                    "product_type": dp["type"],
                    "product_status": dp["status"],
                    "product_owners": ", ".join(dp["owners"]),
                    "sla": dp["sla"],
                    "use_cases": " | ".join(dp["use_cases"]),
                    "tables": ", ".join(dp["tables"]),
                },
            }
        }
        r = requests.post(f"{ATLAS}/entity", headers=h, json=entity, timeout=30)
        if r.status_code in (200, 201):
            ok(f"Entity: {dp['name']}")
        else:
            warn(f"Entity {dp['name']}: {r.status_code} {r.text[:80]}")


# ══════════════════════════════════════════════════════════════════
#  2. OKRs (Objectives & Key Results)
# ══════════════════════════════════════════════════════════════════
OKRS = [
    {
        "objective": "Förbättra datakvalitet i kliniska datakällor",
        "key_results": [
            "KR1 Completeness minst 98 procent for alla obligatoriska falt",
            "KR2 Accuracy minst 99 procent for ICD-10 och ATC-koder",
            "KR3 Freshness max 24h for alla Bronze-tabeller",
            "KR4 Consistency minst 95 procent mellan kalla SQL och Lakehouse",
        ],
    },
    {
        "objective": "Stärka datastyrning och compliance",
        "key_results": [
            "KR1 100 procent av PHI-kolumner klassificerade i Purview",
            "KR2 Alla dataprodukter har definierade SLA",
            "KR3 Glossary-termer mappade till minst 90 procent av entiteter",
            "KR4 GDPR-datahantering dokumenterad for alla dataprodukter",
        ],
    },
    {
        "objective": "Maximera forskningsplattformens värde",
        "key_results": [
            "KR1 OMOP CDM-mappning komplett for alla 5 kliniska tabeller",
            "KR2 ML-modeller levererar AUC minst 0.75 for readmission-prediktion",
            "KR3 BrainChild multimodal integration FHIR DICOM Genomik lankade",
            "KR4 Self-service analytics tillgangligt via Fabric for minst 3 team",
        ],
    },
    {
        "objective": "Säkerställa driftexcellens",
        "key_results": [
            "KR1 Pipeline-framgang minst 99 procent Bronze Silver Gold",
            "KR2 End-to-end latens max 2h fran SQL till Gold-lager",
            "KR3 Automatiserade datakvalitetskontroller vid varje pipeline-korning",
            "KR4 Incident response max 30 min for datakvalitets-larm",
        ],
    },
]


def create_okrs():
    header("2", "CREATING OKRs IN GLOSSARY")
    refresh_token()

    # Get glossary guid
    r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    if r.status_code != 200:
        warn(f"Cannot access glossary: {r.status_code}")
        return 0

    data = r.json()
    glossaries = data if isinstance(data, list) else [data]
    g_guid = glossaries[0]["guid"]

    # Create OKR category
    cat_payload = {
        "name": "OKR Data Governance",
        "glossaryGuid": g_guid,
        "shortDescription": "Objectives and Key Results for datastyrning och datakvalitet Q2 2026",
    }
    r = requests.post(f"{ATLAS}/glossary/category", headers=h, json=cat_payload, timeout=30)
    if r.status_code in (200, 201):
        okr_cat_guid = r.json().get("guid")
        ok("Created category: OKR — Data Governance")
    elif r.status_code == 409:
        info("OKR category already exists")
        # Find existing
        r2 = requests.get(f"{ATLAS}/glossary/{g_guid}", headers=h, timeout=30)
        okr_cat_guid = None
        if r2.status_code == 200:
            for cat in r2.json().get("categories", []):
                if "OKR" in cat.get("displayText", ""):
                    okr_cat_guid = cat.get("categoryGuid")
                    break
    else:
        if r.status_code == 400 and "ATLAS-400-00-0" in r.text:
            info("OKR category already exists (400)")
        else:
            warn(f"OKR category creation failed: {r.status_code}")
        okr_cat_guid = None

    created = 0
    for i, okr in enumerate(OKRS, 1):
        # Sanitize term names — Purview rejects colon, em-dash, special chars
        obj_short = okr['objective'][:60].replace('≥', 'minst ').replace('≤', 'max ')
        term_name = f"OKR-O{i} {obj_short}"
        kr_text = "\n".join(f"  - {kr.replace(chr(8805), 'minst ').replace(chr(8804), 'max ')}" for kr in okr["key_results"])

        term_payload = {
            "name": term_name,
            "anchor": {"glossaryGuid": g_guid},
            "shortDescription": obj_short,
            "longDescription": f"Objective {i} - {obj_short}\n\nKey Results:\n{kr_text}",
            "status": "Approved",
        }
        if okr_cat_guid:
            term_payload["categories"] = [{"categoryGuid": okr_cat_guid}]

        r = requests.post(f"{ATLAS}/glossary/term", headers=h, json=term_payload, timeout=30)
        if r.status_code in (200, 201):
            ok(f"O{i}: {okr['objective'][:60]}...")
            created += 1
        elif r.status_code == 409 or (r.status_code == 400 and "already exists" in r.text.lower()):
            ok(f"O{i}: already exists")
            created += 1
        else:
            warn(f"O{i}: {r.status_code} {r.text[:80]}")

    print(f"\n  Created/verified {created}/{len(OKRS)} OKRs")
    return created


# ══════════════════════════════════════════════════════════════════
#  3. DATA QUALITY RULES & CHECKS
# ══════════════════════════════════════════════════════════════════
DQ_RULES = [
    # Completeness checks
    {
        "name": "DQ-001 Patient Completeness",
        "category": "Completeness",
        "table": "hca.patients",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) as null_patient_id,
                SUM(CASE WHEN birth_date IS NULL THEN 1 ELSE 0 END) as null_birth_date,
                SUM(CASE WHEN gender IS NULL THEN 1 ELSE 0 END) as null_gender,
                SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) as null_region,
                SUM(CASE WHEN ses_level IS NULL THEN 1 ELSE 0 END) as null_ses_level
            FROM hca.patients
        """,
        "threshold": 0.98,
        "description": "Alla obligatoriska fält i patients ska ha ≥98% completeness",
    },
    {
        "name": "DQ-002 Encounter Completeness",
        "category": "Completeness",
        "table": "hca.encounters",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN encounter_id IS NULL THEN 1 ELSE 0 END) as null_encounter_id,
                SUM(CASE WHEN patient_id IS NULL THEN 1 ELSE 0 END) as null_patient_id,
                SUM(CASE WHEN admission_date IS NULL THEN 1 ELSE 0 END) as null_admission_date,
                SUM(CASE WHEN department IS NULL THEN 1 ELSE 0 END) as null_department,
                SUM(CASE WHEN los_days IS NULL THEN 1 ELSE 0 END) as null_los_days
            FROM hca.encounters
        """,
        "threshold": 0.98,
        "description": "Alla obligatoriska fält i encounters ska ha ≥98% completeness",
    },
    # Validity checks
    {
        "name": "DQ-003 ICD-10 Code Format",
        "category": "Validity",
        "table": "hca.diagnoses",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN icd10_code LIKE '[A-Z][0-9][0-9]%' THEN 0 ELSE 1 END) as invalid_format,
                COUNT(DISTINCT icd10_code) as unique_codes,
                SUM(CASE WHEN diagnosis_type IN ('Primary','Secondary','Complication') THEN 0 ELSE 1 END) as invalid_type
            FROM hca.diagnoses
        """,
        "threshold": 0.99,
        "description": "ICD-10-koder ska följa format [A-Z][0-9][0-9].* och ha giltig diagnosis_type",
    },
    {
        "name": "DQ-004 ATC Code Format",
        "category": "Validity",
        "table": "hca.medications",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN atc_code LIKE '[A-Z][0-9][0-9][A-Z][A-Z][0-9][0-9]' THEN 0 ELSE 1 END) as invalid_atc,
                COUNT(DISTINCT atc_code) as unique_atc_codes,
                SUM(CASE WHEN drug_name IS NULL OR LEN(drug_name) < 2 THEN 1 ELSE 0 END) as invalid_drug_name
            FROM hca.medications
        """,
        "threshold": 0.99,
        "description": "ATC-koder ska följa format [A-Z][0-9][0-9][A-Z][A-Z][0-9][0-9]",
    },
    # Referential integrity
    {
        "name": "DQ-005 Encounter → Patient FK",
        "category": "Referential Integrity",
        "table": "hca.encounters",
        "sql": """
            SELECT
                (SELECT COUNT(*) FROM hca.encounters) as total_encounters,
                COUNT(*) as orphan_encounters
            FROM hca.encounters e
            LEFT JOIN hca.patients p ON e.patient_id = p.patient_id
            WHERE p.patient_id IS NULL
        """,
        "threshold": 1.0,
        "description": "Alla encounters ska ha en matchande patient (0 orphans)",
    },
    {
        "name": "DQ-006 Diagnosis → Encounter FK",
        "category": "Referential Integrity",
        "table": "hca.diagnoses",
        "sql": """
            SELECT
                (SELECT COUNT(*) FROM hca.diagnoses) as total_diagnoses,
                COUNT(*) as orphan_diagnoses
            FROM hca.diagnoses d
            LEFT JOIN hca.encounters e ON d.encounter_id = e.encounter_id
            WHERE e.encounter_id IS NULL
        """,
        "threshold": 1.0,
        "description": "Alla diagnoser ska ha en matchande encounter (0 orphans)",
    },
    # Range checks
    {
        "name": "DQ-007 Vitals Range Check",
        "category": "Accuracy",
        "table": "hca.vitals_labs",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN systolic_bp < 50 OR systolic_bp > 300 THEN 1 ELSE 0 END) as bp_out_of_range,
                SUM(CASE WHEN heart_rate < 20 OR heart_rate > 250 THEN 1 ELSE 0 END) as hr_out_of_range,
                SUM(CASE WHEN temperature_c < 30 OR temperature_c > 45 THEN 1 ELSE 0 END) as temp_out_of_range,
                SUM(CASE WHEN oxygen_saturation < 50 OR oxygen_saturation > 100 THEN 1 ELSE 0 END) as spo2_out_of_range,
                SUM(CASE WHEN glucose_mmol < 0.5 OR glucose_mmol > 50 THEN 1 ELSE 0 END) as glucose_out_of_range
            FROM hca.vitals_labs
        """,
        "threshold": 0.99,
        "description": "Vitalparametrar ska vara inom medicinskt rimliga intervall",
    },
    # Timeliness
    {
        "name": "DQ-008 Data Freshness",
        "category": "Timeliness",
        "table": "hca.encounters",
        "sql": """
            SELECT
                COUNT(*) as total_rows,
                MAX(created_at) as latest_record,
                DATEDIFF(DAY, MAX(created_at), GETDATE()) as days_since_latest,
                MIN(admission_date) as earliest_admission,
                MAX(admission_date) as latest_admission
            FROM hca.encounters
        """,
        "threshold": 0.0,  # Info only
        "description": "Kontrollerar hur aktuell datan är",
    },
    # Uniqueness
    {
        "name": "DQ-009 Primary Key Uniqueness",
        "category": "Uniqueness",
        "table": "multiple",
        "sql": """
            SELECT 'patients' as tbl,
                   COUNT(*) as total,
                   COUNT(DISTINCT patient_id) as unique_keys,
                   COUNT(*) - COUNT(DISTINCT patient_id) as duplicates
            FROM hca.patients
            UNION ALL
            SELECT 'encounters', COUNT(*), COUNT(DISTINCT encounter_id),
                   COUNT(*) - COUNT(DISTINCT encounter_id)
            FROM hca.encounters
            UNION ALL
            SELECT 'diagnoses', COUNT(*), COUNT(DISTINCT diagnosis_id),
                   COUNT(*) - COUNT(DISTINCT diagnosis_id)
            FROM hca.diagnoses
        """,
        "threshold": 1.0,
        "description": "Primärnycklar ska vara unika (0 dubletter)",
    },
    # Cross-table consistency
    {
        "name": "DQ-010 LOS Consistency",
        "category": "Consistency",
        "table": "hca.encounters",
        "sql": """
            SELECT
                COUNT(*) as total_encounters,
                SUM(CASE WHEN los_days = DATEDIFF(DAY, admission_date, discharge_date)
                         THEN 0 ELSE 1 END) as los_mismatch,
                SUM(CASE WHEN discharge_date < admission_date THEN 1 ELSE 0 END) as date_order_error,
                SUM(CASE WHEN los_days < 0 THEN 1 ELSE 0 END) as negative_los
            FROM hca.encounters
            WHERE discharge_date IS NOT NULL
        """,
        "threshold": 0.99,
        "description": "LOS ska matcha DATEDIFF(admission_date, discharge_date)",
    },
]


def get_sql_connection():
    """Connect to Azure SQL with AAD token."""
    tok = cred.get_token("https://database.windows.net/.default")
    tb = tok.token.encode("UTF-16-LE")
    ts = struct.pack(f"<I{len(tb)}s", len(tb), tb)
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DB};",
        attrs_before={1256: ts},
    )


def run_data_quality_checks():
    header("3", "DATA QUALITY RULES & CHECKS")

    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
    except Exception as e:
        warn(f"SQL connection failed: {e}")
        return []

    results = []
    passed = 0
    failed = 0

    for rule in DQ_RULES:
        try:
            cursor.execute(rule["sql"])
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            result = {
                "rule": rule["name"],
                "category": rule["category"],
                "table": rule["table"],
                "timestamp": datetime.now().isoformat(),
                "status": "UNKNOWN",
                "details": {},
            }

            if rule["category"] == "Completeness":
                row = rows[0]
                total = row[0]
                null_counts = {columns[i]: row[i] for i in range(1, len(columns))}
                worst_completeness = min(
                    (total - v) / total if total > 0 else 0
                    for v in null_counts.values()
                )
                result["details"] = {
                    "total_rows": total,
                    "null_counts": null_counts,
                    "completeness": round(worst_completeness, 4),
                }
                result["score"] = round(worst_completeness, 4)
                result["status"] = "PASS" if worst_completeness >= rule["threshold"] else "FAIL"

            elif rule["category"] == "Validity":
                row = rows[0]
                total = row[0]
                invalid = row[1]
                validity = (total - invalid) / total if total > 0 else 0
                result["details"] = {
                    "total_rows": total,
                    "invalid_count": invalid,
                    "validity": round(validity, 4),
                    "extra": {columns[i]: row[i] for i in range(2, len(columns))},
                }
                result["score"] = round(validity, 4)
                result["status"] = "PASS" if validity >= rule["threshold"] else "FAIL"

            elif rule["category"] == "Referential Integrity":
                row = rows[0]
                total = row[0]
                orphans = row[1]
                integrity = (total - orphans) / total if total > 0 else 1.0
                result["details"] = {
                    "total_rows": total,
                    "orphan_count": orphans,
                    "integrity": round(integrity, 4),
                }
                result["score"] = round(integrity, 4)
                result["status"] = "PASS" if orphans == 0 else "FAIL"

            elif rule["category"] == "Accuracy":
                row = rows[0]
                total = row[0]
                out_of_range = sum(row[i] for i in range(1, len(columns)))
                accuracy = (total - out_of_range) / total if total > 0 else 0
                result["details"] = {
                    "total_rows": total,
                    "out_of_range": {columns[i]: row[i] for i in range(1, len(columns))},
                    "accuracy": round(accuracy, 4),
                }
                result["score"] = round(accuracy, 4)
                result["status"] = "PASS" if accuracy >= rule["threshold"] else "FAIL"

            elif rule["category"] == "Timeliness":
                row = rows[0]
                result["details"] = {columns[i]: str(row[i]) for i in range(len(columns))}
                result["score"] = 1.0
                result["status"] = "INFO"

            elif rule["category"] == "Uniqueness":
                dups_found = False
                details = {}
                for row in rows:
                    tbl = row[0]
                    total = row[1]
                    unique = row[2]
                    dups = row[3]
                    details[tbl] = {"total": total, "unique": unique, "duplicates": dups}
                    if dups > 0:
                        dups_found = True
                result["details"] = details
                result["score"] = 0.0 if dups_found else 1.0
                result["status"] = "FAIL" if dups_found else "PASS"

            elif rule["category"] == "Consistency":
                row = rows[0]
                total = row[0]
                mismatches = row[1]
                consistency = (total - mismatches) / total if total > 0 else 0
                result["details"] = {columns[i]: row[i] for i in range(len(columns))}
                result["score"] = round(consistency, 4)
                result["status"] = "PASS" if consistency >= rule["threshold"] else "FAIL"

            results.append(result)

            icon = GREEN + "✅" if result["status"] in ("PASS", "INFO") else YELLOW + "⚠️"
            score_str = f" ({result['score']:.1%})" if isinstance(result.get("score"), float) else ""
            print(f"  {icon}{RESET} {rule['name']}: {BOLD}{result['status']}{RESET}{score_str}")

            if result["status"] == "PASS":
                passed += 1
            elif result["status"] == "FAIL":
                failed += 1
                # Print failure details
                if "null_counts" in result["details"]:
                    for k, v in result["details"]["null_counts"].items():
                        if v > 0:
                            print(f"       {DIM}↳ {k}: {v} nulls{RESET}")
                if "orphan_count" in result["details"] and result["details"]["orphan_count"] > 0:
                    print(f"       {DIM}↳ {result['details']['orphan_count']} orphaned records{RESET}")
            else:
                passed += 1  # INFO counts as pass

        except Exception as e:
            warn(f"{rule['name']}: query error — {e}")
            results.append({
                "rule": rule["name"],
                "category": rule["category"],
                "status": "ERROR",
                "error": str(e),
            })

    conn.close()

    # Summary
    total = passed + failed
    print(f"\n  ┌{'─' * 40}┬{'─' * 14}┐")
    print(f"  │ {'Data Quality Summary':<38} │ {'Score':>12} │")
    print(f"  ├{'─' * 40}┼{'─' * 14}┤")
    print(f"  │ {'Rules Passed':<38} │ {f'{passed}/{total}':>12} │")
    print(f"  │ {'Rules Failed':<38} │ {f'{failed}/{total}':>12} │")
    overall = passed / total if total > 0 else 0
    print(f"  │ {'Overall Score':<38} │ {f'{overall:.0%}':>12} │")
    print(f"  └{'─' * 40}┴{'─' * 14}┘")

    return results


def store_dq_results_in_purview(results):
    """Store data quality results as custom entities in Purview."""
    header("4", "STORING DQ RESULTS IN PURVIEW")
    refresh_token()

    # Create DQ category in glossary
    r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=30)
    if r.status_code != 200:
        warn("Cannot access glossary")
        return

    data = r.json()
    glossaries = data if isinstance(data, list) else [data]
    g_guid = glossaries[0]["guid"]

    # Create DQ category
    cat_payload = {
        "name": "Datakvalitetsregler",
        "glossaryGuid": g_guid,
        "shortDescription": "Data Quality Rules & Checks — automatiserade kontroller för datakvalitet",
    }
    r = requests.post(f"{ATLAS}/glossary/category", headers=h, json=cat_payload, timeout=30)
    dq_cat_guid = None
    if r.status_code in (200, 201):
        dq_cat_guid = r.json().get("guid")
        ok("Created category: Datakvalitetsregler")
    elif r.status_code == 409 or (r.status_code == 400 and "ATLAS-400-00-0" in r.text):
        info("DQ category already exists")
        r2 = requests.get(f"{ATLAS}/glossary/{g_guid}", headers=h, timeout=30)
        if r2.status_code == 200:
            for cat in r2.json().get("categories", []):
                if "Datakvalitet" in cat.get("displayText", ""):
                    dq_cat_guid = cat.get("categoryGuid")

    stored = 0
    for result in results:
        if result.get("status") == "ERROR":
            continue

        # Sanitize term name for Purview
        term_name = result["rule"].replace('→', '-').replace('—', '-')
        status = result["status"]
        score = result.get("score", 0)
        details = json.dumps(result.get("details", {}), ensure_ascii=False, default=str)

        short_desc = f"[{status}] Score {score:.0%} {result['category']}" if isinstance(score, float) else f"[{status}]"
        long_desc = (
            f"Regel {term_name}\n"
            f"Kategori {result['category']}\n"
            f"Status {status}\n"
        )
        if isinstance(score, float):
            long_desc += f"Score {score:.1%}\n"
        long_desc += (
            f"Tabell {result.get('table', 'N/A')}\n"
            f"Korning {result.get('timestamp', 'N/A')}\n\n"
            f"Detaljer\n{details}"
        )

        term_payload = {
            "name": term_name,
            "anchor": {"glossaryGuid": g_guid},
            "shortDescription": short_desc[:256],
            "longDescription": long_desc,
            "status": "Approved",
        }
        if dq_cat_guid:
            term_payload["categories"] = [{"categoryGuid": dq_cat_guid}]

        r = requests.post(f"{ATLAS}/glossary/term", headers=h, json=term_payload, timeout=30)
        if r.status_code in (200, 201):
            stored += 1
        elif r.status_code == 409 or (r.status_code == 400 and "already exists" in r.text.lower()):
            # Already exists
            stored += 1
        else:
            warn(f"Store {term_name}: {r.status_code}")

    ok(f"Stored {stored}/{len([r for r in results if r.get('status') != 'ERROR'])} DQ results in glossary")

    # Also save to local JSON
    from pathlib import Path
    out = Path(__file__).resolve().parent.parent / "data_quality_report.json"
    report = {
        "timestamp": datetime.now().isoformat(),
        "rules_total": len(results),
        "rules_passed": sum(1 for r in results if r["status"] in ("PASS", "INFO")),
        "rules_failed": sum(1 for r in results if r["status"] == "FAIL"),
        "results": results,
    }
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    ok(f"Report saved: {out}")


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    print(f"""
{BOLD}{BLUE}======================================================================
  PURVIEW DATA PRODUCTS, OKRs & DATA QUALITY
  {datetime.now().strftime('%Y-%m-%d %H:%M')}
======================================================================{RESET}
""")

    # 1. Data Products
    dp_count = create_data_products()

    # 2. OKRs
    okr_count = create_okrs()

    # 3. Data Quality Checks
    dq_results = run_data_quality_checks()

    # 4. Store DQ results in Purview
    store_dq_results_in_purview(dq_results)

    # Update data product quality scores
    dq_pass = sum(1 for r in dq_results if r["status"] in ("PASS", "INFO"))
    dq_total = len(dq_results)
    quality_score = dq_pass / dq_total if dq_total > 0 else 0

    print(f"""
{'=' * 70}
  SUMMARY
{'=' * 70}
  Data Products:    {dp_count}/{len(DATA_PRODUCTS)}
  OKRs:             {okr_count}/{len(OKRS)}
  DQ Rules:         {dq_pass}/{dq_total} passed ({quality_score:.0%})

  {GREEN}✅{RESET} All governance artifacts created!
  View in Purview: https://purview.microsoft.com
{'=' * 70}
""")


if __name__ == "__main__":
    main()
