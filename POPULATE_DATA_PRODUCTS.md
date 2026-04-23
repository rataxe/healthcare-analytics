# 📦 GUIDE: Populera Data Products i Purview

**Datum:** 2026-04-22  
**Purview Account:** `prviewacc`  
**Status:** 4 Data Products skapade men tomma — behöver assets, metadata och links

---

## 🎯 ÖVERSIKT

Du har **4 Data Products** registrerade i Purview som är tomma:

1. **BrainChild Barncancerforskning** — Genomik och tumördata
2. **ML Feature Store** — Machine learning features
3. **OMOP Forskningsdata** — Standardiserad forskningsdata
4. **Klinisk Patientanalys** — Patientdata och vårdprocesser

**Varje Data Product ska innehålla:**
- ✅ **Assets** (datasets, tables, lakehouse items)
- ✅ **Glossary Terms** (business termer som beskriver produkten)
- ✅ **Metadata** (beskrivning, owner, tags, use cases)
- ✅ **Lineage** (hur data flödar genom produkten)
- ✅ **Documentation** (README, schema definitions)

**Tidskostnad:** ~1-2 timmar totalt (15-30 min per data product)

---

## 🚀 STEG-FÖR-STEG: Populera Data Products

### Steg 1: Öppna Purview Portal

1. Gå till: https://purview.microsoft.com
2. Logga in med: `joandolf@microsoft.com`
3. Navigera till: **Data Catalog** → **Data Products**

**Alternativ väg:**
```
https://web.purview.azure.com/resource/prviewacc
→ Data Catalog (vänster meny)
→ Data Products
```

Du bör se 4 data products i listan.

---

## 📦 DATA PRODUCT 1: BrainChild Barncancerforskning

### Steg 1: Öppna Data Product

1. Klicka på **"BrainChild Barncancerforskning"**
2. Klicka **"Edit"** (penna-ikon uppe till höger)

### Steg 2: Fyll i Grundinformation

| Fält | Värde |
|------|-------|
| **Display Name** | `BrainChild Barncancerforskning` |
| **Description** | `Omfattar genomisk data från barncancerpatienter med DNA-sekvensering, VCF-filer, tumörbiopsier, kliniska samband mellan genetiska varianter och behandlingssvar. Används för precisionsmedicin och forskningsprojekt inom pediatrisk onkologi.` |
| **Owner** | `joandolf@microsoft.com` |
| **Domain** | `Genomik & Forskning` (välj från dropdown efter du skapat domains) |
| **Status** | `Production` eller `Active` |

### Steg 3: Lägg till Tags

```
Tags att lägga till:
- genomics
- cancer-research
- VCF
- pediatric-oncology
- precision-medicine
- NGS
- DNA-sequencing
- BrainChild
```

Klicka **"+ Add tag"** för varje tag

### Steg 4: Lägg till Assets (Data Sources)

**Klicka "Add assets" eller "Link data sources"**

#### **Fabric Lakehouse Assets:**
```
Om du har Fabric lakehouses scanned, lägg till:
- Bronze/genomics/*.vcf
- Silver/genomics/variants_enriched
- Gold/genomics/clinical_variants
- Gold/genomics/patient_genomics
```

**Så här lägger du till:**
1. Klicka **"+ Add asset"**
2. Sök efter lakehouse: `Gold` eller `Silver`
3. Navigera till `Tables/genomics/` folder
4. Markera relevanta tables/files
5. Klicka **"Add"**

#### **SQL Tables (om tillgängliga):**
```
Om SQL-scanning är aktivt:
- dbo.specimens
- dbo.genomic_variants
- dbo.tumor_samples
```

#### **FHIR Resources:**
```
Om FHIR är scannat:
- Specimen (tumörprover)
- DiagnosticReport (genomiska rapporter)
- Observation (varianter)
```

**Om assets inte syns ännu:**
> Detta är OK för POC — assets kan läggas till senare när Fabric/SQL/FHIR-scanning är konfigurerat.  
> Dokumentera istället vilka assets som **ska** ingå i Description-fältet.

### Steg 5: Länka Glossary Terms

**Klicka "Related terms" eller "Link glossary terms"**

**Terms att länka (från "Sjukvårdstermer"):**
```
Genomik & Barncancerforskning:
- VCF
- Genomic Variant
- DNA Sequence
- Tumor Sample
- Specimen
- Biobank
- NGS (Next-Generation Sequencing)
- BrainChild
- Copy Number Variation (CNV)
- Structural Variant
- Mutation
- Germline Variant
- Somatic Variant
```

**Så här lägger du till:**
1. Klicka **"+ Add term"**
2. Sök i glossary: "VCF"
3. Markera termen
4. Klicka **"Add"**
5. Upprepa för alla termer

### Steg 6: Use Cases & Documentation

**Fält: "Use Cases"**
```
1. Precisionsmedicin - Identifiera behandlingsbara mutationer
2. Forskningsstudier - Korrelation genetik ↔ behandlingsrespons
3. Biobank-analys - Tumörprover och DNA-arkiv
4. Variant-tolkning - Klinisk signifikans av genetiska varianter
5. Longitudinell uppföljning - Genetiska förändringar över tid
```

**Fält: "Documentation"**
```markdown
## Dataformat
- VCF (Variant Call Format) - genomiska varianter
- JSON (FHIR Specimen, DiagnosticReport)
- CSV (metadata, kliniska samband)

## Schema
- Specimen: ID, patient_id, sample_type, collection_date, tumor_type
- Variants: CHROM, POS, REF, ALT, QUAL, FILTER, INFO
- Clinical: patient_id, variant_id, treatment_response, survival_months

## Data Lineage
Bronze (raw VCF) → Silver (annotated variants) → Gold (clinical associations)

## Quality Metrics
- Sequencing depth: >30x coverage
- Variant quality score: QUAL > 30
- Population frequency: < 1% in gnomAD
```

### Steg 7: Spara

Klicka **"Save"** → Data Product är nu populerad! ✅

---

## 📦 DATA PRODUCT 2: ML Feature Store

### Grundinformation

| Fält | Värde |
|------|-------|
| **Display Name** | `ML Feature Store` |
| **Description** | `Centraliserad feature store för machine learning med pre-computed features från patient-data, lab-resultat, vitala mätvärden och longitudinella trender. Används för riskprediktion, readmission-modeller och AI-drivna beslutsstöd.` |
| **Owner** | `joandolf@microsoft.com` |
| **Domain** | `ML & Prediktioner` |
| **Tags** | `machine-learning`, `feature-engineering`, `MLflow`, `predictions`, `feature-store` |

### Assets att länka

```
Fabric Lakehouse:
- Gold/features/patient_features
- Gold/features/lab_aggregates
- Gold/features/vital_trends
- Gold/features/medication_patterns

SQL Tables:
- dbo.ml_features
- dbo.feature_metadata
- dbo.feature_lineage
```

### Glossary Terms att länka

```
ML & Analytics:
- Feature Store
- ML Feature
- Feature Engineering
- ML Model
- MLflow Model
- Model Registry
- Batch Scoring
- Prediction
- Risk Score
- Feature Drift
- Model Monitoring
```

### Use Cases

```
1. Riskprediktion - 30-dagars readmission risk
2. Sepsis early warning - Real-time riskscore
3. Medication adherence prediction
4. Treatment response modeling
5. Patient outcome forecasting
```

### Documentation

```markdown
## Feature Categories
1. Demographic features (age, gender, comorbidities)
2. Vitals aggregates (mean/max/min HR, BP, SpO2)
3. Lab trends (sequential lab values, abnormal flags)
4. Medication patterns (adherence, drug interactions)
5. Historical outcomes (prior admissions, procedures)

## Schema
- feature_id, feature_name, patient_id, timestamp
- feature_type: categorical | continuous | binary
- update_frequency: real-time | daily | on-demand

## Data Lineage
Patient → Bronze → Silver (transformations) → Gold (features) → MLflow (training)

## SLA
- Freshness: < 4 hours
- Availability: 99.5%
- Feature drift monitoring: daily
```

---

## 📦 DATA PRODUCT 3: OMOP Forskningsdata

### Grundinformation

| Fält | Värde |
|------|-------|
| **Display Name** | `OMOP Forskningsdata` |
| **Description** | `Standardiserad forskningsdata enligt OMOP CDM (Observational Medical Outcomes Partnership Common Data Model) för epidemiologiska studier, cohort-analyser och cross-institutional research. De-identifierad data för forskningsändamål.` |
| **Owner** | `joandolf@microsoft.com` |
| **Domain** | `Genomik & Forskning` (eller `Interoperabilitet` om du vill ha båda) |
| **Tags** | `OMOP`, `research`, `CDM`, `de-identified`, `observational-studies` |

### Assets att länka

```
CSV Files (från c:\code\brainchild-fhir-demo\brainchild_synthetic_data\omop\):
- person.csv
- condition_occurrence.csv
- drug_exposure.csv
- measurement.csv
- visit_occurrence.csv
- specimen.csv
- genomics/*.csv

Fabric Lakehouse:
- Bronze/omop/person
- Bronze/omop/condition_occurrence
- Silver/omop/person_enriched
- Gold/omop/research_cohorts
```

### Glossary Terms att länka

```
OMOP & Research:
- OMOP Concept
- OMOP CDM
- Concept ID
- Condition Occurrence
- Drug Exposure
- Measurement
- Visit Occurrence
- Cohort
- De-identification
- Research Dataset
```

### Use Cases

```
1. Epidemiologiska studier - Cancer incidence rates
2. Cohort discovery - Identify patient populations
3. Comparative effectiveness research
4. Drug safety surveillance
5. Cross-institutional collaboration
```

### Documentation

```markdown
## OMOP Tables
- PERSON (demographics, de-identified)
- CONDITION_OCCURRENCE (diagnoses with SNOMED CT)
- DRUG_EXPOSURE (medications with RxNorm)
- MEASUREMENT (labs with LOINC)
- VISIT_OCCURRENCE (encounters)
- SPECIMEN (biobank samples)

## Vocabulary Standards
- SNOMED CT: Clinical findings & procedures
- RxNorm: Medications
- LOINC: Laboratory tests
- ICD-10: Diagnoses

## Data Lineage
FHIR R4 → OMOP CDM Mapping → CSV Export → Lakehouse

## De-identification
- PII removed (name, SSN, address)
- Dates shifted by random offset
- Rare conditions suppressed (<5 patients)
- K-anonymity level: k=5
```

---

## 📦 DATA PRODUCT 4: Klinisk Patientanalys

### Grundinformation

| Fält | Värde |
|------|-------|
| **Display Name** | `Klinisk Patientanalys` |
| **Description** | `Identifierbar patientdata för klinisk vård, omfattar EHR-data, vårdtillfällen, diagnoser, läkemedel, vitala mätvärden, lab-resultat och avbildningar (DICOM). Används för operational analytics, quality metrics och clinical dashboards.` |
| **Owner** | `joandolf@microsoft.com` |
| **Domain** | `Klinisk Vård` |
| **Tags** | `clinical`, `EHR`, `patient-data`, `operations`, `quality-metrics` |

### Assets att länka

```
SQL Tables (sql-hca-demo):
- dbo.patients
- dbo.conditions
- dbo.medications
- dbo.observations
- dbo.encounters
- dbo.procedures
- dbo.immunizations
- dbo.vital_signs
- dbo.lab_results
- dbo.radiology_orders
- dbo.discharge_summaries

Fabric Lakehouse:
- Bronze/fhir/Patient
- Silver/fhir/Patient_transformed
- Gold/clinical/patient_360
- Gold/clinical/care_quality_metrics
```

### Glossary Terms att länka

```
Klinisk Vård:
- Patient
- Personnummer (Swedish Personnummer)
- Vårdtillfälle (Encounter)
- Condition
- Diagnos
- ICD-10 Code
- Medication
- Läkemedel
- ATC Code
- Observation
- Vital Signs (Vitala mätvärden)
- Lab Result
- LOINC Code
- Practitioner (Sjukvårdspersonal)
- Avdelning (Department)
- Radiology Order
- DICOM Study
```

### Use Cases

```
1. Patient 360 Dashboard - Helhetsbild av patientens vård
2. Quality metrics - Readmission rates, LOS, mortality
3. Clinical decision support - Drug interactions, allergies
4. Bed management - Census, utilization, capacity planning
5. Care coordination - Handoffs, referrals, follow-ups
```

### Documentation

```markdown
## Data Sources
- EHR System: Epic/Cerner (FHIR R4 export)
- Lab Interface: HL7 v2.x
- Radiology: DICOM PACS
- Vitals: Bedside monitors (HL7 FHIR)

## Schema
- Patient: id, personnummer, name, DOB, gender, address
- Encounter: id, patient_id, admit_date, discharge_date, department
- Condition: id, patient_id, encounter_id, ICD10_code, onset_date
- Medication: id, patient_id, ATC_code, dose, frequency, route

## Data Lineage
EHR (FHIR) → Bronze → Silver (transformed) → Gold (aggregated) → BI Dashboards

## Security & Compliance
- PII Data: YES (identified)
- GDPR Compliant: YES
- Access Control: RBAC + Row-level security
- Audit Logging: All queries logged
- Retention: 10 years (legal requirement)
```

---

## ✅ VERIFIERA ATT DATA PRODUCTS ÄR IFYLLDA

### Checklista per Data Product:

- [ ] **Description**: Fylld i med tydlig beskrivning
- [ ] **Owner**: Tilldelad (joandolf@microsoft.com)
- [ ] **Domain**: Länkad till governance domain
- [ ] **Tags**: 3-5 relevanta tags
- [ ] **Assets**: Minst 3-5 assets länkade (eller dokumenterat vilka som ska länkas)
- [ ] **Glossary Terms**: 8-15 termer länkade
- [ ] **Use Cases**: 3-5 use cases beskrivna
- [ ] **Documentation**: Schema, lineage, compliance notes

### Verifiera i Purview Portal:

1. Gå till: **Data Catalog** → **Data Products**
2. Klicka på varje data product
3. Kontrollera att alla fält är ifyllda
4. Kolla att "Assets" tab visar länkade datasets
5. Kolla att "Terms" tab visar länkade glossary terms

---

## 🔗 AVANCERAT: Länka Lineage till Data Products

### Steg 1: Öppna Lineage View

```
Data Catalog → Search → (sök efter ett asset, t.ex. "patients")
→ Klicka på asset → Lineage tab
```

### Steg 2: Tagga Lineage med Data Product

1. Klicka på en pipeline/process i lineage-grafen
2. Klicka **"Edit"**
3. Under **"Related entities"** → **"Data Products"**
4. Välj relevant data product (t.ex. "Klinisk Patientanalys")
5. **Save**

**Exempel lineage mappings:**
```
FHIR Patient → Bronze → Silver → Gold patient_360
└─ Länka hela kedjan till "Klinisk Patientanalys"

VCF Files → Bronze → Silver variants → Gold clinical_variants
└─ Länka hela kedjan till "BrainChild Barncancerforskning"

OMOP CDM Mapping → OMOP Tables → Research Cohorts
└─ Länka till "OMOP Forskningsdata"

Gold Features → MLflow Training → Model Registry
└─ Länka till "ML Feature Store"
```

---

## 🚨 FELSÖKNING

### Problem: "Kan inte hitta assets att länka"

**Lösning:**
- Assets måste först scannas i Purview via **Data Map** → **Sources**
- Om Fabric lakehouses inte är scanned än: Dokumentera i Description vilka assets som **ska** länkas
- För POC: OK att ha data products utan assets länkade än

### Problem: "Glossary terms visas inte i dropdown"

**Lösning:**
1. Verifiera att termer finns: **Data Catalog** → **Business Glossary**
2. Kontrollera att du har Data Curator-behörighet
3. Refresha sidan (Ctrl+F5)
4. Sök efter term-namn manuellt istället för att scrolla

### Problem: "Kan inte välja Domain"

**Lösning:**
- Governance Domains måste skapas först (se MANUAL_GOVERNANCE_DOMAINS.md)
- Om domains inte finns än: Lämna detta fält tomt, fylla i senare

---

## 📊 SLUTRESULTAT

Efter att ha följt denna guide:

| Data Product | Status | Assets | Terms | Documentation |
|-------------|--------|--------|-------|---------------|
| BrainChild Barncancerforskning | ✅ IFYLLD | 5-10 | 12-15 | ✅ Komplett |
| ML Feature Store | ✅ IFYLLD | 4-8 | 10-12 | ✅ Komplett |
| OMOP Forskningsdata | ✅ IFYLLD | 6-10 | 8-10 | ✅ Komplett |
| Klinisk Patientanalys | ✅ IFYLLD | 10-15 | 15-20 | ✅ Komplett |

**Total tid:** ~1-2 timmar

**Resultat:** Fullständigt dokumenterade Data Products med tydliga beskrivningar, ägarskap, use cases och länkar till data assets! 🎉

---

## 💡 BEST PRACTICES

### 1. Beskrivningar ska vara:
- **Tydliga**: Vad innehåller produkten?
- **Actionable**: Hur används den?
- **Uppdaterade**: Review var 6:e månad

### 2. Assets att prioritera:
- **Ofta använda datasets**: SQL tables, lakehouse tables
- **Kritiska pipelines**: ETL-processer i lineage
- **Approved sources**: Endast godkänd data

### 3. Glossary Terms att länka:
- **Core business terms**: Centrala begrepp (Patient, Diagnos)
- **Technical terms**: Standards (ICD-10, FHIR)
- **Domain-specific**: Unika termer (VCF, OMOP Concept)

### 4. Documentation ska innehålla:
- **Schema**: Kolumner, datatyper, constraints
- **Lineage**: Hur data flödar
- **Quality**: SLA, freshness, completeness
- **Security**: Access controls, PII, compliance

---

## 📞 SUPPORT & NÄSTA STEG

**Microsoft Purview Data Products:**
- Documentation: https://learn.microsoft.com/purview/concept-data-products
- Tutorial: https://learn.microsoft.com/purview/how-to-create-data-products

**Nästa steg:**
1. ✅ Populera alla 4 data products (denna guide)
2. ✅ Skapa Governance Domains (MANUAL_GOVERNANCE_DOMAINS.md)
3. ✅ Länka domains till data products
4. 🔄 Konfigurera Fabric Lakehouse scanning (för att få assets synliga)
5. 🔄 Sätt upp Data Quality Rules per data product
6. 🔄 Aktivera Usage Analytics för att se vilka data products som används mest

---

**Skapat:** 2026-04-22  
**Av:** Automated Purview Configuration  
**För:** Healthcare Analytics POC — prviewacc
