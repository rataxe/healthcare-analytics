# Purview Governance - Strukturerad Felsökning & Åtgärdsplan

> Genererad baserat på diagnostik från `_diag_complete.py` och `_diag_deepdive.py`  
> Konto: `prviewacc` | Datum: 2026-04-22

---

## DEL 1: FELSÖKNINGSANALYS

### Sammanfattning av nuläge

| Område | Status | Detalj |
|--------|--------|--------|
| Samlingar (7) | ✅ OK | Rätt hierarki, alla skapade |
| Datakällor (2) | ✅ OK | SQL + Fabric registrerade |
| SQL-skanningar (3) | ✅ OK | Alla Succeeded |
| Fabric-skanning | ⚠️ Varning | CompletedWithExceptions |
| Glossary (145 termer) | ✅ OK | 5 kategorier, alla tilldelade |
| Lineage (34 Process) | ✅ OK | I halsosjukvard-samling |
| Custom entities (16) | ✅ OK | FHIR/DICOM/Data Products |
| Governance Domains (4) | ✅ OK | Alla Published |
| Klassificeringar | ❌ 2 saknas | SNOMED CT Code, OMOP Concept ID |
| Term-entity-kopplingar | ⚠️ 99/145 | 46 kopplade, 99 saknar entitet |
| Domain-term-koppling | ❌ Ej möjligt | Inget fungerande API |
| MIP Sensitivity Labels | ❌ Manuellt | AADSTS65002-fel |
| barncancer-samling | ⚠️ Tom | 0 entiteter (barn-samlingen har 29) |

---

### Problem 1: SNOMED CT Code — 0 entiteter klassificerade

**Symptom:** Klassificeringen `SNOMED CT Code` skapades men har inte applicerats.

**Rotorsaksanalys:**
- SQL-schemat (`hca`) innehåller **inga SNOMED-kolumner**
- Diagnoses-tabellen har `icd10_code` och `icd10_description` (ICD-10, ej SNOMED)
- OMOP-tabeller med `concept_id` finns **enbart i Fabric Lakehouse**, ej i Azure SQL
- Fabric Lakehouse-tabeller saknar column-level entities (bara tabell-nivå)

**Relevanta entiteter för klassificering:**
- `diagnoses` (2 instanser) — innehåller diagnosdata som kan mappas till SNOMED
- Fabric-tabeller: `condition_occurrence`, `measurement`, `specimen` — OMOP-tabeller med concept_id som ofta baseras på SNOMED

**Allvarlighetsgrad:** Medel — Klassificeringen bör appliceras på tabellnivå

---

### Problem 2: OMOP Concept ID — 0 entiteter klassificerade

**Symptom:** Klassificeringen `OMOP Concept ID` skapades men har inte applicerats.

**Rotorsaksanalys:**
- Azure SQL-tabellerna tillhör det kliniska schemat (`hca`) — inga `*_concept_id`-kolumner
- OMOP-data (condition_occurrence, drug_exposure, measurement, person, specimen, visit_occurrence) lagras i **Fabric Lakehouse** som `fabric_lakehouse_table`
- Totalt 42 fabric_lakehouse_table-entiteter finns i Purview
- Inget sök efter OMOP-relaterade Fabric-tabeller har gjorts tidigare

**Relevanta entiteter:**
- Fabric Lakehouse OMOP-tabeller: condition_occurrence, drug_exposure, measurement, person, specimen, visit_occurrence

**Allvarlighetsgrad:** Medel — Klassificeringen bör appliceras på tabellnivå

---

### Problem 3: Barncancer-samling tom (0 entiteter)

**Symptom:** Samlingen `barncancer` (Barncancerforskning) är tom trots att den har barn-samlingen `fabric-brainchild` med 29 entiteter.

**Rotorsaksanalys:**
- `barncancer` är **gruppsamling** (parent) — alla BrainChild-entiteter ligger i `fabric-brainchild` (child)
- Data product `BrainChild Barncancerforskning` ligger i `halsosjukvard` (fel samling)
- `fabric-brainchild` innehåller: 16 lakehouse-tabeller, 8 FHIR resource types, 2 DICOM modalities, 1 DICOM service, 1 FHIR service, 1 fabric_lake_warehouse
- Purview räknar **inte** barn-samlingars entiteter i föräldern

**Entiteter att flytta till barncancer:**
| Entitet | Nuvarande samling | Typ |
|---------|-------------------|-----|
| BrainChild Barncancerforskning | halsosjukvard | healthcare_data_product |
| BrainChild FHIR Server (R4) | fabric-brainchild | healthcare_fhir_service |
| BrainChild DICOM Server | fabric-brainchild | healthcare_dicom_service |

**Allvarlighetsgrad:** Låg — Kosmetiskt, fabric-brainchild har alla detaljentiteter

---

### Problem 4: Fabric-skanning CompletedWithExceptions

**Symptom:** Skanning `Scan-IzR` slutförde med status `CompletedWithExceptions`.

**Rotorsaksanalys:**
- Exception count map: **TOM** — inga specifika exceptions loggade
- Notifications: **0** — inga varningsnotifikationer
- Skanningen **upptäckte 567+ entiteter** framgångsrikt
- Typfördelning: fabric_lakehouse_path (331), fabric_lakehouse_table (42), fabric_lake_warehouse (19), powerbi_dataset (15), powerbi_workspace (14), powerbi_report (13)

**Möjliga orsaker:**
1. Vissa workspaces/items hade behörighetsproblem (läsrättighetsbrist)
2. Timeout på enskilda items
3. Metadata-format som skannern inte kunde tolka

**Allvarlighetsgrad:** Låg — 567 entiteter lyckades, tom fellog tyder på smärre problem

---

### Problem 5: 99/145 glossary-termer ej länkade till entiteter

**Symptom:** 46 av 145 termer har `assignedEntities`, 99 saknar.

**Fördelning av olänkade termer per kategori:**
| Kategori | Olänkade | Exempel |
|----------|----------|---------|
| Barncancerforskning | 45 | ALL, AML, Barncancerfonden, Behandlingsprotokoll, Biobank |
| Klinisk Data | 25 | ALAT/ASAT, Blodstatus, CRP, Akutmottagning |
| Dataarkitektur | 11 | Apache Spark, Data Lakehouse, Data Lineage, Data Mesh |
| Kliniska Standarder | 10 | ACMG, ATC, Biobankslagen, DRG |
| Interoperabilitet | 8 | CDA, HL7 v2, IHE-profiler, Inera |

**Rotorsaksanalys:**
- Många termer är **konceptuella** (sjukdomsnamn, lagar, standarder) utan direkt dataentitet
- Barncancerforskning (45 olänkade) — cancer-specifika termer som inte har motsvarande SQL-tabeller
- Klinisk Data (25) — labvärden/kliniska begrepp som finns implicit i vitals_labs men inte direkt som entiteter
- Arkitektur/Standarder — tekniska begrepp utan direkt datamapping

**Realistisk bedömning:** Max ~30-40 ytterligare termer kan länkas till befintliga entiteter. Resterande ~60 är konceptuella.

**Allvarlighetsgrad:** Medel — Normalt att inte alla glossary-termer har en direkt entitetskoppling

---

### Problem 6: Governance Domains ej kopplade till termer

**Symptom:** Alla 145 termer har `domainId: "prviewacc"` — rotsamlingens namn, inte governance domain ID.

**Rotorsaksanalys:**
- `domainId` är ett **read-only fält** som speglar glossarens parent-collection
- Testat: PATCH domain=405, POST sub-resources=404, PUT term med businessDomainId=200 men persisterade inte
- Governance Domains API (`datagovernance/catalog/businessDomains`) stödjer **listing** men inte term-linking
- Microsoft dokumenterar att domain-term-koppling görs **enbart via portalen**

**Allvarlighetsgrad:** Medel — Kräver manuellt arbete i portalen

---

### Problem 7: MIP Sensitivity Labels ej aktiverade

**Symptom:** API-anrop ger AADSTS65002 — consent saknas.

**Rotorsaksanalys:**
- Microsoft Information Protection (MIP) kräver admin consent i Azure AD
- Automatisk aktivering via API **fungerar inte** — kräver global admin-godkännande

**Allvarlighetsgrad:** Medel — Manuell admin-åtgärd i Microsoft Purview portal

---

### Problem 8: SQL Medications — 20 000/60 563 rader

**Symptom:** Bara ~33% av medications-data är uppladdad till Azure SQL.

**Rotorsaksanalys:** Uppladdningsskriptet avbröts eller körde timeout.

**Allvarlighetsgrad:** Medel — Data saknas i SQL-databasen

---

### Problem 9: Key Vault saknar fhir-service-url

**Symptom:** Key Vault-hemligheten `fhir-service-url` existerar inte.

**Rotorsaksanalys:** Aldrig skapad vid initial setup.

**Allvarlighetsgrad:** Låg — Kräver en `az keyvault secret set`

---

## DEL 2: STEGVIS ÅTGÄRDSPLAN

### Fas 1: Automatiserade API-fixar (kan köras med script)

#### Åtgärd 1.1: Applicera SNOMED CT Code på relevanta entiteter
```
Mål: Applicera klassificeringen på diagnoses-tabeller + relevanta Fabric OMOP-tabeller
API: PUT /datamap/api/atlas/v2/entity/guid/{guid}/classifications
Targets:
  - diagnoses (SQL, 2 instanser) — icd10_code mappar kliniskt till SNOMED
  - condition_occurrence (Fabric lakehouse) — SNOMED-baserad concept_id
  - measurement (Fabric lakehouse) — SNOMED-baserad concept_id
  - specimen (Fabric lakehouse) — SNOMED-baserad concept_id
Verifiering: Sök classification "SNOMED CT Code" → ska ge > 0
```

#### Åtgärd 1.2: Applicera OMOP Concept ID på OMOP-tabeller
```
Mål: Applicera på alla OMOP Fabric lakehouse-tabeller
API: PUT /datamap/api/atlas/v2/entity/guid/{guid}/classifications
Targets:
  - condition_occurrence (Fabric lakehouse)
  - drug_exposure (Fabric lakehouse)
  - measurement (Fabric lakehouse)
  - person (Fabric lakehouse)
  - specimen (Fabric lakehouse)
  - visit_occurrence (Fabric lakehouse)
Verifiering: Sök classification "OMOP Concept ID" → ska ge ≥ 6
```

#### Åtgärd 1.3: Flytta entiteter till barncancer-samling
```
Mål: Ge barncancer-samlingen egna entiteter
API: POST /datamap/api/entity/moveTo?collectionId=barncancer
Targets:
  - BrainChild Barncancerforskning (healthcare_data_product, nu i halsosjukvard)
  - BrainChild FHIR Server (R4) (healthcare_fhir_service, nu i fabric-brainchild)
  - BrainChild DICOM Server (healthcare_dicom_service, nu i fabric-brainchild)
Verifiering: Sök collectionId=barncancer → ska ge ≥ 3
```

#### Åtgärd 1.4: Länka fler glossary-termer till entiteter
```
Mål: Minska 99 olänkade → ~60 (länka ca 30-40 termer till)
API: PUT /catalog/api/atlas/v2/glossary/term/{termGuid} med assignedEntities
Strategi per kategori:
  Barncancerforskning → healthcare_fhir_resource_type, healthcare_dicom_*, Fabric BC-tabeller
  Klinisk Data → vitals_labs, diagnoses, medications, encounters (SQL + Fabric)
  Dataarkitektur → fabric_lake_warehouse, fabric_lakehouse_table, lineage Process
  Kliniska Standarder → diagnoses (ICD-10), medications (ATC)
  Interoperabilitet → healthcare_fhir_service, FHIR resource types
Verifiering: Räkna assignedEntities > 0 → ska ge > 76
```

---

### Fas 2: Manuella portalåtgärder

#### Åtgärd 2.1: MIP Sensitivity Labels
```
Steg:
  1. Gå till https://purview.microsoft.com → Inställningar → Information Protection
  2. Logga in som Global Admin
  3. Aktivera "Sensitivity labels" → Godkänn consent
  4. Skapa etiketter: "Känslig - PHI", "Intern", "Konfidentiell - Forskning"
  5. Publicera etikettpolicy till Purview-kontot
Verifiering: Se etiketter i Data Catalog → Asset → Sensitivity label dropdown
```

#### Åtgärd 2.2: Koppla Governance Domains till termer
```
Steg:
  1. Gå till https://purview.microsoft.com → Governance Domains
  2. Öppna "Klinisk Vård" → Klicka "Assign glossary terms"
     → Välj termer från Klinisk Data-kategorin (33 termer)
  3. Öppna "Forskning & Genomik" → Assign
     → Välj termer från Barncancerforskning-kategorin (54 termer)
  4. Öppna "Interoperabilitet & Standarder" → Assign
     → Välj termer från Interoperabilitet (22) + Kliniska Standarder (17)
  5. Öppna "Data & Analytics" → Assign
     → Välj termer från Dataarkitektur-kategorin (19 termer)
Verifiering: Öppna varje domain i portalen → se att terms visas
```

#### Åtgärd 2.3: Fabric-skanning
```
Steg:
  1. Gå till Purview → Data Map → Data Sources → Fabric
  2. Välj "Scan-IzR" → Klicka "Run scan now"
  3. Vänta på slutförande → kontrollera status
  4. Om "CompletedWithExceptions" kvarstår: Kontrollera workspace-behörigheter
     → Se till att Purview Managed Identity har "Viewer"-roll i alla Fabric workspaces
Alternative: Acceptera nuvarande status (567 entiteter lyckades, 0 exceptions loggade)
```

---

### Fas 3: Data-komplettering

#### Åtgärd 3.1: Ladda upp resterande medications
```
Steg:
  1. cd c:\code\healthcare-analytics\healthcare-analytics
  2. python scripts/fast_medications.py  (eller deploy_omop.py --table medications)
  3. Verifiera: SELECT COUNT(*) FROM hca.medications → ska ge 60563
```

#### Åtgärd 3.2: Key Vault fhir-service-url
```
Steg:
  az keyvault secret set \
    --vault-name <ditt-keyvault-namn> \
    --name "fhir-service-url" \
    --value "https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com"
Verifiering: az keyvault secret show --vault-name <namn> --name fhir-service-url
```

---

## DEL 3: PRIORITETSORDNING

| Prio | Åtgärd | Typ | Tid | Effekt |
|------|--------|-----|-----|--------|
| 1 | 1.1 SNOMED CT Code | Script | 2 min | Fixar classification gap |
| 2 | 1.2 OMOP Concept ID | Script | 2 min | Fixar classification gap |
| 3 | 1.3 Flytta till barncancer | Script | 1 min | Fixar tom samling |
| 4 | 1.4 Term-entity-kopplingar | Script | 5 min | Förbättrar governance |
| 5 | 2.2 Domain-term-koppling | Portal | 15 min | Kräver manuellt |
| 6 | 3.1 Medications upload | Script | 10 min | Data-komplettering |
| 7 | 2.1 MIP Labels | Portal | 20 min | Kräver Global Admin |
| 8 | 3.2 Key Vault secret | CLI | 1 min | Infra-fix |
| 9 | 2.3 Fabric re-scan | Portal | 15 min | Valfritt |

---

## DEL 4: FÖRVÄNTAD SLUTSTATUS EFTER ÅTGÄRDER

| Mätpunkt | Nu | Efter Fas 1 | Efter Fas 2+3 |
|----------|-----|-------------|---------------|
| Klassificeringar med 0 entiteter | 2 | 0 | 0 |
| Tomma samlingar | 1 (barncancer) | 0 | 0 |
| Olänkade termer | 99 | ~60 | ~60* |
| Domain-term-koppling | 0 | 0 | 145 |
| MIP Labels | Nej | Nej | Ja |
| SQL medications | 20k | 20k | 60k |
| Fabric scan | WithExceptions | WithExceptions | OK/Accepterad |

\* Konceptuella termer utan direkt dataentitet — normalt att ~40% av termer i ett healthcare glossary saknar direkt mapping.
