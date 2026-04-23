"""
==========================================================================
  PURVIEW GOVERNANCE - MANUELLA STEG & STATUS
  Genererat: 2025-session
==========================================================================

SAMMANFATTNING: Varför ser du INGET i Purview-portalen?
=====================================================
Du MÅSTE tilldela dig själv roller i Purview-kollektion-hierarkin.
Utan roller syns inga assets, glossary, klassificeringar eller lineage.

=== STEG 1: PORTALÅTKOMST (KRITISKT - GÖR DETTA FÖRST) ===

A) Klassiska portalen (web.purview.azure.com):
   1. Gå till: https://web.purview.azure.com/resource/prviewacc
   2. Klicka "Data Map" → "Collections" i vänstermenyn
   3. Klicka på ROOT-kollektionen "prviewacc"
   4. Gå till fliken "Role assignments"
   5. Lägg till admin@MngEnvMCAP522719.onmicrosoft.com i ALLA 4 roller:
      - Collection Admin
      - Data Source Admin
      - Data Curator
      - Data Reader
   6. Upprepa för ALLA barnkollektioner:
      - halsosjukvard
      - sql-databases
      - fabric-analytics
      - barncancer
      - fabric-brainchild
      - upiwjm (IT)

B) Nya portalen (purview.microsoft.com):
   1. Kräver "Data Governance Administrator" Entra ID-roll
   2. Azure Portal → Entra ID → Roles → Sök "Data Governance Administrator"
   3. Tilldela till admin@MngEnvMCAP522719.onmicrosoft.com
   4. Kan ta upp till 30 min att propagera


=== STEG 2: MIP SENSITIVITY LABELS ===

Du bekräftade att du har MIP-licens. Aktivering:
   1. Azure Portal → sök "prviewacc" (Purview-kontot)
   2. Settings → Information protection
   3. Klicka "Enable" / "Turn on"
   4. Kräver: Global Administrator ELLER Compliance Administrator-roll
   5. Om labels inte syns: Microsoft 365 Compliance Center →
      Information protection → Labels → Publish labels
   6. Scoped till Purview: "Auto-labeling for schematized data assets"


=== STEG 3: GOVERNANCE DOMAINS (nya portalen) ===

Måste skapas manuellt i purview.microsoft.com:
   1. Gå till: https://purview.microsoft.com
   2. Data Governance → Governance Domains
   3. Skapa:
      a) "Klinisk Data" - Beskrivning: Patientdata, diagnoser, besök
      b) "Genomik & Forskning" - Beskrivning: BrainChild, sekvensering
      c) "Interoperabilitet" - Beskrivning: FHIR, DICOM, standarder
   4. Koppla glossary-termer till respektive domain


=== STEG 4: SQL-SCAN MED KLASSIFICERINGSREGLER ===

För att Purview ska auto-klassificera SQL-kolumner:
   1. Klassiska portalen → Data Map → Sources
   2. Klicka på "sql-hca-demo"
   3. "New scan" → Välj HealthcareAnalyticsDB
   4. Under "Scan rule set" → Skapa ny eller redigera:
      - Lägg till custom classification rules:
        * Swedish Personnummer (regex: \\d{8}-\\d{4})
        * ICD10 Diagnosis Code (regex: [A-Z]\\d{2}\\.?\\d{0,2})
   5. Kör scan → Verifiera klassificeringar i assets


=== STEG 5: MEDICATIONS-UPPLADDNING (valfritt) ===

40,563 rader kvar av 60,563 totalt i medications-tabellen.
   Kör: python scripts/resume_upload.py
   (eller python scripts/fast_medications.py med offset=20000)


=== STEG 6: KEY VAULT-HEMLIGHET ===

   az keyvault secret set --vault-name kv-brainchild --name fhir-service-url --value "https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com"


==========================================================================
  AUTOMATISERAD STATUS - VAD SOM ÄR KLART
==========================================================================

✅ Kollektionshierarki:
   prviewacc (root)
   ├── halsosjukvard ............ 38 entities (4 data products + 34 lineage)
   │   ├── sql-databases ........ 21 entities (SQL tables/views/columns)
   │   └── fabric-analytics ..... 567 entities (Fabric workspace)
   ├── barncancer
   │   └── fabric-brainchild .... 29 entities (FHIR/DICOM + Fabric)
   └── upiwjm (IT)
   [root: 151 glossary items - kan ej flyttas, by design]

✅ Custom Classification TypeDefs (6 st, SPACE i namn):
   - Swedish Personnummer
   - Patient Name PHI
   - ICD10 Diagnosis Code
   - SNOMED CT Code
   - FHIR Resource ID
   - OMOP Concept ID

✅ Kolumn-klassificeringar applicerade (13 st):
   - patients.patient_id:         Swedish Personnummer + Patient Name PHI + FHIR Resource ID
   - patients.birth_date:         Patient Name PHI
   - encounters.patient_id:       Swedish Personnummer
   - encounters.encounter_id:     FHIR Resource ID
   - diagnoses.icd10_code:        ICD10 Diagnosis Code
   - diagnoses.encounter_id:      FHIR Resource ID
   - vitals_labs.encounter_id:    FHIR Resource ID
   - medications.encounter_id:    FHIR Resource ID
   - vw_ml_encounters.patient_id: Swedish Personnummer
   - vw_ml_encounters.primary_icd10: ICD10 Diagnosis Code

✅ Glossary: 145 termer i 5 kategorier (Sjukvårdstermer)
✅ Lineage: 34 Process-entiteter (pipeline-lineage)
✅ Term-Entity-länkar: 45/47 fungerar
✅ Custom entity types: 5 st (FHIR, DICOM, data_product)
✅ Data sources: SQL + Fabric registrerade & skannade
✅ BrainChild Fabric: 6 notebooks, 2 pipelines, lakehouse, 285 filer
✅ Healthcare Analytics Fabric: 3 notebooks, 1 pipeline
✅ SQL schema: 5 tabeller + 1 vy i hca-schema

❌ MIP sensitivity labels: Kräver manuell aktivering (Steg 2)
❌ Governance domains: Kräver manuell skapning (Steg 3)
❌ Medications: 20,000/60,563 uppladdade
❌ Key Vault: fhir-service-url saknas

==========================================================================
"""
print(__doc__)
