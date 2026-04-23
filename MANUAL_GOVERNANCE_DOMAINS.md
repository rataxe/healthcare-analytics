# 📋 MANUELL GUIDE: Skapa Governance Domains i Purview

**Datum:** 2026-04-22  
**Purview Account:** `prviewacc`  
**Status:** Governance Domain API är inte tillgängligt — måste skapas manuellt

---

## 🎯 ÖVERSIKT

Du behöver skapa **4 Governance Domains** för att organisera din data governance:

1. **Klinisk Vård** — Patientdata och vårdprocesser
2. **Genomik & Forskning** — BrainChild barncancerforskning
3. **Interoperabilitet** — FHIR, DICOM, HL7, OMOP standarder
4. **ML & Prediktioner** — Machine learning och riskprediktion

**Tidskostnad:** ~30-45 minuter totalt (8-12 min per domain)

---

## 🚀 STEG-FÖR-STEG INSTRUKTIONER

### Steg 1: Öppna Purview Portal

1. Gå till **Microsoft Purview Portal**: https://purview.microsoft.com
2. Logga in med ditt Microsoft-konto: `joandolf@microsoft.com`
3. Du kommer till startsidan för Purview

> **OBS:** Du kan också nå portalen via Azure Portal:  
> https://portal.azure.com → Purview accounts → `prviewacc` → "Open Microsoft Purview governance portal"

---

### Steg 2: Navigera till Governance Domains

**Väg 1 (Recommended):**
```
Microsoft Purview Portal (startsida)
  └─ Data Catalog (vänster meny)
      └─ Governance domains (under "Organize")
          └─ Klicka "Create domain"
```

**Väg 2 (Alternativ):**
```
Microsoft Purview Portal
  └─ Data governance (top navigation)
      └─ Domains
          └─ "+ New domain"
```

**Väg 3 (Via Settings):**
```
Microsoft Purview Portal
  └─ Settings (kugghjul, överst till höger)
      └─ Business Glossary & Domains
          └─ Domains tab
              └─ "+ Create"
```

> **TIPS:** Om du inte ser "Governance domains" i menyn:
> - Kontrollera att du har rätt behörigheter (Collection Admin eller Data Curator)
> - Försök refresha sidan (Ctrl+F5)
> - Kontakta tenant admin om funktionen inte är aktiverad

---

### Steg 3: Skapa Domain 1 — "Klinisk Vård"

Klicka **"+ Create domain"** eller **"+ New domain"** och fyll i:

#### **Grundinformation:**
| Fält | Värde |
|------|-------|
| **Name** | `Klinisk Vård` |
| **Display name** | `Klinisk Vård` (samma) |
| **Description** | `Omfattar patientdata, diagnoser, vårdtillfällen, laboratorieresultat, vitala mätvärden och medicinering för sjukhusvård och primärvård.` |

#### **Metadata (valfritt men rekommenderat):**
| Fält | Värde |
|------|-------|
| **Owner** | `joandolf@microsoft.com` |
| **Domain Type** | `Clinical` eller `Healthcare` |
| **Business Area** | `Healthcare Operations` |
| **Tags** | `patient-data`, `clinical-workflows`, `EHR` |

#### **Scope (vilka data assets):**
```
Exempel på data assets som ska länkas (görs senare):
- SQL Tables: patients, conditions, medications, observations, encounters
- Collections: "Hälsosjukvård", "SQL Databases"
- Glossary Terms: Patient, Diagnos, Läkemedel, Vitala mätvärden
```

#### **Stewards (Data Stewards):**
```
Lägg till ansvariga personer:
- Data Steward: joandolf@microsoft.com (eller ditt team)
- Backup: admin@MngEnvMCAP522719.onmicrosoft.com
```

**Klicka "Create"** → Domain skapas inom några sekunder

---

### Steg 4: Skapa Domain 2 — "Genomik & Forskning"

Klicka **"+ Create domain"** igen:

#### **Grundinformation:**
| Fält | Värde |
|------|-------|
| **Name** | `Genomik & Forskning` |
| **Display name** | `Genomik & Forskning` |
| **Description** | `BrainChild barncancerforskning med DNA-sekvensering, VCF-filer, tumörbiobank, genomiska varianter och NGS-data för precisionsbehandling.` |

#### **Metadata:**
| Fält | Värde |
|------|-------|
| **Owner** | `joandolf@microsoft.com` |
| **Domain Type** | `Research` |
| **Business Area** | `Oncology Research` |
| **Tags** | `genomics`, `VCF`, `cancer-research`, `NGS`, `precision-medicine` |

#### **Scope:**
```
Data assets (länkas senare):
- Fabric Lakehouse: Bronze/Silver/Gold (genomics/)
- Collections: "Barncancerforskning", "Fabric BrainChild"
- Glossary Terms: VCF, Tumörprover, DNA-sekvensering, Variant
```

#### **Stewards:**
```
- Data Steward: joandolf@microsoft.com
- Research Lead: (lägg till om finns)
```

**Klicka "Create"**

---

### Steg 5: Skapa Domain 3 — "Interoperabilitet"

#### **Grundinformation:**
| Fält | Värde |
|------|-------|
| **Name** | `Interoperabilitet` |
| **Display name** | `Interoperabilitet` |
| **Description** | `FHIR R4, DICOM, HL7 v2/v3, OMOP CDM, SNOMED CT, ICD-10, LOINC och andra hälso-IT standarder för datautbyte och semantisk interoperabilitet.` |

#### **Metadata:**
| Fält | Värde |
|------|-------|
| **Owner** | `joandolf@microsoft.com` |
| **Domain Type** | `Technical` eller `Standards` |
| **Business Area** | `Health IT Standards` |
| **Tags** | `FHIR`, `DICOM`, `HL7`, `OMOP`, `interoperability` |

#### **Scope:**
```
Data assets:
- FHIR Resources: Patient, Observation, Condition, DiagnosticReport
- Collections: "IT", "Fabric Analytics"
- Glossary Terms: FHIR Resource, SNOMED CT Code, ICD-10, LOINC
```

#### **Stewards:**
```
- Data Steward: joandolf@microsoft.com
- IT Architect: (lägg till om finns)
```

**Klicka "Create"**

---

### Steg 6: Skapa Domain 4 — "ML & Prediktioner"

#### **Grundinformation:**
| Fält | Värde |
|------|-------|
| **Name** | `ML & Prediktioner` |
| **Display name** | `ML & Prediktioner` |
| **Description** | `Machine learning-modeller (MLflow), feature engineering, batch scoring, riskprediktion, AI-drivna beslutsstöd och modellövervakning för kliniska tillämpningar.` |

#### **Metadata:**
| Fält | Värde |
|------|-------|
| **Owner** | `joandolf@microsoft.com` |
| **Domain Type** | `Analytics` eller `AI/ML` |
| **Business Area** | `Data Science & AI` |
| **Tags** | `machine-learning`, `MLflow`, `predictions`, `feature-store` |

#### **Scope:**
```
Data assets:
- MLflow Models: risk_prediction, readmission_model
- Feature Store: patient_features, lab_aggregates
- Collections: "Fabric Analytics"
- Glossary Terms: Feature Store, ML Model, Batch Scoring
```

#### **Stewards:**
```
- Data Steward: joandolf@microsoft.com
- ML Engineer: (lägg till om finns)
```

**Klicka "Create"**

---

## ✅ VERIFIERA ATT DOMAINS SKAPADES

### Kontrollera i Purview Portal:

1. **Navigera till:** Data Catalog → Governance domains
2. **Du bör se 4 domains:**
   - ✅ Klinisk Vård
   - ✅ Genomik & Forskning
   - ✅ Interoperabilitet
   - ✅ ML & Prediktioner

3. **Klicka på varje domain** för att bekräfta:
   - Name och description är korrekt
   - Owner är satt
   - Tags är korrekt

### Verifiera via script:

```bash
# Kör verifieringsskript
cd c:\code\healthcare-analytics\healthcare-analytics
python scripts/verify_all_purview.py
```

**Förväntat output:**
```
======================================================================
  5. GOVERNANCE DOMAINS
======================================================================
  Domain entities found: 4
    - Klinisk Vård
    - Genomik & Forskning
    - Interoperabilitet
    - ML & Prediktioner
  [+] Governance domains: OK
```

---

## 🔗 NÄSTA STEG: Länka Domains till Glossary Terms

**Efter att domains är skapade, länka dem till glossary terms:**

### Steg 1: Gå till Glossary
```
Data Catalog → Business Glossary → "Sjukvårdstermer"
```

### Steg 2: Redigera termer per domain

#### **Klinisk Vård (45 termer):**
```
Sök efter och redigera följande termer i glossary:
- Patient, Personnummer, Vårdtillfälle, Encounter
- Condition, Diagnos, ICD-10 Code
- Medication, Läkemedel, ATC Code
- Observation, Lab Result, Vital Signs, LOINC Code
- Practitioner, Sjukvårdspersonal
```

För varje term:
1. Klicka på termen → **"Edit"**
2. Scrolla ner till **"Governance Domain"**
3. Välj **"Klinisk Vård"** från dropdown
4. Klicka **"Save"**

#### **Genomik & Forskning (28 termer):**
```
- VCF, Genomic Variant, DNA Sequence
- Tumor Sample, Specimen, Biobank
- NGS, Sequencing Run, BrainChild
- Copy Number Variation, Structural Variant
```

Länka alla till **"Genomik & Forskning"**

#### **Interoperabilitet (32 termer):**
```
- FHIR Resource, FHIR Patient, FHIR Observation
- DICOM Study, DICOM Series, ImagingStudy
- OMOP Concept, OMOP CDM
- SNOMED CT Code, HL7 Message
```

Länka alla till **"Interoperabilitet"**

#### **ML & Prediktioner (12 termer):**
```
- Feature Store, ML Feature, Feature Engineering
- ML Model, MLflow Model, Model Registry
- Batch Scoring, Prediction, Risk Score
```

Länka alla till **"ML & Prediktioner"**

> **TIPS:** Använd **"Bulk Edit"** om tillgängligt för att redigera flera termer samtidigt:
> 1. Markera flera termer (checkbox)
> 2. Klicka **"Bulk actions"** → **"Edit"**
> 3. Välj domain för alla markerade termer
> 4. **"Save"**

---

## 📊 FÖRVÄNTAD SLUTSTATUS

Efter att alla steg är klara:

| Komponent | Status | Antal |
|-----------|--------|-------|
| Governance Domains | ✅ KLART | 4/4 |
| Domain → Term links | ✅ KLART | ~117/145 termer |
| Domain Stewards | ✅ KLART | 1-2 per domain |

---

## 🚨 FELSÖKNING

### Problem: "Kan inte se Governance domains i menyn"

**Lösning:**
1. Kontrollera behörigheter:
   ```
   Du behöver någon av dessa roller:
   - Collection Admin
   - Data Curator
   - Purview Data Source Administrator
   ```

2. Be admin ge dig behörighet:
   ```
   Portal: https://web.purview.azure.com/resource/prviewacc
   → Data Map → Collections → prviewacc (root)
   → Role assignments → Add
   → Välj "Data Curator" eller "Collection Admin"
   → Lägg till joandolf@microsoft.com
   ```

### Problem: "Domain skapas men syns inte i listan"

**Lösning:**
1. Vänta 1-2 minuter (indexering)
2. Refresha sidan (Ctrl+F5)
3. Logga ut och in igen
4. Kontrollera att du är i rätt collection scope

### Problem: "Kan inte länka domain till glossary term"

**Lösning:**
1. Kontrollera att du har "Data Curator" role på glossary
2. Verifiera att domain är fully created (status = Active)
3. Försök editera en term i taget först (inte bulk edit)

---

## 📞 SUPPORT & DOKUMENTATION

**Microsoft Purview Documentation:**
- Governance Domains Overview:  
  https://learn.microsoft.com/purview/concept-governance-domain

- Create and manage domains:  
  https://learn.microsoft.com/purview/how-to-create-and-manage-governance-domains

- Business Glossary & Domains:  
  https://learn.microsoft.com/purview/concept-business-glossary

**Azure Portal:**
- Purview Account: https://portal.azure.com/#@MngEnvMCAP522719.onmicrosoft.com/resource/subscriptions/5b44c9f3-bbe7-464c-aa3e-562726a12004/resourceGroups/purview/providers/Microsoft.Purview/accounts/prviewacc/overview

**Purview Portal:**
- Governance Portal: https://purview.microsoft.com
- Data Catalog: https://web.purview.azure.com/resource/prviewacc

---

## ✅ SLUTSATS

Efter att ha följt denna guide har du:

1. ✅ **Skapat 4 Governance Domains** manuellt
2. ✅ **Konfigurerat metadata** (owner, tags, description)
3. ✅ **Länkat domains till glossary terms** (~117 termer)
4. ✅ **Verifierat att allt fungerar** via portal och script

**Total tid:** ~30-45 minuter

**Resultat:** Fullständig organisatorisk struktur för data governance! 🎉

---

**Skapat:** 2026-04-22  
**Av:** Automated Purview Configuration  
**För:** Healthcare Analytics POC — prviewacc
