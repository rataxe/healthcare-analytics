# Purview Quick Reference — Manuella Steg

**Status:** 🟢 85% komplett | 🟡 15% kräver manuell åtgärd

---

## ⚡ TL;DR — Vad måste göras manuellt?

| # | Steg | Tid | Prioritet | Status |
|---|------|-----|-----------|--------|
| 1 | **Collection Role Assignments** | 10 min | 🔴 **KRITISK** | ⚠️ |
| 2 | MIP Sensitivity Labels | 15 min | 🟡 Medel | ⚠️ |
| 3 | Domain-Term-kopplingar | 30 min | 🟡 Medel | ⚠️ |
| 4 | SQL Custom Scan Rules | 10 min | 🟢 Låg | ⚠️ |
| 5 | SQL Medications (40k rader) | 30 min | 🟢 Låg | ⚠️ |
| 6 | Key Vault Secret | 2 min | 🟢 Låg | ⚠️ |

**BÖRJA MED STEG 1** — utan det ser du INGET i portalen!

---

## 🚨 STEG 1: Collection Roles (KRITISK — GÖR FÖRST!)

**Varför:** Du ser inga entities, glossary eller lineage utan roller.

**Snabbguide:**
1. Gå till: https://web.purview.azure.com/resource/prviewacc
2. Data Map → Collections
3. För **varje collection** (prviewacc, halsosjukvard, sql-databases, fabric-analytics, barncancer, fabric-brainchild):
   - Klicka på collection → Role assignments
   - Lägg till `admin@MngEnvMCAP522719.onmicrosoft.com` i ALLA 4 roller:
     - ✅ Collection Admin
     - ✅ Data Source Admin
     - ✅ Data Curator
     - ✅ Data Reader
4. Refresh portalen (Ctrl+F5)
5. ✅ Nu ska du se 650+ entities, 145 glossary-termer

**Tid:** 10 minuter  
**Verifiering:** Sök "patients" i portal → SQL-tabell visas

---

## 🔒 STEG 2: MIP Sensitivity Labels

**Varför:** Auto-märkning av PII-data (personnummer, patient names).

**Snabbguide:**
1. Azure Portal → sök "prviewacc"
2. Settings → Information protection → **Enable**
3. Om error (AADSTS65002): Be Global Admin att grant consent
4. M365 Compliance Center: https://compliance.microsoft.com → Labels → Publish labels → Scope: "Schematized data assets"
5. Purview → Data Map → Classifications → Info protection → Skapa auto-labeling rule:
   - If `classification = Swedish Personnummer` → Apply label `Highly Confidential`
6. Kör scan om (SQL-scan + Fabric-scan)

**Tid:** 15 minuter (+ 30-60 min propagation)  
**Kräver:** Global Administrator ELLER Compliance Administrator  
**Verifiering:** Sök "patients" → kolumn `social_security_number` har label "Highly Confidential"

---

## 🏷️ STEG 3: Domain-Term-kopplingar

**Varför:** Organisera 145 termer enligt 4 affärsdomäner.

**Snabbguide:**
1. https://purview.microsoft.com → Data Catalog → Business Glossary
2. För varje term, tilldela domain:

**Domain: Klinisk Data** (45 termer)  
Personnummer, Besökstillfälle, Diagnoskod, Vårdtillfälle, Patient, CCI-score, etc.

**Domain: Genomik & Forskning** (28 termer)  
VCF, DNA-sekvens, Genotyp, FASTA, FASTQ, BAM/CRAM, dbSNP, ClinVar, etc.

**Domain: Interoperabilitet** (32 termer)  
FHIR Patient, FHIR Observation, DICOM Study, HL7 Message, LOINC, SNOMED CT, etc.

**Domain: ML & Prediktioner** (12 termer)  
MLflow Experiment, ML Model, Feature Engineering, Batch Scoring, Risk Category, etc.

3. För varje term: Edit → Business Domain → Välj domain → Save

**Tid:** 20-30 minuter  
**Ingen API finns** — måste göras manuellt  
**Verifiering:** Data Governance → Governance Domains → "Klinisk Data" → visar ~45 termer

---

## 🔍 STEG 4: SQL Custom Scan Rules

**Snabbguide:**
1. https://web.purview.azure.com/resource/prviewacc → Data Map → Sources → sql-hca-demo
2. Scans → Edit scan → Scan rule set → Add custom rules:

```
Swedish Personnummer:  \d{8}-\d{4}
ICD10 Diagnosis Code:  [A-Z]\d{2}\.?\d{0,2}
SNOMED CT Code:        \b\d{6,18}\b
LOINC Code:            \d{4,5}-\d
```

3. Run scan now
4. Vänta 15-30 min

**Tid:** 10 minuter  
**Verifiering:** Sök "diagnoses" → kolumn `icd10_code` har klassificering "ICD10 Diagnosis Code"

---

## 💊 STEG 5: SQL Medications (40k rader kvar)

**Nuvarande:** 20,000 / 60,563 rader (33%)

**Snabbguide:**
```bash
cd c:\code\healthcare-analytics\healthcare-analytics
python scripts/fast_medications.py
# Vänta 20-30 min
# ✅ 60,563 medications uploaded
```

**Verifiering:**
```sql
SELECT COUNT(*) FROM hca.medications;
-- Resultat: 60,563 ✅
```

**Tid:** 30 minuter  
**Alternativ:** `python scripts/fast_medications.py --offset 20000` (om upload avbrutits)

---

## 🔑 STEG 6: Key Vault Secret

**Saknas:** `fhir-service-url` i `kv-brainchild`

**Snabbguide (Azure CLI):**
```bash
az keyvault secret set \
  --vault-name kv-brainchild \
  --name fhir-service-url \
  --value "https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com"
```

**Alternativ (Portal):**
1. Azure Portal → kv-brainchild → Secrets → Generate/Import
2. Name: `fhir-service-url`
3. Value: `https://brainchildhdws-brainchildfhir.fhir.azurehealthcareapis.com`
4. Create

**Tid:** 2 minuter  
**Verifiering:** BrainChild notebook 05_ingest_dicom_bronze.py kör utan KeyError

---

## ✅ Snabb Verifiering (Efter Alla Steg)

```bash
# Terminal:
cd c:\code\healthcare-analytics\healthcare-analytics
python scripts/_verify_plan.py
```

**Förväntat output:**
```
1.1 SNOMED CT Code klassificering:  15 entiteter ✅
1.2 OMOP Concept ID klassificering: 9 entiteter ✅
1.3 barncancer-samling:             29 entiteter ✅
1.4 Term-entity-kopplingar:         143/145 kopplade ✅

2.1 MIP Sensitivity Labels:         Kräver manuell admin-åtgärd  ✅ (om gjort)
2.2 Domain-term-koppling:           Kräver manuell portal-åtgärd ✅ (om gjort)
2.3 Fabric re-scan:                 CompletedWithExceptions      ⚠️ (valfritt)

3.1 SQL medications:                60,563/60,563 ✅
3.2 Key Vault fhir-service-url:     ✅ (om gjort)
```

---

## 📚 Fullständig Guide

Se [PURVIEW_COMPLETE_GUIDE.md](PURVIEW_COMPLETE_GUIDE.md) för:
- Detaljerad status per komponent
- Troubleshooting-guide
- API-dokumentation
- Maintenance & drift-rekommendationer
- Best practices & lärdomar

---

## 📞 Snabb Support

**Problem:** Ser inga entities i portalen  
**Fix:** STEG 1 (Collection Role Assignments)

**Problem:** Scan failed 'Unauthorized'  
**Fix:** Verifiera Purview Managed Identity har SQL Server Reader-roll

**Problem:** Custom classification dyker inte upp  
**Fix:** Kör scan om efter att rule lagts till (vänta 15-30 min)

**Problem:** Glossary-term syns inte i search  
**Fix:** Vänta 5-10 min (indexering), kontrollera att status är "Approved" (inte "Draft")

---

**Status:** 🟢 85% komplett automatiskt | 🟡 15% manuella steg kvar  
**Purview:** https://web.purview.azure.com/resource/prviewacc  
**Uppdaterad:** 2026-04-22
