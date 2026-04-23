# 🔗 Se Lineage i Purview - Steg för Steg

## 📋 Översikt

Efter att du kört `create_lineage_demo.sql` har du skapat:

```
Bronze Layer (Rådata)
    ├── patients_raw
    ├── visits_raw
    └── medications_raw
         ↓
Silver Layer (Curerad)
    ├── patients_clean        (transformerar patients_raw)
    ├── visits_enriched       (joinar visits_raw + patients_clean)
    └── medications_classified (joinar medications_raw + patients_clean)
         ↓
Gold Layer (Analytics)
    ├── patient_summary       (aggregerar patients + visits + medications)
    ├── department_metrics    (aggregerar visits_enriched)
    ├── medication_trends     (aggregerar medications_classified)
    └── high_risk_patients    (komplexer join av flera källor)
```

---

## 🚀 STEG 1: Kör SQL Scriptet

1. Öppna **Azure Portal** → **SQL databases** → **HealthcareAnalyticsDB**
2. Öppna **Query editor**
3. Kopiera hela `scripts/create_lineage_demo.sql`
4. Kör scriptet (Run-knappen)
5. Verifiera att du ser: "LINEAGE DEMO - UPPKOPPLAT!"

**Testad data:**
```sql
-- Se patienter och deras sammanfattningar
SELECT * FROM gold.patient_summary;

-- Se avdelnings-metrics
SELECT * FROM gold.department_metrics;

-- Kör risk-analys
EXEC gold.sp_refresh_patient_analytics;
```

---

## 🔍 STEG 2: Scanna Databasen i Purview

### A. Registrera Data Source (om inte redan gjort)

1. Gå till **Purview Portal**: https://purview.microsoft.com
2. **Data Map** → **Sources** → **Register**
3. Välj **Azure SQL Database**
4. Fyll i:
   - **Name:** `HealthcareAnalyticsDB`
   - **Server:** `sql-hca-demo.database.windows.net`
   - **Database:** `HealthcareAnalyticsDB`
   - **Collection:** Välj "Healthcare Analytics" eller Root
5. Klicka **Register**

### B. Aktivera Lineage Extraction

1. Hitta din SQL Database i Sources listan
2. Klicka på **Edit** (penna-ikonen)
3. Under **Lineage** tab:
   - ✅ Enable **"Turn on lineage extraction"**
   - ✅ Välj **"Microsoft Managed Storage"**
4. Klicka **Apply**

### C. Starta en Full Scan

1. Klicka på din SQL Database källa
2. Klicka **New scan**
3. Konfigurera:
   - **Name:** `HealthcareDB-FullScan-20240423`
   - **Credential:** Välj managed identity (prviewacc)
   - **Lineage extraction:** ON ✅
   - **Scope:** Välj hela databasen
4. **Test connection** → ska vara grön
5. Klicka **Continue** → **Continue** → **Save and run**

### D. Vänta på Scan

- **Tid:** 10-20 minuter för första scannen
- **Status:** Data Map → Sources → Scans → View details
- **Klart när:** Status = "Completed" ✅

---

## 👁️ STEG 3: Se Lineage i Purview

### Metod 1: Från Data Catalog

1. Gå till **Data Catalog** → **Browse**
2. Välj **By source type** → **Azure SQL Database**
3. Klicka på `sql-hca-demo` → `HealthcareAnalyticsDB`
4. Du ser nu alla schemas: `bronze`, `silver`, `gold`
5. Klicka på en view, t.ex. **`gold.patient_summary`**
6. Klicka på **Lineage** tab

**Du ser nu:**
```
bronze.patients_raw ────┐
                        ├──→ silver.patients_clean ───┐
bronze.visits_raw ──────┤                             ├──→ gold.patient_summary
                        └──→ silver.visits_enriched ──┤
bronze.medications_raw ────→ silver.medications_classified ──┘
```

### Metod 2: Från Search

1. Gå till **Data Catalog** → **Search**
2. Sök efter `"patient_summary"`
3. Klicka på resultatet **gold.patient_summary**
4. Klicka på **Lineage** tab

### Metod 3: Från Schema

1. **Data Catalog** → **Browse** → **By source type**
2. Navigera: `Azure SQL Database` → `sql-hca-demo` → `HealthcareAnalyticsDB` → `gold` schema
3. Klicka på **gold** schemat
4. Se alla tabeller/views i schemat
5. Klicka på valfri view → **Lineage** tab

---

## 🎨 STEG 4: Utforska Lineage Features

### Kolumn-nivå Lineage

1. I en view (t.ex. `gold.patient_summary`)
2. Klicka på **Lineage** tab
3. Välj **"Column-level lineage"** toggle
4. Klicka på en kolumn (t.ex. `total_visits`)
5. Se hur den spåras tillbaka till `bronze.visits_raw.visit_id`

### Impact Analysis

1. Klicka på `bronze.patients_raw` tabellen
2. Gå till **Lineage** tab
3. Välj **"Downstream"** direction
4. Se alla views som beror på denna tabell:
   - `silver.patients_clean`
   - `gold.patient_summary`
   - `gold.high_risk_patients`

### Stored Procedure Lineage

1. Sök efter `sp_refresh_patient_analytics`
2. Klicka på **Lineage** tab
3. Se hur proceduren läser från flera views och skapar temp-tabeller

---

## 🏆 VAD DU BORDE SE

### Exempel 1: gold.patient_summary

**Upstream (källdata):**
- bronze.patients_raw
- bronze.visits_raw
- bronze.medications_raw

**Intermediate (transformationer):**
- silver.patients_clean
- silver.visits_enriched
- silver.medications_classified

**Current (slutprodukt):**
- gold.patient_summary

### Exempel 2: silver.visits_enriched

**Upstream:**
- bronze.visits_raw (huvuddata)
- bronze.patients_raw → silver.patients_clean (join-data)

**Downstream:**
- gold.patient_summary
- gold.department_metrics
- gold.high_risk_patients

---

## 🔧 FELSÖKNING

### Problem: Ser ingen lineage

**Lösning 1:** Vänta längre
- Lineage processing kan ta 15-30 min efter scan

**Lösning 2:** Kontrollera Lineage är aktiverat
- Data Map → Sources → Din SQL DB → Edit → Lineage tab
- "Turn on lineage extraction" ska vara ON

**Lösning 3:** Kör om scannen
- Ibland behöver första scannen köras igen
- Data Map → Sources → New scan

### Problem: Views syns inte

**Lösning:** Full scan scope
- Scans → Edit → Scope
- Se till att alla schemas är inkluderade (bronze, silver, gold)

### Problem: Column lineage saknas

**Lösning:** Detta är normalt för vissa transformationer
- Komplexa aggregeringar kan inte alltid spåras på kolumn-nivå
- Tabell-nivå lineage fungerar alltid

---

## 📊 DEMO QUERIES FÖR ATT TESTA

Kör dessa i SQL Database för att se att data flödar:

```sql
-- 1. Se alla patienter med summering
SELECT * FROM gold.patient_summary;

-- 2. Se avdelningarnas performance
SELECT * FROM gold.department_metrics ORDER BY visit_count DESC;

-- 3. Se medicin-trender
SELECT * FROM gold.medication_trends ORDER BY prescription_count DESC;

-- 4. Kör risk-analys (använder flera källor)
EXEC gold.sp_refresh_patient_analytics;

-- 5. Se bronze → silver transformation
SELECT 
    'Bronze' as layer, COUNT(*) as rows FROM bronze.patients_raw
UNION ALL
SELECT 
    'Silver' as layer, COUNT(*) as rows FROM silver.patients_clean
UNION ALL
SELECT 
    'Gold' as layer, COUNT(*) as rows FROM gold.patient_summary;
```

---

## 🎯 NÄSTA NIVÅ

### Lägg till mer data för bättre lineage demo:

```sql
-- Lägg till fler patienter (kör i SQL Query Editor)
INSERT INTO bronze.patients_raw (patient_id, personnummer, fornamn, efternamn, fodelsedatum, kon, postnummer, kommun)
VALUES 
    ('P006', '19881115-1111', 'Johan', 'Karlsson', '1988-11-15', 'M', '11122', 'Stockholm'),
    ('P007', '19750420-2222', 'Karin', 'Nilsson', '1975-04-20', 'F', '41103', 'Göteborg');

-- Lägg till fler besök
INSERT INTO bronze.visits_raw (visit_id, patient_id, visit_date, visit_type, department, icd10_code, cost_sek)
VALUES 
    ('V008', 'P006', '2024-04-15 10:00', 'Planerad', 'Allmänmedicin', 'J06', 1800.00),
    ('V009', 'P007', '2024-04-18 14:30', 'Akut', 'Akutmottagning', 'I10', 8500.00);

-- Kör om queries
SELECT * FROM gold.patient_summary;
EXEC gold.sp_refresh_patient_analytics;
```

### Koppla till Glossary Terms:

1. I Purview, gå till tabellen `bronze.patients_raw`
2. Klicka på kolumnen `personnummer`
3. Klicka **Edit** → **Glossary terms**
4. Lägg till din term "Swedish Personnummer"
5. Spara

Nu syns glossary-kopplingen i lineage! 🎉

---

## ✅ CHECKLISTA

- [ ] Kört `create_lineage_demo.sql` i Azure Portal Query Editor
- [ ] Verifierat att tabeller och views finns
- [ ] Registrerat SQL Database som data source i Purview
- [ ] Aktiverat lineage extraction
- [ ] Kört en full scan
- [ ] Väntat 15-20 min på scan att slutföras
- [ ] Öppnat en gold view och sett lineage tab
- [ ] Sett upstream relationer till bronze tabeller
- [ ] Testat column-level lineage
- [ ] Testat downstream impact analysis

**När alla är klara:** Congratulations! Du har nu full lineage i Purview! 🎊
