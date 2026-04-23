-- ============================================================================
-- PURVIEW LINEAGE DEMO - Skapa tabeller och transformationer
-- Detta script skapar ett mini data warehouse med lineage relationer
-- ============================================================================

-- === STEG 1: Skapa Bronze Layer (rådata) ===

-- Patienter från källsystem
IF OBJECT_ID('bronze.patients_raw', 'U') IS NOT NULL
    DROP TABLE bronze.patients_raw;
GO

CREATE SCHEMA bronze;
GO

CREATE TABLE bronze.patients_raw (
    patient_id VARCHAR(50) PRIMARY KEY,
    personnummer VARCHAR(13),
    fornamn NVARCHAR(100),
    efternamn NVARCHAR(100),
    fodelsedatum DATE,
    kon VARCHAR(10),
    postnummer VARCHAR(10),
    kommun NVARCHAR(100),
    created_at DATETIME2 DEFAULT GETDATE()
);
GO

-- Besök från källsystem
IF OBJECT_ID('bronze.visits_raw', 'U') IS NOT NULL
    DROP TABLE bronze.visits_raw;
GO

CREATE TABLE bronze.visits_raw (
    visit_id VARCHAR(50) PRIMARY KEY,
    patient_id VARCHAR(50),
    visit_date DATETIME2,
    visit_type VARCHAR(50),
    department NVARCHAR(100),
    icd10_code VARCHAR(10),
    cost_sek DECIMAL(10,2),
    created_at DATETIME2 DEFAULT GETDATE()
);
GO

-- Läkemedel från källsystem
IF OBJECT_ID('bronze.medications_raw', 'U') IS NOT NULL
    DROP TABLE bronze.medications_raw;
GO

CREATE TABLE bronze.medications_raw (
    medication_id VARCHAR(50) PRIMARY KEY,
    patient_id VARCHAR(50),
    visit_id VARCHAR(50),
    atc_code VARCHAR(20),
    medication_name NVARCHAR(200),
    dosage NVARCHAR(100),
    prescribed_date DATE,
    created_at DATETIME2 DEFAULT GETDATE()
);
GO

-- === STEG 2: Skapa Silver Layer (curerad data) ===

CREATE SCHEMA silver;
GO

-- Curerade patienter med dataqualitet
CREATE OR ALTER VIEW silver.patients_clean AS
SELECT 
    patient_id,
    personnummer,
    UPPER(fornamn) as fornamn_normalized,
    UPPER(efternamn) as efternamn_normalized,
    fodelsedatum,
    CASE 
        WHEN kon IN ('M', 'Man', 'MALE') THEN 'M'
        WHEN kon IN ('F', 'Kvinna', 'FEMALE') THEN 'F'
        ELSE 'Unknown'
    END as kon_standardized,
    YEAR(GETDATE()) - YEAR(fodelsedatum) as alder,
    postnummer,
    kommun,
    created_at
FROM bronze.patients_raw
WHERE personnummer IS NOT NULL
    AND fodelsedatum IS NOT NULL;
GO

-- Curerade besök med enrichment
CREATE OR ALTER VIEW silver.visits_enriched AS
SELECT 
    v.visit_id,
    v.patient_id,
    v.visit_date,
    YEAR(v.visit_date) as visit_year,
    MONTH(v.visit_date) as visit_month,
    DATENAME(WEEKDAY, v.visit_date) as visit_weekday,
    v.visit_type,
    v.department,
    v.icd10_code,
    SUBSTRING(v.icd10_code, 1, 3) as icd10_category,
    v.cost_sek,
    p.alder as patient_age_at_visit,
    p.kon_standardized as patient_gender,
    p.kommun as patient_municipality
FROM bronze.visits_raw v
INNER JOIN silver.patients_clean p ON v.patient_id = p.patient_id
WHERE v.visit_date IS NOT NULL
    AND v.cost_sek >= 0;
GO

-- Curerade läkemedel med klassificering
CREATE OR ALTER VIEW silver.medications_classified AS
SELECT 
    m.medication_id,
    m.patient_id,
    m.visit_id,
    m.atc_code,
    SUBSTRING(m.atc_code, 1, 1) as atc_level1,
    SUBSTRING(m.atc_code, 1, 3) as atc_level2,
    m.medication_name,
    m.dosage,
    m.prescribed_date,
    CASE 
        WHEN SUBSTRING(m.atc_code, 1, 1) = 'N' THEN 'Nervous system'
        WHEN SUBSTRING(m.atc_code, 1, 1) = 'C' THEN 'Cardiovascular system'
        WHEN SUBSTRING(m.atc_code, 1, 1) = 'A' THEN 'Alimentary tract and metabolism'
        WHEN SUBSTRING(m.atc_code, 1, 1) = 'B' THEN 'Blood and blood forming organs'
        ELSE 'Other'
    END as medication_category,
    p.alder as patient_age,
    p.kon_standardized as patient_gender
FROM bronze.medications_raw m
INNER JOIN silver.patients_clean p ON m.patient_id = p.patient_id
WHERE m.atc_code IS NOT NULL;
GO

-- === STEG 3: Skapa Gold Layer (analytics-ready) ===

CREATE SCHEMA gold;
GO

-- Patient summary för dashboards
CREATE OR ALTER VIEW gold.patient_summary AS
SELECT 
    p.patient_id,
    p.fornamn_normalized,
    p.efternamn_normalized,
    p.alder,
    p.kon_standardized,
    p.kommun,
    COUNT(DISTINCT v.visit_id) as total_visits,
    COUNT(DISTINCT m.medication_id) as total_medications,
    SUM(v.cost_sek) as total_cost_sek,
    MAX(v.visit_date) as last_visit_date,
    DATEDIFF(DAY, MAX(v.visit_date), GETDATE()) as days_since_last_visit
FROM silver.patients_clean p
LEFT JOIN silver.visits_enriched v ON p.patient_id = v.patient_id
LEFT JOIN silver.medications_classified m ON p.patient_id = m.patient_id
GROUP BY 
    p.patient_id,
    p.fornamn_normalized,
    p.efternamn_normalized,
    p.alder,
    p.kon_standardized,
    p.kommun;
GO

-- Department performance metrics
CREATE OR ALTER VIEW gold.department_metrics AS
SELECT 
    department,
    visit_year,
    visit_month,
    COUNT(DISTINCT visit_id) as visit_count,
    COUNT(DISTINCT patient_id) as unique_patients,
    AVG(cost_sek) as avg_cost_per_visit,
    SUM(cost_sek) as total_revenue,
    AVG(patient_age_at_visit) as avg_patient_age
FROM silver.visits_enriched
WHERE visit_year >= 2023
GROUP BY department, visit_year, visit_month;
GO

-- Medication usage trends
CREATE OR ALTER VIEW gold.medication_trends AS
SELECT 
    medication_category,
    atc_level2,
    YEAR(prescribed_date) as prescribed_year,
    COUNT(DISTINCT medication_id) as prescription_count,
    COUNT(DISTINCT patient_id) as unique_patients,
    AVG(patient_age) as avg_patient_age,
    SUM(CASE WHEN patient_gender = 'M' THEN 1 ELSE 0 END) as male_patients,
    SUM(CASE WHEN patient_gender = 'F' THEN 1 ELSE 0 END) as female_patients
FROM silver.medications_classified
WHERE prescribed_date >= '2023-01-01'
GROUP BY medication_category, atc_level2, YEAR(prescribed_date);
GO

-- High-risk patients (många besök + dyra behandlingar)
CREATE OR ALTER VIEW gold.high_risk_patients AS
SELECT TOP 100
    ps.patient_id,
    ps.fornamn_normalized,
    ps.efternamn_normalized,
    ps.alder,
    ps.total_visits,
    ps.total_medications,
    ps.total_cost_sek,
    ps.days_since_last_visit,
    COUNT(DISTINCT v.icd10_category) as unique_diagnoses,
    STRING_AGG(DISTINCT v.icd10_category, ', ') as diagnosis_categories
FROM gold.patient_summary ps
INNER JOIN silver.visits_enriched v ON ps.patient_id = v.patient_id
GROUP BY 
    ps.patient_id,
    ps.fornamn_normalized,
    ps.efternamn_normalized,
    ps.alder,
    ps.total_visits,
    ps.total_medications,
    ps.total_cost_sek,
    ps.days_since_last_visit
HAVING COUNT(DISTINCT v.visit_id) >= 5
    OR SUM(v.cost_sek) >= 100000
ORDER BY ps.total_cost_sek DESC;
GO

-- === STEG 4: Infoga lite demo data ===

-- Infoga demo patienter
INSERT INTO bronze.patients_raw (patient_id, personnummer, fornamn, efternamn, fodelsedatum, kon, postnummer, kommun)
VALUES 
    ('P001', '19850315-1234', 'Anna', 'Andersson', '1985-03-15', 'F', '11122', 'Stockholm'),
    ('P002', '19920728-5678', 'Erik', 'Eriksson', '1992-07-28', 'M', '41103', 'Göteborg'),
    ('P003', '19781203-9012', 'Maria', 'Svensson', '1978-12-03', 'F', '21212', 'Malmö'),
    ('P004', '19650520-3456', 'Lars', 'Larsson', '1965-05-20', 'M', '75222', 'Uppsala'),
    ('P005', '19990102-7890', 'Sofia', 'Johansson', '1999-01-02', 'F', '11122', 'Stockholm');
GO

-- Infoga demo besök
INSERT INTO bronze.visits_raw (visit_id, patient_id, visit_date, visit_type, department, icd10_code, cost_sek)
VALUES 
    ('V001', 'P001', '2024-01-15 09:30', 'Planerad', 'Kardiologi', 'I10', 4500.00),
    ('V002', 'P001', '2024-03-20 14:15', 'Akut', 'Akutmottagning', 'I50', 12000.00),
    ('V003', 'P002', '2024-02-10 10:00', 'Planerad', 'Ortopedi', 'M23', 8500.00),
    ('V004', 'P003', '2024-01-25 11:30', 'Planerad', 'Gynekologi', 'N80', 5200.00),
    ('V005', 'P004', '2024-03-05 08:45', 'Planerad', 'Neurologi', 'G40', 9800.00),
    ('V006', 'P001', '2024-04-10 13:00', 'Uppföljning', 'Kardiologi', 'I10', 2500.00),
    ('V007', 'P005', '2024-02-28 16:20', 'Akut', 'Akutmottagning', 'J06', 3200.00);
GO

-- Infoga demo läkemedel
INSERT INTO bronze.medications_raw (medication_id, patient_id, visit_id, atc_code, medication_name, dosage, prescribed_date)
VALUES 
    ('M001', 'P001', 'V001', 'C09AA01', 'Ramipril', '5mg 1x dagligen', '2024-01-15'),
    ('M002', 'P001', 'V001', 'C07AB03', 'Atenolol', '50mg 1x dagligen', '2024-01-15'),
    ('M003', 'P001', 'V002', 'C03CA01', 'Furosemid', '40mg 2x dagligen', '2024-03-20'),
    ('M004', 'P002', 'V003', 'M01AE01', 'Ibuprofen', '400mg vid behov', '2024-02-10'),
    ('M005', 'P003', 'V004', 'G03AC06', 'Norelgestromin', 'Plåster 1x vecka', '2024-01-25'),
    ('M006', 'P004', 'V005', 'N03AF01', 'Karbamazepin', '200mg 2x dagligen', '2024-03-05'),
    ('M007', 'P005', 'V007', 'J01CA04', 'Amoxicillin', '500mg 3x dagligen', '2024-02-28');
GO

-- === STEG 5: Skapa en Stored Procedure med komplexare lineage ===

CREATE OR ALTER PROCEDURE gold.sp_refresh_patient_analytics
AS
BEGIN
    -- Denna procedure skapar extra lineage genom att materialisera data
    
    -- Skapa temp tabell för patient risk score
    IF OBJECT_ID('tempdb..#patient_risk_scores') IS NOT NULL
        DROP TABLE #patient_risk_scores;
    
    SELECT 
        p.patient_id,
        p.alder,
        COUNT(DISTINCT v.visit_id) as visit_count,
        COUNT(DISTINCT m.medication_id) as med_count,
        SUM(v.cost_sek) as total_cost,
        -- Risk score baserat på besök, läkemedel och kostnad
        (COUNT(DISTINCT v.visit_id) * 10 + 
         COUNT(DISTINCT m.medication_id) * 5 + 
         SUM(v.cost_sek) / 1000) as risk_score
    INTO #patient_risk_scores
    FROM silver.patients_clean p
    LEFT JOIN silver.visits_enriched v ON p.patient_id = v.patient_id
    LEFT JOIN silver.medications_classified m ON p.patient_id = m.patient_id
    GROUP BY p.patient_id, p.alder;
    
    -- Visa resultat
    SELECT 
        patient_id,
        alder,
        visit_count,
        med_count,
        total_cost,
        risk_score,
        CASE 
            WHEN risk_score > 100 THEN 'High Risk'
            WHEN risk_score > 50 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END as risk_category
    FROM #patient_risk_scores
    ORDER BY risk_score DESC;
END;
GO

-- === VERIFIERING ===

PRINT '';
PRINT '============================================================================';
PRINT 'LINEAGE DEMO - UPPKOPPLAT!';
PRINT '============================================================================';
PRINT '';
PRINT 'Skapade:';
PRINT '  - 3 Bronze tabeller (patients_raw, visits_raw, medications_raw)';
PRINT '  - 3 Silver views (patients_clean, visits_enriched, medications_classified)';
PRINT '  - 4 Gold views (patient_summary, department_metrics, medication_trends, high_risk_patients)';
PRINT '  - 1 Stored procedure (sp_refresh_patient_analytics)';
PRINT '';
PRINT 'Demo data:';
PRINT '  - 5 patienter';
PRINT '  - 7 besök';
PRINT '  - 7 läkemedel';
PRINT '';
PRINT 'Testa views:';
PRINT '  SELECT * FROM gold.patient_summary;';
PRINT '  SELECT * FROM gold.department_metrics;';
PRINT '  EXEC gold.sp_refresh_patient_analytics;';
PRINT '';
PRINT 'Nästa steg i Purview:';
PRINT '  1. Kör en full scan av HealthcareAnalyticsDB';
PRINT '  2. Vänta 10-15 min för scan att slutföras';
PRINT '  3. Gå till Data Map → Browse → SQL Database';
PRINT '  4. Klicka på en tabell/view → Lineage tab';
PRINT '  5. Se hur data flödar från Bronze → Silver → Gold!';
PRINT '';
PRINT '============================================================================';
