-- ============================================================
-- Healthcare Analytics — Azure SQL DDL Schema
-- Project: LOS & Readmission Predictor
-- Version: 1.0  |  Author: Healthcare Analytics Team
-- ============================================================

CREATE SCHEMA IF NOT EXISTS hca;
GO

-- ============================================================
-- 1. PATIENTS
-- ============================================================
CREATE TABLE hca.patients (
    patient_id          UNIQUEIDENTIFIER    NOT NULL DEFAULT NEWSEQUENTIALID(),
    birth_date          DATE                NOT NULL,
    gender              CHAR(1)             NOT NULL CHECK (gender IN ('M','F','X')),
    -- Socioekonomisk status: 1=Låg, 2=Medel, 3=Hög
    ses_level           TINYINT             NOT NULL CHECK (ses_level BETWEEN 1 AND 3),
    postal_code         VARCHAR(10)         NOT NULL,
    region              VARCHAR(100)        NOT NULL,
    smoking_status      VARCHAR(20)         NOT NULL DEFAULT 'Unknown'
                            CHECK (smoking_status IN ('Never','Former','Current','Unknown')),
    created_at          DATETIME2           NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_patients PRIMARY KEY (patient_id)
);

-- ============================================================
-- 2. ENCOUNTERS  (en rad per vårdtillfälle)
-- ============================================================
CREATE TABLE hca.encounters (
    encounter_id        UNIQUEIDENTIFIER    NOT NULL DEFAULT NEWSEQUENTIALID(),
    patient_id          UNIQUEIDENTIFIER    NOT NULL,
    admission_date      DATE                NOT NULL,
    discharge_date      DATE                NULL,           -- NULL = pågående
    department          VARCHAR(50)         NOT NULL
                            CHECK (department IN (
                                'Akutmedicin','Kirurgi','Kardiologi',
                                'Neurologi','Ortopedi','Onkologi',
                                'Geriatrik','Infektion','Psykiatri'
                            )),
    admission_source    VARCHAR(30)         NOT NULL
                            CHECK (admission_source IN (
                                'Akutmottagning','Remiss','Planerat',
                                'Ambulans','Överflyttning'
                            )),
    discharge_disposition VARCHAR(30)       NULL
                            CHECK (discharge_disposition IN (
                                'Hemgång','Hemgång_hemtjänst','Korttidsboende',
                                'Annan_vårdenhet','Avliden'
                            )),
    los_days            SMALLINT            NULL,           -- beräknad vid utskrivning
    readmission_30d     BIT                 NULL DEFAULT 0,
    created_at          DATETIME2           NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_encounters PRIMARY KEY (encounter_id),
    CONSTRAINT fk_enc_patient FOREIGN KEY (patient_id)
        REFERENCES hca.patients(patient_id)
);

CREATE INDEX ix_enc_patient   ON hca.encounters(patient_id);
CREATE INDEX ix_enc_admission ON hca.encounters(admission_date);

-- ============================================================
-- 3. DIAGNOSES  (ICD-10, primär + sekundära)
-- ============================================================
CREATE TABLE hca.diagnoses (
    diagnosis_id        UNIQUEIDENTIFIER    NOT NULL DEFAULT NEWSEQUENTIALID(),
    encounter_id        UNIQUEIDENTIFIER    NOT NULL,
    icd10_code          VARCHAR(10)         NOT NULL,       -- ex. 'I50.9'
    icd10_description   VARCHAR(200)        NOT NULL,
    diagnosis_type      VARCHAR(10)         NOT NULL CHECK (diagnosis_type IN ('Primary','Secondary')),
    confirmed_date      DATE                NOT NULL,
    CONSTRAINT pk_diagnoses PRIMARY KEY (diagnosis_id),
    CONSTRAINT fk_diag_encounter FOREIGN KEY (encounter_id)
        REFERENCES hca.encounters(encounter_id)
);

CREATE INDEX ix_diag_encounter ON hca.diagnoses(encounter_id);
CREATE INDEX ix_diag_icd10     ON hca.diagnoses(icd10_code);

-- ============================================================
-- 4. VITALS_LABS  (mätningar under vårdtillfället)
-- ============================================================
CREATE TABLE hca.vitals_labs (
    measurement_id      UNIQUEIDENTIFIER    NOT NULL DEFAULT NEWSEQUENTIALID(),
    encounter_id        UNIQUEIDENTIFIER    NOT NULL,
    measured_at         DATETIME2           NOT NULL,
    -- Vitals
    systolic_bp         SMALLINT            NULL,           -- mmHg
    diastolic_bp        SMALLINT            NULL,           -- mmHg
    heart_rate          SMALLINT            NULL,           -- bpm
    temperature_c       DECIMAL(4,1)        NULL,
    oxygen_saturation   DECIMAL(5,2)        NULL,           -- %
    -- Labs
    glucose_mmol        DECIMAL(5,2)        NULL,           -- mmol/L
    creatinine_umol     DECIMAL(7,2)        NULL,           -- µmol/L
    hemoglobin_g        DECIMAL(5,2)        NULL,           -- g/dL
    sodium_mmol         DECIMAL(5,2)        NULL,
    potassium_mmol      DECIMAL(4,2)        NULL,
    -- Antropometri
    bmi                 DECIMAL(5,2)        NULL,
    weight_kg           DECIMAL(6,2)        NULL,
    CONSTRAINT pk_vitals PRIMARY KEY (measurement_id),
    CONSTRAINT fk_vitals_encounter FOREIGN KEY (encounter_id)
        REFERENCES hca.encounters(encounter_id)
);

-- ============================================================
-- 5. MEDICATIONS  (läkemedel vid inskrivning)
-- ============================================================
CREATE TABLE hca.medications (
    medication_id       UNIQUEIDENTIFIER    NOT NULL DEFAULT NEWSEQUENTIALID(),
    encounter_id        UNIQUEIDENTIFIER    NOT NULL,
    atc_code            VARCHAR(10)         NOT NULL,       -- ATC-kod ex. 'C09AA01'
    drug_name           VARCHAR(150)        NOT NULL,
    dose_mg             DECIMAL(10,3)       NULL,
    frequency           VARCHAR(50)         NULL,           -- ex. '1 gång/dag'
    route               VARCHAR(30)         NULL,           -- Oralt/IV/SC
    start_date          DATE                NOT NULL,
    end_date            DATE                NULL,
    CONSTRAINT pk_medications PRIMARY KEY (medication_id),
    CONSTRAINT fk_med_encounter FOREIGN KEY (encounter_id)
        REFERENCES hca.encounters(encounter_id)
);

-- ============================================================
-- 6. VIEWS — för Fabric Pipeline-ingestion
-- ============================================================
GO

-- Vy för ML-feature export (en rad per encounter)
CREATE OR ALTER VIEW hca.vw_ml_encounters AS
SELECT
    e.encounter_id,
    e.patient_id,
    e.admission_date,
    e.discharge_date,
    e.department,
    e.admission_source,
    e.los_days,
    e.readmission_30d,
    DATEDIFF(YEAR, p.birth_date, e.admission_date) AS age_at_admission,
    p.gender,
    p.ses_level,
    p.smoking_status,
    -- Aggregerade lab-värden (senaste mätningen)
    vl.systolic_bp,
    vl.glucose_mmol,
    vl.creatinine_umol,
    vl.bmi,
    -- Primärdiagnos
    d.icd10_code AS primary_icd10,
    d.icd10_description AS primary_diagnosis
FROM hca.encounters e
JOIN hca.patients p ON e.patient_id = p.patient_id
LEFT JOIN (
    SELECT encounter_id,
           systolic_bp, glucose_mmol, creatinine_umol, bmi,
           ROW_NUMBER() OVER (PARTITION BY encounter_id ORDER BY measured_at DESC) AS rn
    FROM hca.vitals_labs
) vl ON e.encounter_id = vl.encounter_id AND vl.rn = 1
LEFT JOIN (
    SELECT encounter_id, icd10_code, icd10_description
    FROM hca.diagnoses
    WHERE diagnosis_type = 'Primary'
) d ON e.encounter_id = d.encounter_id
WHERE e.discharge_date IS NOT NULL;
GO
