# SQL Commands för Purview Lineage Extraction
# Kör dessa kommandon i Azure Portal Query Editor eller Azure Data Studio

## Server: sql-hca-demo.database.windows.net
## Database: HealthcareAnalyticsDB

## STEG 1: Skapa Database Master Key (om den inte finns)
```sql
-- Kontrollera om Master Key finns
SELECT name, algorithm_desc, create_date
FROM sys.symmetric_keys
WHERE name = '##MS_DatabaseMasterKey##';

-- Skapa Master Key (om den inte finns)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'PurviewLineage2026!Str0ngP@ssw0rd#Secure$';
```

## STEG 2: Skapa SQL Users från External Provider (Entra ID)

### User 1: prviewacc (Purview Account System-Managed Identity)
```sql
-- Object ID: 393d3d46-fd94-4128-a0b4-8ceb7ec371b1
-- Application ID: 3ea9578b-83d6-44b4-9f3e-95db78784916

CREATE USER [prviewacc] FROM EXTERNAL PROVIDER;
```

### User 2: mi-purview (User-Assigned Managed Identity)
```sql
-- Object ID: a1110d1d-6964-43c4-b171-13379215123a
-- Application ID: a8edba22-69c2-4437-88be-6309b5126e79

CREATE USER [mi-purview] FROM EXTERNAL PROVIDER;
```

## STEG 3: Grant db_owner Role

```sql
-- Grant db_owner to prviewacc
ALTER ROLE db_owner ADD MEMBER [prviewacc];

-- Grant db_owner to mi-purview
ALTER ROLE db_owner ADD MEMBER [mi-purview];
```

## STEG 4: Verifiera Konfigurationen

```sql
-- Verifiera att users finns
SELECT 
    name as User_Name, 
    type_desc as User_Type,
    create_date as Created
FROM sys.database_principals
WHERE name IN ('prviewacc', 'mi-purview');

-- Verifiera role membership
SELECT 
    rp.name as Role_Name,
    mp.name as Member_Name
FROM sys.database_role_members rm
JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
WHERE mp.name IN ('prviewacc', 'mi-purview') 
  AND rp.name = 'db_owner';
```

---

## ALTERNATIV: Kör alla kommandon på en gång

```sql
-- === COMPLETE LINEAGE SETUP SCRIPT ===

-- 1. Create Master Key
IF NOT EXISTS (
    SELECT * FROM sys.symmetric_keys 
    WHERE name = '##MS_DatabaseMasterKey##'
)
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'PurviewLineage2026!Str0ngP@ssw0rd#Secure$';
    PRINT 'Master key created';
END
ELSE
    PRINT 'Master key already exists';
GO

-- 2. Create prviewacc user
IF NOT EXISTS (
    SELECT * FROM sys.database_principals 
    WHERE name = 'prviewacc' AND type = 'E'
)
BEGIN
    CREATE USER [prviewacc] FROM EXTERNAL PROVIDER;
    PRINT 'User prviewacc created';
END
ELSE
    PRINT 'User prviewacc already exists';
GO

-- 3. Grant db_owner to prviewacc
IF NOT EXISTS (
    SELECT 1 
    FROM sys.database_role_members rm
    JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
    JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
    WHERE rp.name = 'db_owner' AND mp.name = 'prviewacc'
)
BEGIN
    ALTER ROLE db_owner ADD MEMBER [prviewacc];
    PRINT 'Granted db_owner to prviewacc';
END
ELSE
    PRINT 'prviewacc already has db_owner';
GO

-- 4. Create mi-purview user
IF NOT EXISTS (
    SELECT * FROM sys.database_principals 
    WHERE name = 'mi-purview' AND type = 'E'
)
BEGIN
    CREATE USER [mi-purview] FROM EXTERNAL PROVIDER;
    PRINT 'User mi-purview created';
END
ELSE
    PRINT 'User mi-purview already exists';
GO

-- 5. Grant db_owner to mi-purview
IF NOT EXISTS (
    SELECT 1 
    FROM sys.database_role_members rm
    JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
    JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
    WHERE rp.name = 'db_owner' AND mp.name = 'mi-purview'
)
BEGIN
    ALTER ROLE db_owner ADD MEMBER [mi-purview];
    PRINT 'Granted db_owner to mi-purview';
END
ELSE
    PRINT 'mi-purview already has db_owner';
GO

-- 6. Verify configuration
PRINT '';
PRINT '=== VERIFICATION ===';
PRINT '';

SELECT 
    'User Check' as Check_Type,
    name as User_Name, 
    type_desc as User_Type
FROM sys.database_principals
WHERE name IN ('prviewacc', 'mi-purview');

SELECT 
    'Role Check' as Check_Type,
    rp.name as Role_Name,
    mp.name as Member_Name
FROM sys.database_role_members rm
JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
WHERE mp.name IN ('prviewacc', 'mi-purview') 
  AND rp.name = 'db_owner';
```

---

## Hur man kör kommandona:

### Alternativ 1: Azure Portal Query Editor
1. Gå till Azure Portal
2. Navigera till SQL Database: `HealthcareAnalyticsDB`
3. Välj "Query editor" från vänstermenyn
4. Logga in med **Azure AD authentication**
5. Kopiera och klistra in SQL-kommandona ovan
6. Klicka "Run"

### Alternativ 2: Azure Data Studio
1. Öppna Azure Data Studio
2. Anslut till: `sql-hca-demo.database.windows.net`
3. Database: `HealthcareAnalyticsDB`
4. Authentication: **Azure Active Directory**
5. Kör SQL-kommandona

### Alternativ 3: SSMS (SQL Server Management Studio)
1. Anslut till servern med Azure AD authentication
2. Öppna New Query mot `HealthcareAnalyticsDB`
3. Kör SQL-kommandona

---

## Efter konfigurationen:

1. **Azure Portal** → **Purview** → **Data Map** → **Sources**
2. Hitta din **Azure SQL Database** data source
3. **Edit data source** → Aktivera **"Lineage extraction"**
4. Välj authentication: **Managed Identity (prviewacc)**
5. **Run a new scan**
6. Verifiera att lineage visas i **Data Catalog**

---

## Troubleshooting

### Om "CREATE USER FROM EXTERNAL PROVIDER" misslyckas:
- Kontrollera att SQL Server har en **Azure AD admin** konfigurerad
- Azure Portal → SQL Server → Settings → Azure Active Directory admin
- Sätt din användare eller en grupp som AD admin

### Om Master Key redan finns:
- Detta är OK - skip första steget
- Felmeddelandet "There is already a master key" är förväntad

### Om användare redan finns:
- Detta är OK - skip CREATE USER
- Fortsätt med ALTER ROLE kommandot
