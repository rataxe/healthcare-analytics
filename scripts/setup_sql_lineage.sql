-- ============================================================================
-- PURVIEW SQL LINEAGE SETUP
-- Kör detta script i Azure Portal Query Editor eller Azure Data Studio
-- ============================================================================

-- === STEG 1: Kontrollera om Master Key finns ===
IF NOT EXISTS (
    SELECT * FROM sys.symmetric_keys 
    WHERE name = '##MS_DatabaseMasterKey##'
)
BEGIN
    PRINT 'Creating Master Key...';
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'PurviewLineage2026!Str0ngP@ssw0rd#Secure$';
    PRINT 'Master Key created successfully';
END
ELSE
BEGIN
    PRINT 'Master Key already exists';
END
GO

-- === STEG 2: Skapa användare för prviewacc (Purview System MSI) ===
IF NOT EXISTS (
    SELECT * FROM sys.database_principals 
    WHERE name = 'prviewacc' AND type = 'E'
)
BEGIN
    PRINT 'Creating user: prviewacc';
    CREATE USER [prviewacc] FROM EXTERNAL PROVIDER;
    PRINT 'User prviewacc created successfully';
END
ELSE
BEGIN
    PRINT 'User prviewacc already exists';
END
GO

-- === STEG 3: Ge db_owner till prviewacc ===
PRINT 'Granting db_owner to prviewacc';
ALTER ROLE db_owner ADD MEMBER [prviewacc];
PRINT 'db_owner granted to prviewacc';
GO

-- === STEG 4: Skapa användare för mi-purview (User-assigned MSI) ===
IF NOT EXISTS (
    SELECT * FROM sys.database_principals 
    WHERE name = 'mi-purview' AND type = 'E'
)
BEGIN
    PRINT 'Creating user: mi-purview';
    CREATE USER [mi-purview] FROM EXTERNAL PROVIDER;
    PRINT 'User mi-purview created successfully';
END
ELSE
BEGIN
    PRINT 'User mi-purview already exists';
END
GO

-- === STEG 5: Ge db_owner till mi-purview ===
PRINT 'Granting db_owner to mi-purview';
ALTER ROLE db_owner ADD MEMBER [mi-purview];
PRINT 'db_owner granted to mi-purview';
GO

-- === VERIFIERING ===
PRINT '';
PRINT '============================================================================';
PRINT 'VERIFICATION - Check if users are created correctly';
PRINT '============================================================================';

-- Kontrollera användare
SELECT 
    'Database Users' as Check_Type,
    name as User_Name, 
    type_desc as Type,
    create_date as Created_Date
FROM sys.database_principals
WHERE name IN ('prviewacc', 'mi-purview')
ORDER BY name;

-- Kontrollera roller
SELECT 
    'Role Memberships' as Check_Type,
    rp.name as Role_Name,
    mp.name as Member_Name,
    mp.type_desc as Member_Type
FROM sys.database_role_members rm
JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
JOIN sys.database_principals mp ON rm.member_principal_id = mp.principal_id
WHERE mp.name IN ('prviewacc', 'mi-purview')
ORDER BY mp.name;

-- Kontrollera Master Key
SELECT 
    'Master Key' as Check_Type,
    name as Key_Name,
    CASE 
        WHEN name = '##MS_DatabaseMasterKey##' THEN 'Exists'
        ELSE 'Not Found'
    END as Status
FROM sys.symmetric_keys
WHERE name = '##MS_DatabaseMasterKey##';

PRINT '';
PRINT '============================================================================';
PRINT 'SETUP COMPLETE!';
PRINT 'Next step: Enable Lineage extraction in Purview Portal UI';
PRINT '============================================================================';
