# Steg-för-steg: Kör SQL Lineage Setup i Azure Portal

## 📋 FÖRBEREDELSE

**Du behöver:**
- Azure Portal access
- SQL Database: `HealthcareAnalyticsDB` på server `sql-hca-demo.database.windows.net`
- Azure AD admin rättigheter på SQL Server

---

## 🚀 STEG 1: Öppna Azure Portal Query Editor

1. Gå till **Azure Portal**: https://portal.azure.com

2. Sök efter **SQL databases** i sökfältet

3. Klicka på databasen: **HealthcareAnalyticsDB**

4. I vänstermenyn, under **"Query editor"**, klicka på **"Query editor (preview)"**

---

## 🔐 STEG 2: Logga in med Azure AD

1. I Query Editor login-dialogen, välj:
   - **Authentication type:** `Active Directory authentication`

2. Klicka **OK** för att logga in

3. Du ska nu se Query Editor med en tom frågeruta

---

## 📄 STEG 3: Kopiera SQL-scriptet

1. Öppna filen: `scripts/setup_sql_lineage.sql`

2. **Kopiera HELA innehållet** (Ctrl+A, Ctrl+C)

3. **Klistra in i Query Editor** (Ctrl+V)

---

## ▶️ STEG 4: Kör scriptet

1. Klicka på knappen **"Run"** högst upp i Query Editor

2. Du ska se output som:
   ```
   Master Key created successfully
   User prviewacc created successfully
   db_owner granted to prviewacc
   User mi-purview created successfully
   db_owner granted to mi-purview
   ```

3. Längst ner ska du se en tabell med:
   - **Database Users**: prviewacc och mi-purview
   - **Role Memberships**: Båda i db_owner rollen
   - **Master Key**: Exists

---

## ✅ STEG 5: Verifiera

Kör denna query för att dubbelkolla:

```sql
-- Kontrollera användare och roller
SELECT 
    dp.name as User_Name,
    dp.type_desc as User_Type,
    STRING_AGG(rp.name, ', ') as Roles
FROM sys.database_principals dp
LEFT JOIN sys.database_role_members rm ON dp.principal_id = rm.member_principal_id
LEFT JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
WHERE dp.name IN ('prviewacc', 'mi-purview')
GROUP BY dp.name, dp.type_desc;
```

**Förväntat resultat:**
```
User_Name    User_Type       Roles
prviewacc    EXTERNAL_USER   db_owner
mi-purview   EXTERNAL_USER   db_owner
```

---

## 🔧 OM DET GÅR FEL

### Fel: "Incorrect syntax near '`'"
**Lösning:** Du har kopierat PowerShell-kod istället för ren SQL. Använd `setup_sql_lineage.sql` filen.

### Fel: "Cannot find the user 'prviewacc'"
**Lösning:** Kontrollera att Managed Identity finns i Azure:
```powershell
az identity show --name prviewacc --resource-group <din-resource-group>
```

### Fel: "CREATE USER permission denied"
**Lösning:** Din användare saknar admin-rättigheter. Kontakta SQL Server admin.

### Fel: "Master Key already exists"
**Det är OK!** Scriptet hanterar detta automatiskt.

---

## 🎯 NÄSTA STEG

Efter SQL-setup är klar:

1. **Gå till Purview Portal:** https://purview.microsoft.com

2. **Data Map → Sources**

3. **Hitta din SQL Database** (eller registrera om den inte finns)

4. **Edit → Lineage**

5. **Enable "Lineage extraction"**

6. **Spara**

7. **Kör en ny scan** för att börja samla lineage data

---

## 📞 HJÄLP

Om problem kvarstår, kolla loggarna:
- Azure Portal → SQL Database → Query performance insight
- Purview Portal → Data Map → Sources → SQL Database → Scans → View details
