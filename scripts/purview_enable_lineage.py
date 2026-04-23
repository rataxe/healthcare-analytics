"""Enable lineage extraction on healthcare-scan:
1. Grant Purview MSI db_owner
2. CREATE MASTER KEY
3. Update scan with enableLineage=true
"""
import pyodbc, struct, requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)

# ── 1. SQL: Grant db_owner + CREATE MASTER KEY ──
print("1. SQL prerequisites")
print("=" * 50)
sql_token = cred.get_token("https://database.windows.net/.default")
tb = sql_token.token.encode("UTF-16-LE")
ts = struct.pack(f"<I{len(tb)}s", len(tb), tb)
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=sql-hca-demo.database.windows.net;"
    "DATABASE=HealthcareAnalyticsDB;",
    attrs_before={1256: ts},
)
conn.autocommit = True
c = conn.cursor()

# Check if Purview MSI user exists
try:
    c.execute("SELECT name FROM sys.database_principals WHERE name = 'prviewacc'")
    row = c.fetchone()
    if row:
        print(f"  ✅ User 'prviewacc' already exists")
    else:
        c.execute("CREATE USER [prviewacc] FROM EXTERNAL PROVIDER")
        print(f"  ✅ Created user 'prviewacc'")
except Exception as e:
    if "already exists" in str(e):
        print(f"  ✅ User 'prviewacc' already exists")
    else:
        print(f"  ⚠️ Create user: {e}")

# Grant db_owner
try:
    c.execute("ALTER ROLE db_owner ADD MEMBER [prviewacc]")
    print(f"  ✅ Granted db_owner to 'prviewacc'")
except Exception as e:
    if "already a member" in str(e).lower() or "already" in str(e).lower():
        print(f"  ✅ 'prviewacc' already has db_owner")
    else:
        print(f"  ⚠️ Grant db_owner: {e}")

# CREATE MASTER KEY
try:
    c.execute("IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##') CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'PurviewL1neage2026!'")
    print(f"  ✅ Master key ensured")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"  ✅ Master key already exists")
    else:
        print(f"  ⚠️ Master key: {e}")

conn.close()

# ── 2. Update scan with enableLineage=true ──
print(f"\n2. Update scan — enableLineage=true")
print("=" * 50)

SCAN_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
API_VER = "2023-09-01"
purview_token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {purview_token}", "Content-Type": "application/json"}

# Get current scan definition
url = f"{SCAN_EP}/scan/datasources/sql-hca-demo/scans/healthcare-scan?api-version={API_VER}"
r = requests.get(url, headers=h)
if r.status_code != 200:
    print(f"  ⚠️ GET scan failed: {r.status_code}")
    exit(1)

scan = r.json()
print(f"  Current enableLineage: {scan['properties'].get('enableLineage')}")

# Update with lineage enabled
scan["properties"]["enableLineage"] = True
r2 = requests.put(url, headers=h, json=scan)
if r2.status_code in (200, 201):
    print(f"  ✅ Scan updated — enableLineage=true ({r2.status_code})")
else:
    print(f"  ⚠️ Update failed: {r2.status_code} {r2.text[:300]}")
