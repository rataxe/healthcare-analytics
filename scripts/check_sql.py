"""Quick check of SQL table row counts."""
import struct
import pyodbc
from azure.identity import AzureCliCredential

SERVER = "sql-hca-demo.database.windows.net"
DATABASE = "HealthcareAnalyticsDB"

EXPECTED = {
    "hca.patients": 10000,
    "hca.encounters": 17292,
    "hca.diagnoses": 30297,
    "hca.vitals_labs": 48131,
    "hca.medications": 60563,
}

credential = AzureCliCredential()
token = credential.get_token("https://database.windows.net/.default")
token_bytes = token.token.encode("utf-16-le")
token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};",
    attrs_before={1256: token_struct},
)

cursor = conn.cursor()
all_ok = True
for table, expected in EXPECTED.items():
    row = cursor.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    actual = row[0]
    status = "✅" if actual >= expected else "❌"
    if actual < expected:
        all_ok = False
    print(f"  {status} {table}: {actual} / {expected}")

conn.close()
print(f"\nAll tables complete: {'YES ✅' if all_ok else 'NO — medications upload may still be running'}")
