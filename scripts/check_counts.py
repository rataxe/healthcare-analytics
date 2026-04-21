"""Quick SQL row count check."""
import pyodbc, struct
from azure.identity import AzureCliCredential

cred = AzureCliCredential()
token = cred.get_token("https://database.windows.net/.default").token
token_bytes = token.encode("utf-16-le")
token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=sql-hca-demo.database.windows.net;"
    "DATABASE=HealthcareAnalyticsDB",
    attrs_before={1256: token_struct},
)
cursor = conn.cursor()
expected = {"patients": 10000, "encounters": 17292, "diagnoses": 30297, "vitals_labs": 48131, "medications": 60563}
for t, exp in expected.items():
    cursor.execute(f"SELECT COUNT(*) FROM hca.{t}")
    cnt = cursor.fetchone()[0]
    ok = "OK" if cnt == exp else "UPLOADING"
    print(f"hca.{t}: {cnt}/{exp} [{ok}]")
conn.close()
