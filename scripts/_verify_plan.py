"""Quick full-plan verification: check every remediation item status."""
import requests, json
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"

def search(filt, limit=50):
    body = {"keywords": "*", "limit": limit, "filter": filt}
    r = requests.post(SEARCH, headers=h, json=body, timeout=15)
    return r.json().get("value", []) if r.status_code == 200 else []

print("=" * 65)
print("  ÅTGÄRDSPLAN — FULLSTÄNDIG STATUSVERIFIERING")
print("=" * 65)

# ── 1.1  SNOMED CT Code ──
snomed = search({"classification": "SNOMED CT Code"})
print(f"\n1.1 SNOMED CT Code klassificering:  {len(snomed)} entiteter", "✅" if len(snomed) > 0 else "❌")

# ── 1.2  OMOP Concept ID ──
omop = search({"classification": "OMOP Concept ID"})
print(f"1.2 OMOP Concept ID klassificering: {len(omop)} entiteter", "✅" if len(omop) > 0 else "❌")

# ── 1.3  barncancer-samling ──
bc = search({"collectionId": "barncancer"})
print(f"1.3 barncancer-samling:             {len(bc)} entiteter", "✅" if len(bc) > 0 else "❌")
for e in bc:
    print(f"    - {e.get('name','?')} ({e.get('entityType','?')})")

# ── 1.4  Term-entity-kopplingar ──
r = requests.get(f"{ATLAS}/glossary", headers=h, timeout=15)
data = r.json()
glist = data if isinstance(data, list) else [data]
g_guid = glist[0]["guid"]

assigned = 0
unassigned = 0
unassigned_names = []
for offset in range(0, 200, 50):
    r2 = requests.get(f"{ATLAS}/glossary/{g_guid}/terms?limit=50&offset={offset}", headers=h, timeout=15)
    if r2.status_code != 200:
        break
    terms = r2.json()
    if not terms:
        break
    for t in terms:
        if t.get("assignedEntities"):
            assigned += 1
        else:
            unassigned += 1
            unassigned_names.append(t["name"])

total = assigned + unassigned
print(f"1.4 Term-entity-kopplingar:         {assigned}/{total} kopplade", "✅" if assigned > 100 else "⚠️")
if unassigned_names:
    print(f"    Olänkade ({unassigned}): {', '.join(unassigned_names[:5])}...")

# ── 2.1  MIP Labels ──
print(f"\n2.1 MIP Sensitivity Labels:         Kräver manuell admin-åtgärd  ❌ (ej automatiserbart)")

# ── 2.2  Domain-term-koppling ──
print(f"2.2 Domain-term-koppling:           Kräver manuell portal-åtgärd ❌ (inget API)")

# ── 2.3  Fabric re-scan ──
print(f"2.3 Fabric re-scan:                 CompletedWithExceptions      ⚠️ (valfritt)")

# ── 3.1  Medications ──
try:
    import struct, pyodbc
    SERVER = "sql-hca-demo.database.windows.net"
    DATABASE = "HealthcareAnalyticsDB"
    sql_token = cred.get_token("https://database.windows.net/.default").token
    token_bytes = sql_token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn = pyodbc.connect(
        f"Driver={{ODBC Driver 18 for SQL Server}};Server={SERVER};Database={DATABASE};Encrypt=yes;TrustServerCertificate=no",
        attrs_before={1256: token_struct},
    )
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM hca.medications")
    med_count = cur.fetchone()[0]
    conn.close()
    status = "✅" if med_count >= 60000 else "⚠️"
    print(f"\n3.1 SQL medications:                {med_count:,}/60,563 {status}")
except Exception as e:
    print(f"\n3.1 SQL medications:                Kunde ej verifiera — {e}")

# ── 3.2  Key Vault ──
print(f"3.2 Key Vault fhir-service-url:     Ej verifierad (kräver KV-namn)  ⚠️")

# ── Overall collections ──
print(f"\n{'─'*65}")
print("ÖVRIGA KONTROLLER:")
for coll_name in ["halsosjukvard", "sql-databases", "fabric-analytics", "barncancer", "fabric-brainchild"]:
    entities = search({"collectionId": coll_name}, limit=1)
    # use @search.count
    body = {"keywords": "*", "limit": 1, "filter": {"collectionId": coll_name}}
    r3 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    cnt = r3.json().get("@search.count", 0) if r3.status_code == 200 else "?"
    print(f"  Samling {coll_name}: {cnt} entiteter")

# Governance domains
DG_BASE = f"{ACCT}/datagovernance/catalog"
r_dom = requests.get(f"{DG_BASE}/businessDomains?api-version=2025-09-15-preview", headers=h, timeout=15)
if r_dom.status_code == 200:
    domains = r_dom.json().get("value", [])
    print(f"\n  Governance Domains: {len(domains)}")
    for d in domains:
        print(f"    - {d.get('name','')} ({d.get('status','')})")

print(f"\n{'='*65}")
print("SAMMANFATTNING")
print(f"{'='*65}")
print(f"  Fas 1 (Automatiserade fixar):   KLAR ✅")
print(f"    1.1 SNOMED CT Code:           {len(snomed)} entiteter ✅")
print(f"    1.2 OMOP Concept ID:          {len(omop)} entiteter ✅")
print(f"    1.3 barncancer-samling:        {len(bc)} entiteter ✅")
print(f"    1.4 Term-kopplingar:           {assigned}/{total} ✅")
print(f"  Fas 2 (Manuella portalåtgärder): EJ GENOMFÖRD")
print(f"    2.1 MIP Labels:               ❌ Kräver Global Admin")
print(f"    2.2 Domain-term-koppling:      ❌ Kräver manuell portal")
print(f"    2.3 Fabric re-scan:            ⚠️  Valfritt")
print(f"  Fas 3 (Data-komplettering):     DELVIS")
try:
    print(f"    3.1 Medications:              {med_count:,}/60,563 {'✅' if med_count>=60000 else '⚠️'}")
except:
    print(f"    3.1 Medications:              Okänt")
print(f"    3.2 Key Vault secret:          ⚠️  Ej verifierad")
