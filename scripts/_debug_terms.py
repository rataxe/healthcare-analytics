"""Debug: check term names and test term GET API."""
import requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
tok = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

ATLAS = "https://prviewacc.purview.azure.com/catalog/api/atlas/v2"
GLOSSARY_GUID = "d939ea20-9c67-48af-98d9-b66965f7cde1"

# Get all terms
r = requests.get(f"{ATLAS}/glossary/{GLOSSARY_GUID}/terms?limit=200", headers=h, timeout=30)
terms = r.json()
print(f"Total terms: {len(terms)}")
for t in sorted(terms, key=lambda x: x["name"]):
    print(f"  {t['name']:45s} {t['guid']}")

# Test GET on first term (no api-version)
first = terms[0]
guid = first["guid"]
print(f"\n--- GET /glossary/terms/{guid} (no api-version) ---")
r2 = requests.get(f"{ATLAS}/glossary/terms/{guid}", headers=h, timeout=15)
print(f"Status: {r2.status_code}")
if r2.status_code == 200:
    data = r2.json()
    assigned = data.get("assignedEntities", [])
    print(f"Term: {data.get('name')}, assignedEntities: {len(assigned)}")
    for a in assigned[:3]:
        dtext = a.get("displayText", "?")
        aguid = a.get("guid", "?")[:20]
        print(f"  -> {dtext} ({aguid})")

# Test with api-version (the one that gave 404)
print(f"\n--- GET with api-version=2022-03-01-preview ---")
r3 = requests.get(f"{ATLAS}/glossary/terms/{guid}?api-version=2022-03-01-preview", headers=h, timeout=15)
print(f"Status: {r3.status_code}")

# Also check which of the expected terms are missing
expected = [
    "DICOMweb", "Personnummer", "Pseudonymisering", "Patientdemografi", "Vårdkontakt",
    "SBCR (Svenska Barncancerregistret)", "Histopatologi", "Biobank",
    "VCF (Variant Call Format)", "MR (Magnetresonanstomografi)",
    "T1-viktad MR", "T2-viktad MR", "FLAIR", "Tumörstadium",
    "Behandlingsprotokoll", "Informerat samtycke", "Etikprövning",
]
term_names = {t["name"] for t in terms}
print("\n--- Missing terms ---")
for e in expected:
    if e not in term_names:
        # Check for partial match
        matches = [n for n in term_names if e.lower() in n.lower() or n.lower() in e.lower()]
        if matches:
            print(f"  MISSING: {e}  (similar: {matches})")
        else:
            print(f"  MISSING: {e}")

# Check FHIR/DICOM entities and their collectionId from search
print("\n--- FHIR/DICOM entity collection membership ---")
SEARCH = "https://prviewacc.purview.azure.com/catalog/api/search/query?api-version=2022-08-01-preview"
for etype in ["healthcare_fhir_service", "healthcare_fhir_resource_type",
              "healthcare_dicom_service", "healthcare_dicom_modality"]:
    body = {"keywords": "*", "filter": {"entityType": etype}, "limit": 20}
    r4 = requests.post(SEARCH, headers=h, json=body, timeout=15)
    if r4.status_code == 200:
        for ent in r4.json().get("value", []):
            name = ent.get("name", "?")
            coll = ent.get("collectionId", "?")
            eid = ent.get("id", "?")[:20]
            print(f"  {etype:40s} {name:35s} coll={coll:20s} id={eid}")
