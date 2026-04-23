"""
Create a glossary in the Fabric Purview domain (upiwjm) and map terms to Fabric assets.
The existing 'Kund' glossary is in domain 'prviewacc' which can't cross-reference Fabric assets.
"""
import requests, json, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
token = cred.get_token("https://purview.azure.net/.default").token
h = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))

TENANT_EP = "https://71c4b6d5-0065-4c6c-a125-841a582754eb-api.purview-service.microsoft.com"
ATLAS_EP = f"{TENANT_EP}/catalog/api/atlas/v2"
SEARCH_EP = f"{TENANT_EP}/catalog/api/search/query?api-version=2022-08-01-preview"

# ── 1. Check governance domains ──
print("=" * 70)
print("1. Checking governance domains")
print("=" * 70)

# Check if we can list domains
for path in ["/catalog/api/atlas/v2/types/typedefs", "/datamap/api/domains?api-version=2023-09-01"]:
    r = sess.get(f"{TENANT_EP}{path}", headers=h, timeout=30)
    # Just check status
    # print(f"  {path}: {r.status_code}")

# Check the bronze lakehouse entity to understand domain structure
r = sess.post(SEARCH_EP, headers=h, json={"keywords": "bronze_lakehouse", "limit": 3}, timeout=30)
for a in r.json().get("value", []):
    if a.get("entityType") == "fabric_lake_warehouse" and "bronze" in a.get("name", "").lower():
        lh_guid = a["id"]
        print(f"  bronze_lakehouse GUID: {lh_guid}")
        print(f"  collectionId: {a.get('collectionId')}")
        
        # Get full entity details
        r2 = sess.get(f"{ATLAS_EP}/entity/guid/{lh_guid}?minExtInfo=true", headers=h, timeout=30)
        if r2.status_code == 200:
            ent = r2.json().get("entity", {})
            print(f"  domainId: {ent.get('domainId', 'none')}")
            print(f"  collectionId: {ent.get('collectionId', 'none')}")
            attrs = ent.get("attributes", {})
            print(f"  qualifiedName: {attrs.get('qualifiedName', 'none')[:100]}")
        break

# ── 2. Create a new glossary with the Fabric domain ──
print(f"\n{'=' * 70}")
print("2. Creating Fabric-domain glossary")
print("=" * 70)

# First check existing glossaries
r = sess.get(f"{ATLAS_EP}/glossary", headers=h, timeout=30)
glossaries = r.json() if r.status_code == 200 else []
print(f"  Existing glossaries: {len(glossaries)}")
for g in glossaries:
    print(f"    {g.get('name')} ({g.get('guid', '?')[:12]}...) domainId={g.get('domainId', '?')}")

# Try creating a new glossary in the upiwjm domain  
fabric_glossary_guid = None
for g in glossaries:
    if g.get("name") == "Fabric Assets":
        fabric_glossary_guid = g["guid"]
        print(f"\n  ✅ 'Fabric Assets' glossary already exists: {fabric_glossary_guid[:12]}...")
        break

if not fabric_glossary_guid:
    # Try to create with explicit domain
    body = {
        "name": "Fabric Assets", 
        "shortDescription": "Business glossary för Fabric-tillgångar (lakehouses, notebooks, pipelines)",
        "longDescription": "Affärstermer mappade till Microsoft Fabric-resurser i Healthcare-Analytics och BrainChild-Demo",
    }
    r = sess.post(f"{ATLAS_EP}/glossary", headers=h, json=body, timeout=30)
    print(f"\n  Create glossary: {r.status_code}")
    if r.status_code in (200, 201):
        fabric_glossary_guid = r.json().get("guid")
        print(f"  ✅ Created: {fabric_glossary_guid[:12]}...")
        print(f"  Domain: {r.json().get('domainId', '?')}")
    else:
        print(f"  Error: {r.text[:400]}")

if not fabric_glossary_guid:
    print("  ❌ Could not create Fabric glossary")
    exit(1)

# Check domain of new glossary
r = sess.get(f"{ATLAS_EP}/glossary/{fabric_glossary_guid}", headers=h, timeout=30)
if r.status_code == 200:
    gdata = r.json()
    print(f"  Glossary domain: {gdata.get('domainId', 'none')}")
time.sleep(0.5)

# ── 3. Collect Fabric asset GUIDs ──
print(f"\n{'=' * 70}")
print("3. Collecting Fabric asset GUIDs")
print("=" * 70)

fabric_assets = {}
for kw in ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse", "gold_omop", "lh_brainchild"]:
    r = sess.post(SEARCH_EP, headers=h, json={"keywords": kw, "limit": 5}, timeout=30)
    for a in r.json().get("value", []):
        if a.get("entityType") == "fabric_lake_warehouse" and kw.replace("_", "") in a["name"].replace("_", "").lower():
            fabric_assets[kw] = a["id"]
            print(f"  ✅ {a['name']} ({a['id'][:12]}...)")
            break
    time.sleep(0.3)

# ── 4. Create terms in Fabric glossary and assign to Fabric assets ──
print(f"\n{'=' * 70}")
print("4. Creating terms and assigning to Fabric assets")
print("=" * 70)

FABRIC_TERM_MAPPINGS = [
    # (term_name, short_desc, target_asset_keys)
    ("Bronze Layer", "Rådata-lager i Medallion-arkitekturen. Ingestion utan transformation.", ["bronze_lakehouse"]),
    ("Silver Layer", "Rensat och berikat datalager. Feature engineering och validering.", ["silver_lakehouse"]),
    ("Gold Layer", "Aggregerat och ML-klart datalager. Slutlig analytisk vy.", ["gold_lakehouse", "gold_omop"]),
    ("OMOP CDM", "OMOP Common Data Model v5.4 – standardiserad klinisk datamodell.", ["gold_omop"]),
    ("FHIR R4", "HL7 FHIR R4 – standardformat för hälso- och sjukvårdsdata.", ["lh_brainchild"]),
    ("DICOM", "Digital Imaging and Communications in Medicine – bildformat för radiologi.", ["lh_brainchild"]),
    ("Genomic Medicine Sweden", "GMS – nationellt program för genomisk medicin i Sverige.", ["lh_brainchild"]),
    ("Protected Health Information", "PHI – känsliga personuppgifter inom hälso- och sjukvård.", ["lh_brainchild"]),
    ("Medallion Architecture", "Medallion-arkitektur (Bronze/Silver/Gold) för datalakehouse.", ["bronze_lakehouse", "silver_lakehouse", "gold_lakehouse"]),
    ("FHIR ImagingStudy", "HL7 FHIR R4 ImagingStudy – radiologisk studie med DICOM.", ["lh_brainchild"]),
    ("FHIR Specimen", "HL7 FHIR R4 Specimen – biologiskt prov/vävnad.", ["lh_brainchild"]),
    ("Genomic Variant", "Genomisk variant – DNA-sekvensförändring identifierad via VCF.", ["lh_brainchild"]),
    ("SBCR", "Svenska BarnCancerRegistret – nationellt kvalitetsregister.", ["lh_brainchild"]),
    ("BTB", "BioBank – biologiska prover och metadata.", ["lh_brainchild"]),
    ("VCF", "Variant Call Format – standardformat för genomiska varianter.", ["lh_brainchild"]),
    ("OMOP Person", "OMOP CDM Person-tabell – demografisk information.", ["gold_omop"]),
    ("OMOP Visit Occurrence", "OMOP CDM Visit Occurrence – standardiserad vårdkontakt.", ["gold_omop"]),
    ("OMOP Condition Occurrence", "OMOP CDM Condition Occurrence – standardiserad diagnos.", ["gold_omop"]),
    ("OMOP Drug Exposure", "OMOP CDM Drug Exposure – standardiserad läkemedelsexponering.", ["gold_omop"]),
    ("OMOP Measurement", "OMOP CDM Measurement – standardiserad mätning.", ["gold_omop"]),
]

success_count = 0
for term_name, short_desc, target_keys in FABRIC_TERM_MAPPINGS:
    # Create term
    body = {
        "name": term_name,
        "shortDescription": short_desc,
        "anchor": {"glossaryGuid": fabric_glossary_guid},
    }
    r = sess.post(f"{ATLAS_EP}/glossary/term", headers=h, json=body, timeout=30)
    
    term_guid = None
    if r.status_code in (200, 201):
        term_guid = r.json().get("guid")
    elif "already exists" in r.text.lower() or r.status_code == 409:
        # Term already exists — find its GUID
        r2 = sess.get(f"{ATLAS_EP}/glossary/{fabric_glossary_guid}/terms?limit=100", headers=h, timeout=30)
        if r2.status_code == 200:
            for t in r2.json():
                if t.get("name") == term_name:
                    term_guid = t["guid"]
                    break
    
    if not term_guid:
        print(f"  ⚠️ '{term_name}': Could not create/find ({r.status_code} {r.text[:80]})")
        time.sleep(0.3)
        continue
    
    # Assign to Fabric assets
    target_guids = [fabric_assets[k] for k in target_keys if k in fabric_assets]
    if not target_guids:
        print(f"  ⏭️ '{term_name}': No target assets found")
        continue
    
    # Check already assigned
    r3 = sess.get(f"{ATLAS_EP}/glossary/term/{term_guid}", headers=h, timeout=30)
    already = set()
    if r3.status_code == 200:
        for ae in r3.json().get("assignedEntities", []):
            already.add(ae.get("guid"))
    
    new_guids = [g for g in target_guids if g not in already]
    if not new_guids:
        print(f"  ✅ '{term_name}' -> already mapped")
        success_count += 1
        continue
    
    assign_body = [{"guid": g} for g in new_guids]
    r4 = sess.post(
        f"{ATLAS_EP}/glossary/terms/{term_guid}/assignedEntities",
        headers=h, json=assign_body, timeout=30,
    )
    if r4.status_code in (200, 204):
        names = [k for k in target_keys if k in fabric_assets]
        print(f"  ✅ '{term_name}' -> {', '.join(names)} ({len(new_guids)} new)")
        success_count += 1
    else:
        print(f"  ⚠️ '{term_name}': assign {r4.status_code} {r4.text[:200]}")
    
    time.sleep(0.3)

# ── 5. Summary ──
print(f"\n{'=' * 70}")
print(f"5. Summary: {success_count}/{len(FABRIC_TERM_MAPPINGS)} term mappings")
print("=" * 70)

r = sess.get(f"{ATLAS_EP}/glossary/{fabric_glossary_guid}/terms?limit=100", headers=h, timeout=30)
if r.status_code == 200:
    for t in sorted(r.json(), key=lambda x: x.get("name", "")):
        name = t.get("name")
        assigned = t.get("assignedEntities", [])
        if assigned:
            entities = [a.get("displayText", "?") for a in assigned]
            print(f"  ✅ {name} -> {', '.join(entities)}")
        else:
            print(f"  ⬜ {name} (unmapped)")
