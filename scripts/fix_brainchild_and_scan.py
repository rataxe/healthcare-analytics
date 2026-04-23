"""
Fix BrainChild entities & upload missing tables
=================================================
1. Move BrainChild entities from fabric-analytics → fabric-brainchild
2. Upload 7 missing tables to lh_brainchild in Fabric
3. Investigate Fabric scan exceptions (CompletedWithExceptions)
4. Re-run Fabric scan to pick up new tables
5. Create Data Products (now that domains exist)
"""
import csv
import io
import json
import os
import sys
import time

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import requests
from azure.identity import AzureCliCredential

cred = AzureCliCredential(process_timeout=30)
sess = requests.Session()

ACCT = "https://prviewacc.purview.azure.com"
TENANT_ID = "71c4b6d5-0065-4c6c-a125-841a582754eb"
SCAN_EP = f"{ACCT}/scan"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
DATAMAP = f"{ACCT}/datamap/api/atlas/v2"
SEARCH = f"{ACCT}/catalog/api/search/query?api-version=2022-08-01-preview"
SCAN_API = "2022-07-01-preview"
DG_API = "2025-09-15-preview"
DG_BASE = f"https://{TENANT_ID}-api.purview-service.microsoft.com/datagovernance/catalog"

BC_WS = "5c9b06e2-1c7f-4671-a902-46d0372bf0fd"
BC_LH_ID = "ffe48d2d-99d1-4fa1-9a2e-4de0079a3852"

# Synthetic data paths (relative to brainchild_synthetic_data)
SYNTH_ROOT = r"c:\code\brainchild-fhir-demo\brainchild_synthetic_data"

MISSING_TABLES = {
    "gene_sequence": os.path.join(SYNTH_ROOT, "omop", "genomics", "gene_sequence.csv"),
    "variant_occurrence": os.path.join(SYNTH_ROOT, "omop", "genomics", "variant_occurrence.csv"),
    "sbcr_registrations": os.path.join(SYNTH_ROOT, "sbcr", "registrations.csv"),
    "sbcr_treatments": os.path.join(SYNTH_ROOT, "sbcr", "treatments.csv"),
    "sbcr_followup": os.path.join(SYNTH_ROOT, "sbcr", "followup.csv"),
    "patients_master": os.path.join(SYNTH_ROOT, "patients", "patients_master.csv"),
    "specimen": os.path.join(SYNTH_ROOT, "omop", "specimen.csv"),
}

# ── Colors ──
B = "\033[94m"
G = "\033[92m"
Y = "\033[93m"
R = "\033[91m"
C = "\033[96m"
D = "\033[2m"
BOLD = "\033[1m"
RST = "\033[0m"

stats = {"ok": 0, "fixed": 0, "errors": 0, "skipped": 0}


def hdr(title):
    print(f"\n{BOLD}{B}{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}{RST}")


def ok(msg):
    stats["ok"] += 1
    print(f"  {G}OK{RST}    {msg}")


def fixed(msg):
    stats["fixed"] += 1
    print(f"  {G}FIXED{RST} {msg}")


def err(msg):
    stats["errors"] += 1
    print(f"  {R}ERR{RST}   {msg}")


def warn(msg):
    print(f"  {Y}WARN{RST}  {msg}")


def info(msg):
    print(f"  {D}INFO{RST}  {msg}")


_tokens = {}


def get_headers(scope="purview"):
    scope_url = {
        "purview": "https://purview.azure.net/.default",
        "fabric": "https://analysis.windows.net/powerbi/api/.default",
        "storage": "https://storage.azure.com/.default",
    }[scope]
    if scope not in _tokens or _tokens[scope][1] < time.time() - 2400:
        token = cred.get_token(scope_url)
        _tokens[scope] = (token.token, time.time())
    return {"Authorization": f"Bearer {_tokens[scope][0]}", "Content-Type": "application/json"}


# ══════════════════════════════════════════════════════════════════════
# 1. MOVE BRAINCHILD ENTITIES → fabric-brainchild COLLECTION
# ══════════════════════════════════════════════════════════════════════
def move_bc_entities():
    hdr("1. MOVE BRAINCHILD ENTITIES → fabric-brainchild")
    h = get_headers()

    # Find all fabric entities that belong to BrainChild workspace
    body = {"keywords": "*", "filter": {"entityType": "fabric_lakehouse_table"}, "limit": 100}
    r = sess.post(SEARCH, headers=h, json=body, timeout=30)
    if r.status_code != 200:
        err(f"Search failed: {r.status_code}")
        return

    bc_entities = []
    for ent in r.json().get("value", []):
        qname = ent.get("qualifiedName", "")
        if BC_WS in qname:
            bc_entities.append(ent)

    # Also find lakehouses
    body2 = {"keywords": "*", "filter": {"entityType": "fabric_lake_warehouse"}, "limit": 50}
    r2 = sess.post(SEARCH, headers=h, json=body2, timeout=30)
    if r2.status_code == 200:
        for ent in r2.json().get("value", []):
            qname = ent.get("qualifiedName", "")
            if BC_WS in qname:
                bc_entities.append(ent)

    info(f"Found {len(bc_entities)} BrainChild entities to move")

    moved = 0
    already = 0
    for ent in bc_entities:
        guid = ent["id"]
        name = ent.get("name", "?")
        etype = ent.get("entityType", "?")
        current_coll = ent.get("collectionId", "?")

        if current_coll == "fabric-brainchild":
            already += 1
            continue

        # Move entity to fabric-brainchild collection
        move_body = {"entityGuids": [guid]}
        r = sess.post(
            f"{ACCT}/account/collections/fabric-brainchild/entity?api-version=2019-11-01-preview",
            headers=h, json=move_body, timeout=30,
        )
        if r.status_code in (200, 204):
            fixed(f"{name} ({etype}) -> fabric-brainchild")
            moved += 1
        elif r.status_code == 404:
            # Collection might not exist — try via catalog API
            move_body2 = {
                "entities": [{
                    "guid": guid,
                    "collectionId": "fabric-brainchild",
                }]
            }
            r2 = sess.post(f"{ATLAS}/entity/moveEntitiesToCollection",
                           headers=h, json={"entityGuids": [guid], "collectionId": "fabric-brainchild"},
                           timeout=30)
            if r2.status_code in (200, 204):
                fixed(f"{name} ({etype}) -> fabric-brainchild (catalog API)")
                moved += 1
            else:
                err(f"Move {name}: {r.status_code} / {r2.status_code}")
        else:
            err(f"Move {name}: {r.status_code} - {r.text[:120]}")
        time.sleep(0.2)

    if already:
        info(f"{already} entities already in fabric-brainchild")
    info(f"Moved {moved} entities")


# ══════════════════════════════════════════════════════════════════════
# 2. UPLOAD MISSING TABLES TO FABRIC LAKEHOUSE
# ══════════════════════════════════════════════════════════════════════
def upload_missing_tables():
    hdr("2. UPLOAD 7 MISSING TABLES TO lh_brainchild")
    h_fabric = get_headers("fabric")
    h_storage = get_headers("storage")

    # Get OneLake endpoint from lakehouse properties
    r = sess.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{BC_WS}/lakehouses/{BC_LH_ID}",
        headers=h_fabric, timeout=30,
    )
    if r.status_code != 200:
        err(f"Cannot get lakehouse info: {r.status_code}")
        return

    lh_info = r.json()
    lh_name = lh_info.get("displayName", "lh_brainchild")
    info(f"Lakehouse: {lh_name} (id={BC_LH_ID})")

    # OneLake endpoint
    onelake_base = f"https://onelake.dfs.fabric.microsoft.com/{BC_WS}/{BC_LH_ID}"

    uploaded = 0
    for table_name, csv_path in MISSING_TABLES.items():
        if not os.path.exists(csv_path):
            err(f"Source file not found: {csv_path}")
            continue

        # Read CSV and check size
        file_size = os.path.getsize(csv_path)
        info(f"Uploading {table_name} ({file_size:,} bytes) from {os.path.basename(csv_path)}")

        # Upload as delta table via Fabric Tables API (load table)
        # Use the lakehouse table load API
        try:
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                columns = reader.fieldnames or []

            if not rows:
                warn(f"{table_name}: CSV is empty")
                continue

            info(f"  {len(rows)} rows, {len(columns)} columns: {', '.join(columns[:5])}...")

            # Step 1: Upload CSV to OneLake Files section
            file_path = f"Files/staging/{table_name}.csv"
            upload_url = f"{onelake_base}/{file_path}?resource=file"

            # Create the file
            r_create = sess.put(
                upload_url,
                headers={**h_storage, "Content-Length": "0"},
                timeout=30,
            )
            if r_create.status_code not in (200, 201):
                # Try creating the staging directory first
                dir_url = f"{onelake_base}/Files/staging?resource=directory"
                sess.put(dir_url, headers={**h_storage, "Content-Length": "0"}, timeout=30)
                r_create = sess.put(upload_url, headers={**h_storage, "Content-Length": "0"}, timeout=30)

            if r_create.status_code not in (200, 201):
                err(f"{table_name}: Cannot create file: {r_create.status_code} {r_create.text[:100]}")
                continue

            # Append data
            with open(csv_path, "rb") as f:
                data = f.read()

            append_url = f"{onelake_base}/{file_path}?position=0&action=append"
            r_append = sess.patch(
                append_url,
                headers={**h_storage, "Content-Type": "application/octet-stream", "Content-Length": str(len(data))},
                data=data,
                timeout=60,
            )
            if r_append.status_code not in (200, 202):
                err(f"{table_name}: Append failed: {r_append.status_code}")
                continue

            # Flush
            flush_url = f"{onelake_base}/{file_path}?position={len(data)}&action=flush"
            r_flush = sess.patch(flush_url, headers={**h_storage, "Content-Length": "0"}, timeout=30)
            if r_flush.status_code not in (200, 202):
                err(f"{table_name}: Flush failed: {r_flush.status_code}")
                continue

            info(f"  Uploaded to OneLake: {file_path}")

            # Step 2: Load table using Fabric Tables API
            load_url = (
                f"https://api.fabric.microsoft.com/v1/workspaces/{BC_WS}"
                f"/lakehouses/{BC_LH_ID}/tables/{table_name}/load"
            )
            load_body = {
                "relativePath": f"Files/staging/{table_name}.csv",
                "pathType": "File",
                "mode": "Overwrite",
                "formatOptions": {
                    "format": "Csv",
                    "header": True,
                    "delimiter": ",",
                },
            }
            r_load = sess.post(load_url, headers=h_fabric, json=load_body, timeout=120)
            if r_load.status_code in (200, 202):
                # Check if it's a long-running operation
                if r_load.status_code == 202:
                    op_url = r_load.headers.get("Location", "")
                    retry_after = int(r_load.headers.get("Retry-After", "5"))
                    info(f"  Table load in progress (LRO)...")
                    # Poll for completion
                    for _ in range(30):
                        time.sleep(retry_after)
                        if op_url:
                            r_poll = sess.get(op_url, headers=h_fabric, timeout=30)
                            if r_poll.status_code == 200:
                                status = r_poll.json().get("status", "")
                                if status in ("Succeeded", "Completed"):
                                    break
                                elif status == "Failed":
                                    err(f"  Table load failed: {r_poll.json()}")
                                    break
                            elif r_poll.status_code == 202:
                                continue
                            else:
                                break
                        else:
                            time.sleep(5)
                            break

                fixed(f"{table_name}: loaded as Delta table ({len(rows)} rows)")
                uploaded += 1
            else:
                err(f"{table_name}: Load failed: {r_load.status_code} {r_load.text[:200]}")

        except Exception as e:
            err(f"{table_name}: {e}")
        time.sleep(1)

    info(f"Uploaded {uploaded}/{len(MISSING_TABLES)} tables")


# ══════════════════════════════════════════════════════════════════════
# 3. INVESTIGATE FABRIC SCAN EXCEPTIONS
# ══════════════════════════════════════════════════════════════════════
def investigate_scan():
    hdr("3. INVESTIGATE FABRIC SCAN (CompletedWithExceptions)")
    h = get_headers()

    # Get scan runs for Fabric datasource
    r = sess.get(f"{SCAN_EP}/datasources/Fabric/scans?api-version={SCAN_API}", headers=h, timeout=30)
    if r.status_code != 200:
        err(f"Cannot list scans: {r.status_code}")
        return

    scans = r.json().get("value", [])
    for scan in scans:
        scan_name = scan["name"]
        info(f"Scan: {scan_name} (kind={scan.get('kind', '?')})")

        # Get scan properties
        props = scan.get("properties", {})
        for k, v in props.items():
            if v and k not in ("createdAt", "lastModifiedAt"):
                val_str = json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                if len(val_str) > 120:
                    val_str = val_str[:120] + "..."
                info(f"  {k}: {val_str}")

        # Get runs
        r2 = sess.get(f"{SCAN_EP}/datasources/Fabric/scans/{scan_name}/runs?api-version={SCAN_API}",
                       headers=h, timeout=30)
        if r2.status_code != 200:
            continue

        runs = r2.json().get("value", [])
        for run in runs[:3]:
            run_id = run.get("id", "?")
            status = run.get("status", "?")
            start = run.get("startTime", "?")
            end = run.get("endTime", "?")
            diag = run.get("diagnostics", {})
            error = run.get("error", {})

            color = G if status == "Succeeded" else R if "Fail" in status else Y
            print(f"\n    {color}Run: {status}{RST} | {start}")

            if diag:
                for dk, dv in diag.items():
                    if dv:
                        val = json.dumps(dv) if isinstance(dv, (dict, list)) else str(dv)
                        if len(val) > 200:
                            val = val[:200] + "..."
                        print(f"      diag.{dk}: {val}")

            if error:
                print(f"      {R}error: {json.dumps(error)[:300]}{RST}")

            # Get detailed run info
            r3 = sess.get(
                f"{SCAN_EP}/datasources/Fabric/scans/{scan_name}/runs/{run_id}?api-version={SCAN_API}",
                headers=h, timeout=30
            )
            if r3.status_code == 200:
                detail = r3.json()
                for dk in ["scanResultId", "assetsDiscovered", "assetsClassified"]:
                    if dk in detail:
                        print(f"      {dk}: {detail[dk]}")

            # Try to get scan result
            scan_result_id = run.get("scanResultId", run_id)
            r4 = sess.get(
                f"{SCAN_EP}/datasources/Fabric/scans/{scan_name}/runs/{run_id}/listScanLevelErrors?api-version={SCAN_API}",
                headers=h, timeout=30
            )
            if r4.status_code == 200:
                errors = r4.json()
                if errors:
                    print(f"      {R}Scan-level errors:{RST}")
                    if isinstance(errors, dict):
                        for ek, ev in errors.items():
                            if ev:
                                val = json.dumps(ev) if isinstance(ev, (dict, list)) else str(ev)
                                print(f"        {ek}: {val[:200]}")
                    elif isinstance(errors, list):
                        for e in errors[:5]:
                            print(f"        {e}")
        time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════
# 4. TRIGGER FABRIC RE-SCAN
# ══════════════════════════════════════════════════════════════════════
def trigger_rescan():
    hdr("4. TRIGGER FABRIC RE-SCAN")
    h = get_headers()

    # Find the existing Fabric scan
    r = sess.get(f"{SCAN_EP}/datasources/Fabric/scans?api-version={SCAN_API}", headers=h, timeout=30)
    if r.status_code != 200:
        err(f"Cannot list scans: {r.status_code}")
        return

    scans = r.json().get("value", [])
    if not scans:
        warn("No Fabric scans found")
        return

    scan_name = scans[0]["name"]
    info(f"Triggering scan: {scan_name}")

    r2 = sess.post(
        f"{SCAN_EP}/datasources/Fabric/scans/{scan_name}/runs?api-version={SCAN_API}",
        headers=h, json={}, timeout=30
    )
    if r2.status_code in (200, 201, 202):
        fixed(f"Scan triggered: {scan_name}")
        info("Scan may take 2-5 minutes to complete")
    else:
        err(f"Trigger scan failed: {r2.status_code} {r2.text[:200]}")


# ══════════════════════════════════════════════════════════════════════
# 5. CREATE DATA PRODUCTS (nu att domäner finns)
# ══════════════════════════════════════════════════════════════════════
def create_data_products():
    hdr("5. CREATE DATA PRODUCTS (11 st)")
    h = get_headers()

    # First, find domain GUIDs
    domain_guids = {}
    for base in [DG_BASE, f"{ACCT}/datagovernance/catalog"]:
        r = sess.get(f"{base}/domains?api-version={DG_API}", headers=h, timeout=30)
        if r.status_code == 200:
            for d in r.json().get("value", []):
                name = d.get("name", "")
                did = d.get("id") or d.get("guid")
                domain_guids[name] = did
                ok(f"Domain: {name} (id={str(did)[:20]}...)")
            break

    if not domain_guids:
        warn("Cannot find governance domains via API")
        warn("Trying alternative: search by expected names...")

        # Try to find domains by name mapping
        # User said they created "Klinisk Vård" and "Forskning"
        # Map to our expected names
        for base in [f"{ACCT}/datamap/api"]:
            for api_ver in ["2023-10-01-preview", "2023-02-01-preview"]:
                r = sess.get(f"{base}/governance-domains?api-version={api_ver}", headers=h, timeout=30)
                if r.status_code == 200:
                    for d in r.json().get("value", []):
                        name = d.get("name", "")
                        did = d.get("id") or d.get("guid")
                        domain_guids[name] = did
                    break
            if domain_guids:
                break

    if not domain_guids:
        warn("Governance domains API not accessible from REST")
        warn("Data products need domain IDs — will try portal-based domain names")
        warn("Please provide domain GUIDs manually or create data products via portal")
        return

    info(f"Found {len(domain_guids)} domains: {list(domain_guids.keys())}")

    # Map domain names (user might have named them slightly differently)
    # Expected: "Klinisk Vård", "Barncancerforskning"
    # User said: "Klinisk Vård", "Forskning"
    domain_name_map = {}
    for expected_name in ["Klinisk Vård", "Barncancerforskning"]:
        if expected_name in domain_guids:
            domain_name_map[expected_name] = domain_guids[expected_name]
        else:
            # Try partial match
            for actual_name, did in domain_guids.items():
                if (expected_name.lower() in actual_name.lower() or
                        actual_name.lower() in expected_name.lower() or
                        "forskning" in actual_name.lower() and "cancer" in expected_name.lower()):
                    domain_name_map[expected_name] = did
                    info(f"Mapped '{expected_name}' -> '{actual_name}' ({did})")
                    break

    # Get existing data products
    dp_guids = {}
    for base in [DG_BASE, f"{ACCT}/datagovernance/catalog"]:
        r = sess.get(f"{base}/dataProducts?api-version={DG_API}", headers=h, timeout=30)
        if r.status_code == 200:
            for p in r.json().get("value", []):
                dp_guids[p.get("name", "")] = p.get("id") or p.get("guid")
            break

    # Create data products
    ALL_PRODUCTS = {
        "Klinisk Vård": [
            {"name": "Patientdemografi", "description": "Demografisk patientdata. Kalla: hca.patients (SQL). Standard: FHIR Patient, OMOP Person."},
            {"name": "Vardbesok & utfall", "description": "Vardbesoksdata med LOS och aterinlaggningsrisk."},
            {"name": "Diagnoser (ICD-10)", "description": "Diagnosinformation klassificerad med ICD-10."},
            {"name": "Medicinering (ATC)", "description": "Lakemedelsdata klassificerad med ATC."},
            {"name": "Vitalparametrar & labb", "description": "Vitalparametrar och labresultat."},
            {"name": "ML-prediktion (LOS & readmission)", "description": "ML-modell for vardtid och aterinlaggningsprediktion."},
        ],
        "Barncancerforskning": [
            {"name": "FHIR Patientresurser", "description": "BrainChild FHIR R4-resurser: Patient, Encounter, Condition, Observation, Specimen."},
            {"name": "Medicinsk bilddiagnostik (DICOM)", "description": "MR-hjarna och patologidata i DICOM-format."},
            {"name": "Genomik (GMS/VCF)", "description": "Genomiska varianter i VCF-format och GMS DiagnosticReports."},
            {"name": "Biobanksdata (BTB)", "description": "Biobanksprover fran Barntumrbanken med SNOMED-kodning."},
            {"name": "Kvalitetsregister (SBCR)", "description": "Svenska BarncancerRegistret — registreringar, behandlingar, uppfoljning."},
        ],
    }

    created = 0
    for domain_name, products in ALL_PRODUCTS.items():
        domain_id = domain_name_map.get(domain_name)
        if not domain_id:
            warn(f"No domain ID for '{domain_name}' — skipping its {len(products)} data products")
            continue

        for product in products:
            if product["name"] in dp_guids:
                ok(f"Data product exists: {product['name']}")
                continue

            body = {
                "name": product["name"],
                "description": product["description"],
                "domainId": domain_id,
            }

            success = False
            for base in [DG_BASE, f"{ACCT}/datagovernance/catalog"]:
                r = sess.post(f"{base}/dataProducts?api-version={DG_API}", headers=h, json=body, timeout=30)
                if r.status_code in (200, 201):
                    dp_guids[product["name"]] = r.json().get("id") or r.json().get("guid")
                    fixed(f"Data product: {product['name']} -> {domain_name}")
                    created += 1
                    success = True
                    break
                elif r.status_code == 409:
                    ok(f"Already exists: {product['name']}")
                    success = True
                    break

            if not success:
                err(f"Cannot create '{product['name']}': {r.status_code} {r.text[:120]}")
            time.sleep(0.3)

    info(f"Created {created} data products")


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(f"\n{BOLD}{B}{'=' * 70}")
    print(f"  FIX BRAINCHILD ENTITIES & UPLOAD MISSING TABLES")
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 70}{RST}")

    # Step 1: Move BC entities to correct collection
    move_bc_entities()

    # Step 2: Upload 7 missing tables
    upload_missing_tables()

    # Step 3: Investigate scan exceptions
    investigate_scan()

    # Step 4: Trigger re-scan
    trigger_rescan()

    # Step 5: Create data products
    create_data_products()

    # Summary
    hdr("SUMMARY")
    print(f"  {G}Fixed:   {stats['fixed']}{RST}")
    print(f"  {R}Errors:  {stats['errors']}{RST}")
    print(f"  OK:      {stats['ok']}")
    print(f"\n  {D}After scan completes (~5 min), re-run purview_full_diagnostic.py --diag-only{RST}")
    print()
