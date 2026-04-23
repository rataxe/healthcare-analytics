#!/usr/bin/env python3
"""
Create Missing Glossary Terms for Data Products
Adds all terms needed for data product documentation
"""

import requests
import json
from azure.identity import AzureCliCredential

# Configuration
PURVIEW_ACCOUNT = "prviewacc"
PURVIEW_ENDPOINT = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"
ATLAS_API = f"{PURVIEW_ENDPOINT}/catalog/api/atlas/v2"

# Missing terms organized by category
MISSING_TERMS = {
    "Klinisk Data": [
        {
            "name": "Patient",
            "shortDescription": "En person som tar emot eller har tagit emot hälso- och sjukvård",
            "longDescription": "Patient-resurs i FHIR R4 som innehåller demografisk information, kontaktuppgifter, och administrativa detaljer för en individ som mottar vård."
        },
        {
            "name": "Encounter",
            "shortDescription": "Ett vårdtillfälle eller interaktion mellan patient och vårdgivare",
            "longDescription": "Omfattar sjukhusvistelser, öppenvårdsbesök, akutmottagning, och andra vårdkontakter. Inkluderar datum, avdelning, behandlande läkare och vårdnivå."
        },
        {
            "name": "Condition",
            "shortDescription": "En klinisk diagnos, problem eller hälsotillstånd",
            "longDescription": "Dokumenterad diagnos enligt ICD-10 eller SNOMED CT, med onset-datum, svårighetsgrad och klinisk status (active/resolved)."
        },
        {
            "name": "Medication",
            "shortDescription": "Ett läkemedel eller farmaceutisk produkt",
            "longDescription": "Läkemedelsordinationer och administreringar kodade enligt ATC-systemet, med dos, frekvens, administreringsväg och behandlingsperiod."
        },
        {
            "name": "Observation",
            "shortDescription": "Kliniska observationer, mätningar och bedömningar",
            "longDescription": "Vitala parametrar, laboratorieresultat, fysiska undersökningar och andra kliniska observationer kodade med LOINC eller SNOMED CT."
        },
        {
            "name": "Practitioner",
            "shortDescription": "Legitimerad vårdpersonal",
            "longDescription": "Läkare, sjuksköterskor och annan vårdpersonal med formella kvalifikationer. Inkluderar namn, legitimationsnummer, specialitet och arbetsplats."
        },
        {
            "name": "DICOM Study",
            "shortDescription": "En radiologisk undersökning i DICOM-format",
            "longDescription": "En samling medicinska bilder från en specifik undersökning (CT, MRI, röntgen, ultraljud). Inkluderar metadata om modalitet, kroppsdel, datum och referrerande läkare."
        },
        {
            "name": "Lab Result",
            "shortDescription": "Laboratorieprovsvar",
            "longDescription": "Svar från klinisk kemi, hematologi, mikrobiologi eller patologi. Innehåller analysvärde, enhet, referensintervall och flaggor för avvikande resultat."
        },
        {
            "name": "Vital Signs",
            "shortDescription": "Vitala parametrar (puls, blodtryck, temperatur, andning, saturation)",
            "longDescription": "Kontinuerliga eller periodiska mätningar av vitala funktioner. Standardiserade LOINC-koder för interoperabilitet mellan system."
        },
        {
            "name": "Radiology Order",
            "shortDescription": "Remiss för radiologisk undersökning",
            "longDescription": "Beställning av bilddiagnostik med klinisk frågeställning, indikation och prioritet. Länkas till DICOM Study när undersökningen är genomförd."
        },
        {
            "name": "Discharge Summary",
            "shortDescription": "Utskrivningssammanfattning från vårdtillfälle",
            "longDescription": "Epikris som sammanfattar vårdförlopp, diagnoser, behandlingar, prover och fortsatt vårdplan vid utskrivning från slutenvård."
        },
    ],
    "Barncancerforskning": [
        {
            "name": "VCF",
            "shortDescription": "Variant Call Format - standardformat för genomiska varianter",
            "longDescription": "Textbaserat filformat för DNA-sekvenseringsvarianter. Innehåller kromosomposition, referensallel, alternativa alleler, kvalitetsscore och funktionella annnoteringar."
        },
        {
            "name": "Genomic Variant",
            "shortDescription": "En genetisk variation i DNA-sekvens",
            "longDescription": "SNV (single nucleotide variant), indel, CNV eller strukturell variant identifierad genom DNA-sekvensering. Annoterad med klinisk signifikans och populationsfrekvens."
        },
        {
            "name": "DNA Sequence",
            "shortDescription": "DNA-sekvensdata från NGS",
            "longDescription": "Rådata (FASTQ) eller alignad sekvensering (BAM/CRAM) från whole-genome, whole-exome eller targeted panel sequencing."
        },
        {
            "name": "Tumor Sample",
            "shortDescription": "Tumörvävnadsprov för molekylär analys",
            "longDescription": "Biopsi eller kirurgiskt prov från tumör för DNA/RNA-sekvensering, immunhistokemi eller andra molekylära analyser. Inkluderar tumörtyp, grad och samlingsdatum."
        },
        {
            "name": "Specimen",
            "shortDescription": "Biologiskt prov för analys",
            "longDescription": "FHIR Specimen-resurs för blod, vävnad, saliv eller annat biologiskt material. Metadata om provtagning, förvaring och kvalitet enligt biobanksstandard."
        },
        {
            "name": "NGS",
            "shortDescription": "Next-Generation Sequencing",
            "longDescription": "Moderna DNA-sekvenseringsmetoder (Illumina, PacBio, Oxford Nanopore) för massiv parallell sekvensering av genom eller transkriptom."
        },
        {
            "name": "BrainChild",
            "shortDescription": "Barncancerforskningsprojekt för hjärntumörer",
            "longDescription": "Forskningsprogram för pediatrisk hjärntumör med fokus på genetisk karakterisering, precisionsmedicin och longitudinell uppföljning av behandlingsrespons."
        },
        {
            "name": "Copy Number Variation",
            "shortDescription": "CNV - förändringar i antal kopior av DNA-segment",
            "longDescription": "Deletioner eller duplikationer av DNA-regioner, identifierade genom NGS eller mikroarray. Kan ha klinisk betydelse för cancerdiagnostik och prognos."
        },
        {
            "name": "Structural Variant",
            "shortDescription": "Stora genomiska omlagringar",
            "longDescription": "Translokationer, inversioner, stora deletioner/insertioner (>50 bp) som påverkar genomstruktur. Detekteras med long-read sequencing eller paired-end NGS."
        },
        {
            "name": "Mutation",
            "shortDescription": "Genetisk förändring i DNA",
            "longDescription": "Hereditär (germline) eller förvärvad (somatisk) DNA-förändring. Klassificeras enligt ACMG-riktlinjer som patogen, benign eller VUS (variant of uncertain significance)."
        },
        {
            "name": "Germline Variant",
            "shortDescription": "Ärftlig genetisk variant",
            "longDescription": "Variant närvarande i alla kroppsceller, ärvd från föräldrar. Relevant för cancer predispositionssyndrom och familjär riskbedömning."
        },
        {
            "name": "Somatic Variant",
            "shortDescription": "Förvärvad mutation i tumör",
            "longDescription": "Mutation som uppstår i tumörceller men inte finns i normala celler. Driver tumörutveckling och kan vara behandlingsbar med targeted therapy."
        },
    ],
    "ML & Prediktioner": [
        {
            "name": "ML Feature",
            "shortDescription": "En variabel eller attribut i maskininlärningsmodell",
            "longDescription": "Pre-computed feature från patient-data (demografi, vitala, lab, diagnoser). Standardiserad, versionerad och dokumenterad för reproducerbarhet."
        },
        {
            "name": "ML Model",
            "shortDescription": "Tränad maskininlärningsmodell",
            "longDescription": "Prediktiv modell (regression, klassificering, neural network) för kliniska utfall. Inkluderar performance metrics, feature importance och fairness-utvärdering."
        },
        {
            "name": "MLflow Model",
            "shortDescription": "Modell registrerad i MLflow",
            "longDescription": "Versionerad modell med metadata, träningsparametrar, dependencies och deployment-artifacts. Stödjer A/B-testning och automated retraining."
        },
        {
            "name": "Model Registry",
            "shortDescription": "Central repository för ML-modeller",
            "longDescription": "MLflow Model Registry med versionhantering, staging/production-miljöer, godkännandeflöden och modellövervakning för governance."
        },
        {
            "name": "Batch Scoring",
            "shortDescription": "Batch-prediktion på stora dataset",
            "longDescription": "Offline-inferens för att generera prediktioner på många patienter samtidigt. Används för populationshälsoanalyser och riskstratifiering."
        },
        {
            "name": "Prediction",
            "shortDescription": "Modellgenererad prediktion eller riskscore",
            "longDescription": "Output från ML-modell (sannolikhet, riskklass, numerisk score). Loggas med confidence interval, modellversion och input-features för auditability."
        },
        {
            "name": "Risk Score",
            "shortDescription": "Numerisk riskvärdering för kliniskt utfall",
            "longDescription": "Prognostisk score (0-100%) för t.ex. 30-dagars readmission, mortalitet eller komplikationer. Kalibrerad mot lokala data för validitet."
        },
        {
            "name": "Feature Drift",
            "shortDescription": "Förändring i feature-distribution över tid",
            "longDescription": "Statistisk drift som indikerar att input-data har förändrats jämfört med träningsdata. Kräver modell-retraining eller recalibration."
        },
        {
            "name": "Model Monitoring",
            "shortDescription": "Övervakning av modellprestanda i produktion",
            "longDescription": "Kontinuerlig tracking av accuracy, fairness, latency och drift. Automatiska alerts vid försämrad performance eller bias-detektering."
        },
    ],
    "Kliniska Standarder": [
        {
            "name": "OMOP Concept",
            "shortDescription": "Standardiserat begrepp i OMOP CDM-vokabulär",
            "longDescription": "Unikt concept_id som mappar kliniska termer till standardvokabulär (SNOMED CT, RxNorm, LOINC). Möjliggör interoperabilitet mellan institutioner."
        },
        {
            "name": "Condition Occurrence",
            "shortDescription": "OMOP-tabell för diagnoser och hälsotillstånd",
            "longDescription": "Dokumenterade diagnoser med SNOMED CT concept_id, onset/slut-datum, och länk till vårdtillfälle. Primära och sekundära diagnoser separerade."
        },
        {
            "name": "Drug Exposure",
            "shortDescription": "OMOP-tabell för läkemedelsexponeringar",
            "longDescription": "Ordinerade och administrerade läkemedel kodade med RxNorm. Inkluderar dos, administreringsväg, start/stopp-datum och prescriber."
        },
        {
            "name": "Measurement",
            "shortDescription": "OMOP-tabell för laboratorieprover och mätningar",
            "longDescription": "Lab-resultat, vitala parametrar och andra kvantitativa observationer med LOINC-koder. Värde, enhet, referensintervall och abnormal-flaggor."
        },
        {
            "name": "Visit Occurrence",
            "shortDescription": "OMOP-tabell för vårdtillfällen",
            "longDescription": "Sjukhusvistelser, poliklinikbesök, akutmottagning med admit/discharge-datum, visit_concept_id (inpatient/outpatient/ER) och caring_site."
        },
        {
            "name": "Cohort",
            "shortDescription": "Definierad patientgrupp för forskning",
            "longDescription": "Urval av patienter baserat på inklusionskriterier (diagnoser, procedurer, lab-värden, ålder). Används för epidemiologiska studier och kliniska trials."
        },
        {
            "name": "De-identification",
            "shortDescription": "Avidentifiering av personuppgifter för forskningsändamål",
            "longDescription": "Borttagning eller anonymisering av PII enligt GDPR/HIPAA. Tekniker: date-shifting, k-anonymity, differential privacy. Behåller analytisk nytta."
        },
    ],
}


def get_auth_headers():
    """Get authentication headers for Purview API"""
    cred = AzureCliCredential(process_timeout=60)
    token = cred.get_token("https://purview.azure.net/.default").token
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def get_glossary_guid():
    """Get the GUID of the main glossary"""
    headers = get_auth_headers()
    response = requests.get(f"{ATLAS_API}/glossary", headers=headers, timeout=30)
    response.raise_for_status()
    
    glossaries = response.json()
    if isinstance(glossaries, list):
        return glossaries[0]["guid"]
    return glossaries["guid"]


def get_category_guid(glossary_guid, category_name):
    """Get GUID for a specific category"""
    headers = get_auth_headers()
    response = requests.get(f"{ATLAS_API}/glossary/{glossary_guid}", headers=headers, timeout=30)
    response.raise_for_status()
    
    data = response.json()
    categories = data.get("categories", [])
    
    for cat in categories:
        if category_name.lower() in cat.get("displayText", "").lower():
            return cat["categoryGuid"]
    
    return None


def create_glossary_term(glossary_guid, category_guid, term_data):
    """Create a glossary term"""
    headers = get_auth_headers()
    
    payload = {
        "name": term_data["name"],
        "shortDescription": term_data["shortDescription"],
        "longDescription": term_data.get("longDescription", term_data["shortDescription"]),
        "anchor": {
            "glossaryGuid": glossary_guid
        }
    }
    
    if category_guid:
        payload["categories"] = [{"categoryGuid": category_guid}]
    
    try:
        response = requests.post(
            f"{ATLAS_API}/glossary/term",
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            return True, None
        else:
            return False, f"HTTP {response.status_code}: {response.text[:100]}"
            
    except Exception as e:
        return False, str(e)


def main():
    print("=" * 70)
    print("📝 CREATE MISSING GLOSSARY TERMS FOR DATA PRODUCTS")
    print("=" * 70)
    print(f"Purview Account: {PURVIEW_ACCOUNT}")
    
    # Count total terms
    total_terms = sum(len(terms) for terms in MISSING_TERMS.values())
    print(f"Terms to create: {total_terms}")
    print()
    
    try:
        # Get glossary GUID
        print("📚 Step 1: Getting glossary GUID...")
        glossary_guid = get_glossary_guid()
        print(f"   Glossary GUID: {glossary_guid}\n")
        
        # Process each category
        success_count = 0
        failed_count = 0
        
        for category_name, terms in MISSING_TERMS.items():
            print(f"📂 Category: {category_name}")
            print(f"   Terms: {len(terms)}")
            
            # Get category GUID
            category_guid = get_category_guid(glossary_guid, category_name)
            
            if category_guid:
                print(f"   Category GUID: {category_guid}")
            else:
                print(f"   ⚠️  Category not found, creating terms without category")
            
            # Create each term
            for term_data in terms:
                term_name = term_data["name"]
                success, error = create_glossary_term(glossary_guid, category_guid, term_data)
                
                if success:
                    print(f"      ✅ {term_name}")
                    success_count += 1
                else:
                    print(f"      ❌ {term_name}: {error}")
                    failed_count += 1
            
            print()
        
        # Summary
        print("=" * 70)
        print("📊 SUMMARY")
        print("=" * 70)
        print(f"✅ Successfully created: {success_count} terms")
        print(f"❌ Failed to create: {failed_count} terms")
        print(f"📈 Success rate: {success_count}/{total_terms} ({100*success_count/total_terms:.1f}%)")
        print()
        
        if success_count > 0:
            print("🎉 Glossary terms created!")
            print("   Next: Run link_glossary_to_data_products.py to link them to data products")
    
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
