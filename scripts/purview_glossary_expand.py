"""
Purview Glossary: Barncancerforskning & Klinisk Vård
=====================================================
Skapar ~80 nya glossarytermer + tilldelar alla (nya + befintliga) till rätt kategorier.

Kategorier:
  - Barncancerforskning  (diagnoser, behandlingar, forskning, biobank)
  - Klinisk Data          (vitalparametrar, labvärden, vårdprocesser)
  - Kliniska Standarder   (kodverk, regelverk, kvalitet)
  - Interoperabilitet     (FHIR, DICOM, integration)
  - Dataarkitektur        (medallion, ETL, ML, OMOP)

Usage:
  python scripts/purview_glossary_expand.py
"""
import json, os, sys, time

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import requests
from azure.identity import AzureCliCredential
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

cred = AzureCliCredential(process_timeout=30)
sess = requests.Session()
sess.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1,
                                                      status_forcelist=[429, 502, 503])))

ACCT = "https://prviewacc.purview.azure.com"
ATLAS = f"{ACCT}/catalog/api/atlas/v2"
GLOSSARY_GUID = "d939ea20-9c67-48af-98d9-b66965f7cde1"

# Category GUIDs
CAT_BARNCANCER = "a4b7c43f-b028-4132-a8fe-745f4254234e"
CAT_KLINISK    = "b971f80a-dad9-4742-8626-aa5a07216708"
CAT_STANDARD   = "716df4e0-9ae5-46c3-90ee-b01c7b5b08d8"
CAT_INTEROP    = "7ddea2c9-fb2b-4883-9096-ce3c8dcd1d81"
CAT_DATAARK    = "0363c301-7938-4622-9f27-21a3559b0581"

# Formatting
G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; C = "\033[96m"
B = "\033[94m"; D = "\033[2m"; BOLD = "\033[1m"; RST = "\033[0m"

_token_cache = {}


def get_headers():
    if "purview" not in _token_cache or _token_cache["purview"][1] < time.time() - 2400:
        token = cred.get_token("https://purview.azure.net/.default")
        _token_cache["purview"] = (token.token, time.time())
    return {"Authorization": f"Bearer {_token_cache['purview'][0]}",
            "Content-Type": "application/json"}


def hdr(title):
    print(f"\n{BOLD}{B}{'═' * 70}")
    print(f"  {title}")
    print(f"{'═' * 70}{RST}")


def ok(msg):   print(f"  {G}✓{RST} {msg}")
def warn(msg): print(f"  {Y}⚠{RST} {msg}")
def info(msg): print(f"  {D}·{RST} {msg}")


# ══════════════════════════════════════════════════════════════════════
# NYA TERMER — Barncancerforskning
# ══════════════════════════════════════════════════════════════════════

BARNCANCER_TERMS = [
    # --- Diagnoser ---
    ("ALL (Akut Lymfatisk Leukemi)",
     "Den vanligaste cancerformen hos barn. Utgör ca 25% av all barncancer. "
     "Karakteriseras av okontrollerad tillväxt av lymfocytprekursorer i benmärgen. "
     "Behandlas enligt NOPHO ALL-protokollet med hög överlevnadsgrad (>90%).",
     "Diagnos"),

    ("AML (Akut Myeloisk Leukemi)",
     "Näst vanligaste leukemin hos barn. Aggressiv benmärgsmalignitet med snabb progress. "
     "Kräver intensiv kemoterapi och ibland stamcellstransplantation.",
     "Diagnos"),

    ("CNS-tumör",
     "Hjärntumörer och ryggmärgstumörer — näst vanligaste barncancergruppen. "
     "Inkluderar medulloblastom, gliom, ependymom och kraniofaryngiom. "
     "Behandling kombinerar kirurgi, strålning och cytostatika beroende på typ och grad.",
     "Diagnos"),

    ("Neuroblastom",
     "Embryonal tumör utgående från sympatiska nervsystemet. Vanligast hos barn under 5 år. "
     "Bred klinisk presentation — från spontan regression till aggressiv metastaserad sjukdom. "
     "MYCN-amplifiering är viktigaste prognostiska markören.",
     "Diagnos"),

    ("Wilms tumör (Nefroblastom)",
     "Vanligaste njurtumören hos barn. Upptäcks ofta som palpabel buk hos barn 2-5 år. "
     "Utmärkt prognos med kombinerad behandling (kirurgi + kemoterapi ± strålning). "
     "Klassificeras enligt SIOP-protokollet baserat på histologiskt svar efter kemoterapi.",
     "Diagnos"),

    ("Retinoblastom",
     "Malign ögontumör hos barn, ofta diagnostiserad före 3 års ålder. "
     "RB1-genmutation (tumörsuppressorgen). Kan vara hereditär (bilateral) eller sporadisk. "
     "Tidig diagnos via leukokori (vit pupillreflex) ger god prognos.",
     "Diagnos"),

    ("Ewing sarkom",
     "Malign bentumör vanligast hos tonåringar. Karakteristisk translokation t(11;22) "
     "skapar EWS-FLI1 fusionsgen. Behandling kräver kemoterapi + kirurgi/strålning.",
     "Diagnos"),

    ("Osteosarkom",
     "Vanligaste primära maligna bentumören hos barn och ungdomar. "
     "Typiskt lokaliserad kring knäleden under tillväxtspurten. "
     "Behandling: neoadjuvant kemoterapi → kirurgi → adjuvant kemoterapi.",
     "Diagnos"),

    ("Rhabdomyosarkom",
     "Vanligaste mjukdelssarkomet hos barn. Utgår från skelettmuskulatur. "
     "Två huvudtyper: embryonalt (bättre prognos) och alveolärt (PAX-FOXO1-fusion). "
     "Behandlas multimodalt enligt CWS/EpSSG-protokoll.",
     "Diagnos"),

    ("Medulloblastom",
     "Vanligaste maligna hjärntumören hos barn. Lokaliserad i lillhjärnan (bakre skallgropen). "
     "Fyra molekylära subgrupper: WNT, SHH, Grupp 3, Grupp 4 — med olika prognos. "
     "WNT-subgrupp har bäst prognos (>95% överlevnad).",
     "Diagnos"),

    ("Hodgkins lymfom",
     "Lymfkörtelcancer vanligast hos tonåringar. Reed-Sternberg-celler är diagnostiska. "
     "Mycket god prognos (>95%) med moderna behandlingsprotokoll. "
     "Stadieindelning enligt Ann Arbor-klassifikationen.",
     "Diagnos"),

    ("Non-Hodgkins lymfom (barn)",
     "Heterogen grupp lymfoida maligniteter hos barn. Inkluderar Burkitt-lymfom, "
     "diffust storcelligt B-cellslymfom, lymfoblastiskt lymfom och ALCL. "
     "Snabbt växande men ofta kemoterapikänsliga.",
     "Diagnos"),

    # --- Behandling & forskning ---
    ("Kemoterapi",
     "Läkemedelsbehandling med cytostatika som dödar eller hämmar tillväxt av cancerceller. "
     "Barncancerprotokoll använder ofta kombinationsbehandling i faser: "
     "induktion, konsolidering, underhåll. Dosjustering baseras på kroppsyta (m²).",
     "Behandling"),

    ("Strålbehandling",
     "Användning av joniserande strålning för tumörbehandling. "
     "Protonterapi används alltmer för barncancer för att minska strålskador på frisk vävnad. "
     "Särskilt viktigt för CNS-tumörer och Hodgkins lymfom.",
     "Behandling"),

    ("Stamcellstransplantation",
     "Hematopoetisk stamcellstransplantation (HSCT) — ersätter sjuk benmärg med friska stamceller. "
     "Kan vara autolog (egna celler) eller allogen (donator). "
     "Indikation vid högrisk-AML, recidiv-ALL, och vissa solida tumörer.",
     "Behandling"),

    ("CAR-T cellterapi",
     "Chimeric Antigen Receptor T-cellterapi — genetiskt modifierade T-celler som riktas mot tumörceller. "
     "Godkänd för recidiverad/refraktär B-cells-ALL hos barn (tisagenlecleucel/Kymriah). "
     "Revolutionerande immunterapi med potentiellt kurativ effekt.",
     "Behandling"),

    ("Protonterapi",
     "Avancerad strålbehandlingsform som använder protoner istället för fotoner. "
     "Ger skarpare dosfördelning (Bragg-topp) och skonar omgivande frisk vävnad. "
     "Skandionkliniken i Uppsala behandlar barncancerpatienter från hela Norden.",
     "Behandling"),

    ("Immunterapi",
     "Behandling som stärker eller styr immunsystemets cancerbekämpning. "
     "Inkluderar checkpointhämmare (anti-PD1/PDL1), bispecifika antikroppar (blinatumomab), "
     "antikropps-läkemedelskonjugat och CAR-T. Allt viktigare inom barncancer.",
     "Behandling"),

    ("Minimal Residual Disease (MRD)",
     "Mätning av kvarvarande tumörceller efter behandling — ner till 1 cell per 10 000-1 000 000. "
     "Mäts via flödescytometri eller PCR. Viktigaste prognostiska faktorn vid ALL-behandling. "
     "Styr behandlingsintensitet i moderna protokoll.",
     "Forskningsmetod"),

    ("NOPHO (Nordic Society of Paediatric Haematology and Oncology)",
     "Nordisk förening som koordinerar barncancerbehandling och forskning i Norden. "
     "Utvecklar gemensamma behandlingsprotokoll för ALL, AML och andra barncancerformer. "
     "NOPHO ALL-2008 och ALL-Together är centrala protokoll.",
     "Organisation"),

    ("SIOP (International Society of Paediatric Oncology)",
     "Internationellt samfund för pediatrisk onkologi. "
     "Koordinerar globala kliniska prövningar och utvecklar behandlingsstandarder. "
     "SIOP-protokollet för Wilms tumör är internationell standard.",
     "Organisation"),

    ("Whole Genome Sequencing (WGS)",
     "Helgenomsekvensering — kartläggning av hela patientens DNA-sekvens (~3 miljarder baspar). "
     "Identifierar somatiska mutationer, strukturella varianter och kopienummervariationer. "
     "Används i precisionsmedicin för att hitta målriktade behandlingar.",
     "Forskningsmetod"),

    ("Whole Exome Sequencing (WES)",
     "Sekvensering av alla proteinkodande gener (~22 000 gener, ~2% av genomet). "
     "Kostnadseffektivt alternativ till WGS för att hitta kliniskt relevanta mutationer. "
     "Standard inom diagnostisk genetik vid barncancer.",
     "Forskningsmetod"),

    ("RNA-sekvensering",
     "Transkriptomanalys som mäter genuttryck och identifierar fusionsgener. "
     "Kritisk för subklassificering av barncancerformer — t.ex. PAX-FOXO1 vid rhabdomyosarkom, "
     "EWS-FLI1 vid Ewing sarkom. Komplement till DNA-sekvensering.",
     "Forskningsmetod"),

    ("Flödescytometri",
     "Teknik för snabb analys av cellpopulationer baserat på ytmarkörer och intracellulära proteiner. "
     "Fundamental för leukemidiagnostik och MRD-mätning. "
     "Identifierar cellinjespecifika markörer (CD19, CD10, CD34 etc.).",
     "Forskningsmetod"),

    ("Immunhistokemi (IHK)",
     "Vävnadsfärgning med antikroppar för att påvisa specifika proteiner in situ. "
     "Standardmetod inom patologisk diagnostik av solida tumörer. "
     "Viktiga markörer: Ki-67 (proliferation), synaptofysin (neuroblastom), desmin (rhabdomyosarkom).",
     "Forskningsmetod"),

    ("Ki-67 Proliferationsindex",
     "Immunhistokemisk markör som mäter andelen aktivt delande celler i tumörvävnad. "
     "Uttrycks i procent. Högt Ki-67 (>20%) indikerar snabbt växande tumör. "
     "Viktig prognostisk och prediktiv faktor inom onkologi.",
     "Biomarkör"),

    ("MYCN-amplifiering",
     "Genomisk förändring med extra kopior av MYCN-onkogenen på kromosom 2p. "
     "Starkaste negativa prognostiska markören vid neuroblastom. "
     "Förekommer i ~20% av neuroblastom och driver aggressiv tumörtillväxt.",
     "Biomarkör"),

    ("Tumörmutationsbörda (TMB)",
     "Antal somatiska mutationer per megabas DNA i tumörvävnad. "
     "Prediktiv biomarkör för immunterapi-svar (hög TMB → bättre respons på checkpointhämmare). "
     "Generellt lägre hos barncancer jämfört med vuxencancer.",
     "Biomarkör"),

    ("Barncancerfonden",
     "Svensk ideell organisation som finansierar barncancerforskning. "
     "Finansierar ~70% av all barncancerforskning i Sverige. "
     "Driver även patient- och familjestöd samt opinionsbildning.",
     "Organisation"),

    ("Överlevnadsanalys",
     "Statistisk analys av tid till händelse (death, relapse, progression). "
     "Kaplan-Meier-kurvor och Cox regression är standardmetoder. "
     "5-årsöverlevnad för barncancer i Sverige är ~85%. Event-free survival (EFS) är primärt utfallsmått.",
     "Forskningsmetod"),

    ("Seneffektsmottagning",
     "Specialiserad uppföljningsmottagning för tidigare barncancerpatienter i vuxen ålder. "
     "Monitorerar långtidseffekter av behandling: kardiotoxicitet, fertilitet, sekundär malignitet, "
     "endokrina störningar, neurokognitiv påverkan. Följer IGHG-riktlinjer.",
     "Klinisk process"),

    ("Molekylär tumörboard",
     "Multidisciplinärt team som tolkar genomikresultat och rekommenderar precisionsmedicin. "
     "Inkluderar onkolog, genetiker, patolog, bioinformatiker. "
     "Diskuterar actionable mutations och tillgängliga kliniska prövningar.",
     "Klinisk process"),

    ("Liquid Biopsy (Flytande biopsi)",
     "Blodprov-baserad tumördiagnostik genom analys av cirkulerande tumör-DNA (ctDNA). "
     "Icke-invasivt alternativ till vävnadsbiopsi. Möjliggör tidig recidivdetektion "
     "och behandlingsmonitorering i realtid.",
     "Forskningsmetod"),
]

# ══════════════════════════════════════════════════════════════════════
# NYA TERMER — Klinisk Vård
# ══════════════════════════════════════════════════════════════════════

KLINISK_TERMS = [
    ("Triage",
     "Systematisk prioritering av patienter baserat på medicinskt behov. "
     "RETTS (Rapid Emergency Triage and Treatment System) används i Sverige. "
     "Fem prioritetsnivåer: röd (livshot), orange, gul, grön, blå.",
     "Vårdprocess"),

    ("Akutmottagning",
     "Sjukhusenhet för akut omhändertagande. Dokumenterar triage-nivå, vitala parametrar, "
     "åtgärder och beslut. Genererar encounter-data med hög informationstäthet. "
     "Viktig datakälla för prediktionsmodeller (sepsis, stroke, hjärtinfarkt).",
     "Vårdprocess"),

    ("Remiss",
     "Formell begäran om konsultation eller vård hos annan vårdgivare/specialist. "
     "Innehåller anamnes, frågeställning och önskad åtgärd. "
     "Elektroniska remisser via journalsystem möjliggör spårbarhet och ledtidsmätning.",
     "Vårdprocess"),

    ("Epikris",
     "Sammanfattande läkaranteckning vid vårdtidens slut. "
     "Innehåller diagnoser (huvud- och bidiagnoser), utförda åtgärder, "
     "rekommendationer och uppföljningsplan. Viktig för vårdkontinuitet och kodning.",
     "Vårdprocess"),

    ("Multidisciplinär konferens (MDK)",
     "Strukturerat teammöte där specialister gemensamt diskuterar patientfall. "
     "Standard inom onkologi — kirurg, onkolog, radiolog, patolog, kontaktsjuksköterska. "
     "Beslut dokumenteras som MDK-protokoll med behandlingsrekommendation.",
     "Vårdprocess"),

    ("Vårdplan",
     "Individuell behandlingsplan som beskriver mål, åtgärder, ansvar och tidsramar. "
     "Samordnad individuell plan (SIP) för patienter med behov från flera vårdgivare. "
     "Stärker patientdelaktighet och vårdkontinuitet.",
     "Vårdprocess"),

    ("Intensivvård (IVA)",
     "Specialiserad vård av kritiskt sjuka patienter med organsvikt. "
     "Högt dataintensiv — kontinuerlig monitorering genererar tusentals datapunkter per dygn. "
     "Apache II och SOFA-score används för svårighetsbedömning.",
     "Vårdnivå"),

    ("Palliativ vård",
     "Lindrande vård vid obotlig sjukdom, fokus på livskvalitet. "
     "Inom barncancer: symtomkontroll, psykosocialt stöd till familjen, "
     "brytpunktssamtal. Dokumenteras med specifika ESAS/PPS-skalor.",
     "Vårdnivå"),

    ("NEWS (National Early Warning Score)",
     "Standardiserat poängsystem för tidig upptäckt av försämring hos inneliggande patienter. "
     "Baseras på andningsfrekvens, syremättnad, temperatur, blodtryck, puls, medvetandegrad. "
     "Aggregerad poäng triggar eskaleringsåtgärder (0-4 normal, 5-6 medium, ≥7 hög risk).",
     "Klinisk skala"),

    ("GCS (Glasgow Coma Scale)",
     "Standardskala för bedömning av medvetandegrad. "
     "Tre komponenter: ögonöppning (1-4), verbal respons (1-5), motorisk respons (1-6). "
     "Totalpoäng 3-15. Under 8 = svår skallskada, indikation för intubation.",
     "Klinisk skala"),

    ("VAS/NRS Smärtskattning",
     "Visuell Analog Skala (VAS) och Numeric Rating Scale (NRS) för smärtbedömning. "
     "NRS: 0-10 (0=ingen smärta, 10=värsta tänkbara). "
     "Barn <7 år: ansiktsskala (Wong-Baker FACES). Dokumenteras som vitalparameter.",
     "Klinisk skala"),

    ("eGFR (Estimerad Glomerulär Filtration)",
     "Uppskattning av njurfunktion baserat på kreatinin, ålder, kön. "
     "CKD-EPI-formeln är standard. eGFR <60 = nedsatt njurfunktion. "
     "Kritiskt vid dosering av nefrotoxiska cytostatika (cisplatin, metotrexat).",
     "Labvärde"),

    ("Troponin",
     "Hjärtskademarkör — mäter myokardskada. Högsensitivt troponin T/I. "
     "Viktig vid antracyklinbehandling (doxorubicin) som kan ge kardiotoxicitet. "
     "Serieprover används vid misstänkt hjärtinfarkt och kemoterapiövervakning.",
     "Labvärde"),

    ("CRP (C-reaktivt protein)",
     "Akutfasprotein som stiger vid inflammation och infektion. "
     "Normal <5 mg/L. Vid neutropen feber hos cancerpatienter: CRP >100 indikerar allvarlig infektion. "
     "Används för monitorering av infektionsförlopp och behandlingssvar.",
     "Labvärde"),

    ("Blodstatus (Hb, LPK, TPK)",
     "Komplett blodvärde — hemoglobin, leukocyt- och trombocytantal med differentialräkning. "
     "Fundamental vid kemoterapimonitorering: neutropeni (ANC <0.5) = behandlingspaus, "
     "trombocytopeni (TPK <20) = transfusionsbehov. Tas dagligen vid aktiv behandling.",
     "Labvärde"),

    ("Kreatinin",
     "Njurfunktionsmarkör — nedbrytningsprodukt från muskelmetabolism. "
     "Förhöjt kreatinin indikerar nedsatt njurfunktion. "
     "Särskilt viktigt vid cisplatinbehandling och metotrexatinfusioner inom barncancervård.",
     "Labvärde"),

    ("ALAT/ASAT (Levertransaminaser)",
     "Leverenzymer som stiger vid leverskada. ALAT mer specifik för lever. "
     "Förhöjda värden vid kemoterapi-inducerad hepatotoxicitet, VOD (veno-occlusive disease), "
     "eller infektioner. Behandlingsdosjustering vid ALAT >5x övre normalvärde.",
     "Labvärde"),

    ("Informerat samtycke (kliniskt)",
     "Patient/vårdnadshavares godkännande av behandling efter information om risker och alternativ. "
     "Krav enligt Patientlagen (2014:821). Vid barncancer: åldersanpassad information till barnet, "
     "formellt samtycke från vårdnadshavare. Separat samtycke krävs för forskningsdeltagande.",
     "Regelverk"),

    ("Patientdatalagen (PDL)",
     "Svensk lag (2008:355) som reglerar behandling av personuppgifter inom hälso- och sjukvården. "
     "Styr journalföring, sammanhållen journalföring, åtkomst och sekretess. "
     "Kräver spärrmöjlighet för patienten. Inre sekretess mellan vårdenheter.",
     "Regelverk"),

    ("Kvalitetsregister",
     "Nationella kvalitetsregister samlar patientdata för uppföljning och förbättring. "
     "Barncancerregistret (SBCR) är ett av ~100 svenska kvalitetsregister. "
     "Data används för forskning, benchmarking och klinisk förbättring.",
     "Regelverk"),

    ("Kontaktsjuksköterska",
     "Namngiven sjuksköterska som är patientens fasta vårdkontakt genom cancerbehandlingen. "
     "Lagstadgad rättighet sedan 2010. Koordinerar vård, informerar, "
     "och fungerar som länk mellan patient/familj och vårdteamet.",
     "Vårdprocess"),

    ("Brytpunktssamtal",
     "Strukturerat samtal vid övergång från kurativ till palliativ vårdinriktning. "
     "Dokumenteras i journal med specifik sökordsmall. "
     "Inom barncancer: involverar föräldrar och vid mognad barnet. Etiskt komplext.",
     "Vårdprocess"),

    ("Rehabilitering (barn)",
     "Tvärprofessionell rehabilitering under och efter cancerbehandling. "
     "Inkluderar fysioterapi, arbetsterapi, psykolog, logoped, specialpedagog. "
     "Neurokognitiv rehabilitering särskilt viktig efter CNS-tumör och strålbehandling.",
     "Vårdprocess"),
]

# ══════════════════════════════════════════════════════════════════════
# NYA TERMER — Kliniska Standarder & Kodverk
# ══════════════════════════════════════════════════════════════════════

STANDARD_TERMS = [
    ("KVÅ (Klassifikation av vårdåtgärder)",
     "Svenskt kodverk för medicinska åtgärder. Ägs av Socialstyrelsen. "
     "Används för DRG-gruppering och ersättningsberäkning. "
     "Exempel: DT000 (datortomografi huvud), GD005 (kemoterapi iv).",
     "Kodverk"),

    ("ICF (International Classification of Functioning)",
     "WHO-klassifikation av funktionstillstånd, funktionshinder och hälsa. "
     "Kompletterar ICD-10 med funktionsperspektiv: kroppsfunktion, aktivitet, delaktighet. "
     "Används inom rehabilitering och habilitering.",
     "Kodverk"),

    ("NordDRG",
     "Nordiskt DRG-system för casemix-klassificering av sjukhusvård. "
     "Grupperar vårdkontakter baserat på diagnos + åtgärd → DRG-grupp → kostnadsvikt. "
     "Grunden för sjukhusersättning i Sverige.",
     "Kodverk"),

    ("ATC (Anatomical Therapeutic Chemical Classification)",
     "WHO-klassifikation av läkemedel i fem nivåer. "
     "Systematisk kodning: L01 (antineoplastiska medel), L01CA (vincaalkaloider), L01CA02 (vinkristin). "
     "Standard i Sverige via FASS och Läkemedelsverket.",
     "Kodverk"),

    ("GCP (Good Clinical Practice)",
     "Internationell etisk och vetenskaplig kvalitetsstandard för kliniska prövningar. "
     "ICH-GCP krav: informerat samtycke, monitorering, source data verification. "
     "Obligatorisk för läkemedelsprövningar inom barncancer.",
     "Regelverk"),

    ("Etikprövningslagen",
     "Svensk lag (2003:460) om etikprövning av forskning som avser människor. "
     "Kräver godkännande från Etikprövningsmyndigheten före studiestart. "
     "Barncancer: extra skyddsaspekter vid forskning på barn (särskilt sårbar grupp).",
     "Regelverk"),

    ("GDPR i vården",
     "EU:s dataskyddsförordning tillämpad på hälso- och sjukvårdsdata. "
     "Hälsodata = känsliga personuppgifter (artikel 9). Rättslig grund: allmänt intresse. "
     "Kräver DPIA vid storskalig behandling, pseudonymisering vid forskning.",
     "Regelverk"),

    ("Biobankslagen",
     "Svensk lag (2023:38) om biobanker i hälso- och sjukvården och forskning. "
     "Reglerar insamling, förvaring och användning av biologiska prover. "
     "Barntumörbanken (BTB) lyder under biobankslagen. Kräver samtycke.",
     "Regelverk"),
]

# ══════════════════════════════════════════════════════════════════════
# NYA TERMER — Interoperabilitet
# ══════════════════════════════════════════════════════════════════════

INTEROP_TERMS = [
    ("HL7 v2",
     "Meddelandestandard för hälso- och sjukvårdssystem. Äldsta HL7-standarden (sedan 1987). "
     "Pipe-delimiterad meddelandeformat: MSH|^~\\&|... "
     "Fortfarande dominerande i svenska sjukhussystem för lab, ADT och order.",
     "Standard"),

    ("CDA (Clinical Document Architecture)",
     "XML-baserat dokumentformat för kliniska dokument. Del av HL7-familjen. "
     "Används i Sverige för elektroniska remisser/svar och nationell patientöversikt (NPÖ). "
     "Strukturerad header + narrativt block + maskintolkbar sektion.",
     "Standard"),

    ("IHE-profiler",
     "Integrating the Healthcare Enterprise — tekniska ramar för interoperabilitet. "
     "Definierar aktörer och transaktioner. Viktiga profiler: XDS (dokumentdelning), "
     "PIX (patientidentifiering), PDQ (patientdemografi), RAD (radiologi).",
     "Standard"),

    ("Nationell Patientöversikt (NPÖ)",
     "Tjänst för sammanhållen journalföring i Sverige. Möjliggör att behörig vårdpersonal "
     "kan ta del av journalinformation från andra landsting/regioner. "
     "Bygger på CDA-dokument via Ineras nationella tjänsteplattform.",
     "Sverige"),

    ("Inera",
     "Organisation som driver Sveriges digitala infrastruktur inom vård och omsorg. "
     "Äger tjänsteplattformen, 1177, NPÖ, Pascal, SITHS-kort. "
     "Central aktör för e-hälsa och nationell interoperabilitet.",
     "Organisation"),

    ("SITHS-kort",
     "Säker IT-identifiering inom hälso- och sjukvården. Tjänste-ID-kort med e-legitimation. "
     "Krävs för inloggning i journalsystem, e-recept och NPÖ. "
     "Utfärdas av Inera med certifikat för autentisering och signering.",
     "Infrastruktur"),

    ("openEHR",
     "Öppen standard för klinisk informationsmodellering. Tvånivåmodellering: "
     "referensmodell (teknik) + arketyper (klinisk kunskap). "
     "Växande användning internationellt. Möjliggör semantisk interoperabilitet.",
     "Standard"),

    ("Terminologitjänst",
     "Centraliserad tjänst som tillhandahåller kodverk, valueset och terminologier. "
     "FHIR-baserade terminologitjänster (ValueSet, CodeSystem, ConceptMap). "
     "Socialstyrelsen tillhandahåller svenska kodverk via Nationella fackspråket.",
     "Infrastruktur"),
]

# ══════════════════════════════════════════════════════════════════════
# NYA TERMER — Dataarkitektur
# ══════════════════════════════════════════════════════════════════════

DATAARK_TERMS = [
    ("Data Lakehouse",
     "Arkitekturmönster som kombinerar Data Lake (billig lagring, schema-on-read) "
     "med Data Warehouse (ACID-transaktioner, schemavalidering). "
     "Microsoft Fabric Lakehouse använder Delta Lake-format. Grunden i vår arkitektur.",
     "Arkitektur"),

    ("Delta Lake",
     "Open-source lagringsformat ovanpå Parquet med ACID-transaktioner, "
     "tidsresor (time travel), schema evolution och Z-ordering. "
     "Standard i Microsoft Fabric och Databricks. Möjliggör reliable bronze→silver→gold pipeline.",
     "Teknologi"),

    ("Apache Spark",
     "Distribuerad beräkningsmotor för storskalig databearbetning. "
     "PySpark/SparkSQL används i Fabric Notebooks för ETL-pipeline. "
     "Batch + streaming (Structured Streaming). Optimerat med Photon-motorn i Fabric.",
     "Teknologi"),

    ("ETL-pipeline",
     "Extract-Transform-Load — process för att flytta data mellan system. "
     "I vår arkitektur: SQL → Bronze (extract), Bronze → Silver (transform/cleanse), "
     "Silver → Gold (aggregate/enrich). Orkestrerad via Fabric Data Pipelines.",
     "Arkitektur"),

    ("Data Mesh",
     "Decentraliserad dataarkitektur där domänteam äger sin data som produkt. "
     "Fyra principer: domänägarskap, data som produkt, self-serve plattform, federerad governance. "
     "Purview Data Products implementerar data-som-produkt-principen.",
     "Arkitektur"),

    ("Master Data Management (MDM)",
     "Process och teknik för att skapa en enda pålitlig version av masterdata. "
     "Patient-MDM: säkerställer att patient_id är konsistent över alla system "
     "(SQL, FHIR, Fabric, OMOP). Golden record-koncept.",
     "Arkitektur"),

    ("Data Quality Score",
     "Kvantitativt mått på datakvalitet baserat på dimensioner: "
     "fullständighet, korrekthet, konsistens, aktualitet, unikhet, validitet. "
     "Implementerat i Gold-lagret via data_quality_report.json och validationsscripts.",
     "Kvalitet"),

    ("Schema Evolution",
     "Förmåga att ändra tabellschema utan att bryta befintliga pipelines. "
     "Delta Lake stöder addColumn, renameColumn (med mappning), typeWidening. "
     "Kritiskt i vårdata där nya fält tillkommer (nya labprover, nya formulär).",
     "Teknologi"),

    ("Data Lineage",
     "Spårbarhet av data genom hela flödet — från källa till konsument. "
     "Purview Process-entiteter visualiserar lineage. Kolumnnivå-lineage visar "
     "hur enskilda fält transformeras. Regulatoriskt krav inom sjukvårdsdata.",
     "Kvalitet"),

    ("Feature Store",
     "Centraliserad lagringsplats för ML-features med versionshantering. "
     "Gold-lagrets ml_features-tabell fungerar som feature store: "
     "förberäknade features (CCI, åldersgrupp, antal diagnoser) för prediktionsmodeller.",
     "ML"),
]


# ══════════════════════════════════════════════════════════════════════
# BEFINTLIGA TERMER → KATEGORITILLDELNING
# (termer som redan finns men saknar kategorikoppling)
# ══════════════════════════════════════════════════════════════════════

EXISTING_TERM_CATEGORIES = {
    # Barncancerforskning
    CAT_BARNCANCER: [
        "BTB (Barntumörbanken)", "SBCR (Svenska Barncancerregistret)",
        "Genomic Medicine Sweden (GMS)", "Genomisk variant", "VCF (Variant Call Format)",
        "HGVS-nomenklatur", "ACMG-klassificering", "Tumörsite", "Tumörstadium",
        "Seneffekter", "Behandlingsprotokoll", "FFPE (Formalinfixerat paraffin)",
        "Biobank", "Histopatologi", "ICD-O-3", "FLAIR",
        "T1-viktad MR", "T2-viktad MR", "MR (Magnetresonanstomografi)",
        "Etikprövning", "Informerat samtycke",
    ],
    # Klinisk Data
    CAT_KLINISK: [
        "Patientdemografi", "Personnummer", "Svenskt personnummer",
        "Vårdkontakt", "Vårdtid (LOS)", "Vitalparametrar", "Labresultat",
        "Charlson Comorbidity Index", "Återinläggningsrisk", "ML-prediktion",
        "Skyddad hälsoinformation (PHI)", "Pseudonymisering",
    ],
    # Kliniska Standarder
    CAT_STANDARD: [
        "ICD-10", "SNOMED-CT", "LOINC", "DRG-klassificering",
    ],
    # Interoperabilitet
    CAT_INTEROP: [
        "FHIR R4", "FHIR Patient", "FHIR Encounter", "FHIR Condition",
        "FHIR Observation", "FHIR MedicationRequest", "FHIR DiagnosticReport",
        "FHIR ImagingStudy", "FHIR Specimen", "DICOM", "DICOMweb",
    ],
    # Dataarkitektur
    CAT_DATAARK: [
        "Medallion-arkitektur", "Bronze-lager", "Silver-lager", "Gold-lager",
        "Feature Engineering", "OMOP CDM", "OMOP Person", "OMOP Visit Occurrence",
        "OMOP Condition Occurrence", "OMOP Drug Exposure", "OMOP Measurement",
        "OMOP Specimen", "OMOP Genomics",
    ],
}


# ══════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    print(f"\n{BOLD}{B}{'═' * 70}")
    print(f"  PURVIEW GLOSSARY: BARNCANCERFORSKNING & KLINISK VÅRD")
    print(f"  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 70}{RST}")

    h = get_headers()

    # ── 1. Hämta befintliga termer ──
    hdr("1. HÄMTA BEFINTLIGA TERMER")
    r = sess.get(f"{ATLAS}/glossary/{GLOSSARY_GUID}/terms?limit=200&offset=0",
                 headers=h, timeout=15)
    if r.status_code != 200:
        print(f"  {R}FATAL: Kunde inte hämta termer: {r.status_code}{RST}")
        return
    existing = {t["name"]: t["guid"] for t in r.json()}
    info(f"Befintliga termer: {len(existing)}")

    # ── 2. Skapa nya termer ──
    all_new = [
        ("Barncancerforskning", CAT_BARNCANCER, BARNCANCER_TERMS),
        ("Klinisk Vård",        CAT_KLINISK,    KLINISK_TERMS),
        ("Kliniska Standarder", CAT_STANDARD,   STANDARD_TERMS),
        ("Interoperabilitet",   CAT_INTEROP,    INTEROP_TERMS),
        ("Dataarkitektur",      CAT_DATAARK,    DATAARK_TERMS),
    ]

    created = 0
    skipped = 0
    failed = 0

    for cat_label, cat_guid, terms in all_new:
        hdr(f"2. NYA TERMER: {cat_label}")
        for name, long_desc, short_desc in terms:
            if name in existing:
                info(f"Finns redan: {name}")
                skipped += 1
                continue

            body = {
                "name": name,
                "qualifiedName": f"Sjukvårdstermer@{name}",
                "shortDescription": short_desc,
                "longDescription": long_desc,
                "anchor": {"glossaryGuid": GLOSSARY_GUID},
                "categories": [{"categoryGuid": cat_guid}],
            }
            r = sess.post(f"{ATLAS}/glossary/term", headers=h, json=body, timeout=15)
            if r.status_code in (200, 201):
                guid = r.json().get("guid", "?")
                existing[name] = guid
                ok(f"{name} (guid={guid[:12]}...)")
                created += 1
            elif r.status_code == 409:
                info(f"Finns redan (409): {name}")
                skipped += 1
            else:
                warn(f"{name}: {r.status_code} — {r.text[:120]}")
                failed += 1
            time.sleep(0.15)

    # ── 3. Tilldela kategorier till befintliga termer ──
    hdr("3. TILLDELA KATEGORIER TILL BEFINTLIGA TERMER")
    assigned = 0
    for cat_guid, term_names in EXISTING_TERM_CATEGORIES.items():
        for tname in term_names:
            if tname not in existing:
                info(f"Finns ej: {tname} (hoppar)")
                continue

            term_guid = existing[tname]
            # Hämta nuvarande term för att se om kategori redan satt
            r = sess.get(f"{ATLAS}/glossary/term/{term_guid}", headers=h, timeout=10)
            if r.status_code != 200:
                warn(f"Kunde inte hämta term {tname}: {r.status_code}")
                continue

            term_data = r.json()
            current_cats = term_data.get("categories", [])
            already_has = any(c.get("categoryGuid") == cat_guid for c in current_cats)
            if already_has:
                info(f"Redan tilldelad: {tname}")
                continue

            # Lägg till kategori
            new_cats = list(current_cats) + [{"categoryGuid": cat_guid}]
            update_body = {
                "guid": term_guid,
                "name": tname,
                "qualifiedName": term_data.get("qualifiedName", f"Sjukvårdstermer@{tname}"),
                "anchor": {"glossaryGuid": GLOSSARY_GUID},
                "categories": new_cats,
            }
            r2 = sess.put(f"{ATLAS}/glossary/term/{term_guid}", headers=h,
                          json=update_body, timeout=15)
            if r2.status_code in (200, 201):
                ok(f"{tname} → kategori tilldelad")
                assigned += 1
            else:
                warn(f"{tname}: {r2.status_code} — {r2.text[:120]}")
            time.sleep(0.15)

    # ── Sammanfattning ──
    hdr("SAMMANFATTNING")
    total_after = len(existing)
    print(f"  {G}Nya termer skapade:        {created}{RST}")
    print(f"  {D}Redan existerande:         {skipped}{RST}")
    if failed:
        print(f"  {Y}Misslyckade:               {failed}{RST}")
    print(f"  {G}Kategorier tilldelade:     {assigned}{RST}")
    print(f"  {BOLD}Totalt antal termer nu:    {total_after}{RST}")


if __name__ == "__main__":
    main()
