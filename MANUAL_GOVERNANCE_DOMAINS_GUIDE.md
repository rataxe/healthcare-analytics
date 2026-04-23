# Skapa Governance Domains i Purview Portal

## PROBLEMET
- Governance Domains kan INTE skapas via REST API
- Glossary terms är INTE samma som governance domains
- UI visar "0 governance domains" i Enterprise glossary eftersom vi försökte använda workaround

## LÖSNING: Manuell skapande i Portal

### Steg 1: Öppna Governance Domains
1. Gå till: https://purview.microsoft.com/governance/domains
2. Eller: Azure Portal → prviewacc → Unified Catalog → Discovery → Enterprise glossary → "Governance domains" tab

### Steg 2: Skapa 4 Governance Domains

#### Domain 1: Klinisk Vård (Clinical Care)
```
Name: Klinisk Vård
English Name: Clinical Care
Description: Governance domain för kliniska patientdata, OMOP-standarder, och elektronisk patientjournal.
Parent Domain: (none - root level)
Domain Owners: joandolf@microsoft.com
```

**Länka Data Product:**
- Klinisk Patientanalys

#### Domain 2: Forskning & Genomik (Research & Genomics)
```
Name: Forskning & Genomik  
English Name: Research & Genomics
Description: Governance domain för genomisk forskning, BrainChild-studie, VCF-data, och precision medicine.
Parent Domain: (none - root level)
Domain Owners: joandolf@microsoft.com
```

**Länka Data Product:**
- BrainChild Barncancerforskning

#### Domain 3: Interoperabilitet & Standarder (Interoperability & Standards)
```
Name: Interoperabilitet & Standarder
English Name: Interoperability & Standards
Description: Governance domain för FHIR, DICOM, standardiserade medicinska kodverk, och registerdata.
Parent Domain: (none - root level)
Domain Owners: joandolf@microsoft.com
```

**Länka Data Product:**
- OMOP Forskningsdata

#### Domain 4: Data & Analytics (Analytics & ML)
```
Name: Data & Analytics
English Name: Data & Analytics
Description: Governance domain för machine learning features, prediktiva modeller, och analytiska datastores.
Parent Domain: (none - root level)
Domain Owners: joandolf@microsoft.com
```

**Länka Data Product:**
- ML Feature Store

### Steg 3: Länka Data Products till Domains

För varje Data Product:
1. Unified Catalog → Data products
2. Öppna data product (t.ex. "BrainChild Barncancerforskning")
3. Edit/Properties
4. Välj "Governance Domain"
5. Välj rätt domain från dropdown
6. Save

### Steg 4: Verifiera

Efter skapande ska du se:
- Enterprise glossary → Governance domains tab: **4 governance domains**
- Data products → Explore by governance domain: **4 domains med länkade products**

## RENSA UPP GAMLA WORKAROUND

### Ta bort Glossary Term Workaround
Våra glossary terms (CDM, GPM, CR, MLA) är inte governance domains. Ta bort dem:

1. Unified Catalog → Enterprise glossary → Glossary terms
2. Filtrera på category "Governance Domains"
3. Ta bort terms:
   - Domain: Clinical Data Management (CDM)
   - Domain: Genomics & Precision Medicine (GPM)
   - Domain: Cancer Registry (CR)
   - Domain: ML & Analytics (MLA)
4. Ta bort category "Governance Domains" (om tom)

### Rensa Meanings från Data Products
Data products kan fortfarande ha meanings-länkar till gamla glossary terms:

```python
# Kör detta script för att rensa:
python scripts/cleanup_old_domain_workaround.py
```

## VARFÖR DETTA INTE FUNGERADE MED API

Governance Domains är en **UI-only** feature i Purview:
- Ingen REST API för create/update/delete
- Purview_DataDomain entity type existerar men skapas bara via UI
- Glossary terms och Governance domains är separata koncept
- Purview team har inte publicerat API dokumentation för domains

**Konklusion:** Governance domains måste skapas manuellt i Portal UI. REST APIs fungerar inte.

## ESTIMERAD TID
- Skapa 4 domains: ~10 minuter
- Länka 4 data products: ~5 minuter
- Rensa gamla workaround: ~5 minuter
- **Total: ~20 minuter**

## NÄSTA STEG EFTER MANUELL SKAPANDE
1. Verifiera alla 4 domains syns i UI
2. Verifiera alla 4 products har domain-länkar
3. Kör: `python scripts/verify_domains_complete.py`
4. Fortsätt med Fabric OneLake integration (när mi-purview permissions propagerat)
