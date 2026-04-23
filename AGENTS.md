# AGENTS.md
# GitHub Copilot läser denna fil automatiskt i alla konversationer.
# Styr vilket beteende och vilka regler som gäller per uppgiftstyp.

---

## Routing – välj mode baserat på nyckelord

Läs uppgiftens nyckelord och välj mode automatiskt utan att fråga:

| Nyckelord | Mode |
|-----------|------|
| neo4j, cypher, kunskapsgraf, knowledge graph, ppr, merge, ontologi, triple, entity linking | `neo4j-expert` |
| fabric, pyspark, delta, medallion, bronze, silver, gold, scd, lakehouse, notebook, eventhouse, kql | `fabric-engineer` |
| langchain, openai, retrieval, rag, embedding, prompt, llm, langsmith, azure openai | `ai-engineer` |
| fastapi, route, pydantic, rest, api, endpoint, middleware, httpx | `backend-dev` |
| pytest, test, ci, cd, github actions, docker, pipeline, deploy, coverage | `devops` |
| word, powerpoint, draw.io, presentation, dokumentation, kundrapport, slide | `documentation` |
| provision, bicep, azd, resource group, key vault, managed identity, service principal, private endpoint, rbac, swedencentral, subscription, capacity, sku, tenant settings, entra, federation, siths | `azure-architect` |
| purview, governance, glossary, classification, lineage, sensitivity, data catalog, data product, metadata, steward, domain | `purview-governance` |
| ml, machine learning, scikit, lightgbm, synapseml, mlflow, predict, model, train, evaluate, los, readmission, feature engineering, auc, f1, confusion matrix | `ml-engineer` |

Om uppgiften berör flera domäner – svara i det primära modet och nämn vilka delar som kräver ett annat mode.

---

## Gemensamma regler – gäller alltid

### Python
- Type hints alltid – `def func(x: str) -> list[dict]:`
- `pathlib.Path` för filsökvägar, aldrig `os.path`
- f-strängar alltid, aldrig `.format()` eller `%`
- `logging`-modulen alltid, aldrig `print()`
- `dataclasses` för datamodeller, `pydantic` för API-modeller

### Säkerhet
- Inga hårdkodade credentials – alltid miljövariabler eller Key Vault
- `DefaultAzureCredential()` i Python – testar Managed Identity automatiskt i Azure
- Känslig data loggas aldrig

---

## Azure – absoluta regler

- `swedencentral` alltid för GDPR-känslig kunddata
- Managed Identity i produktion – aldrig Service Principal med lösenord i kod
- RBAC på Key Vault – `--enable-rbac-authorization true`, aldrig Access Policies
- Tags på alla resurser: `environment`, `project`, `managedBy`
- `what-if` innan varje Bicep-deployment i produktion
- Minsta möjliga behörighet – aldrig Owner på subscription-nivå

---

## Fabric – absoluta regler

- `spark`-variabeln är global i Fabric Notebooks – aldrig `SparkSession.builder`
- Delta-format alltid – `spark.read.format("delta")`
- Medallion-arkitektur alltid: bronze → silver → gold

---

## Neo4j / Kunskapsgraf – absoluta regler

- **LLM genererar ALDRIG Cypher** – alla queries är hårdkodade konstanter
- **LLM resonerar ALDRIG kliniskt** – formaterar bara text från verifierad subgraf
- **All retrieval är deterministisk** – samma input ger alltid samma output
- `MERGE` alltid, aldrig `CREATE` för noder som kan finnas
- `content_hash` på alla noder för inkrementell ingestning

---

## Dokumentation – regler

- Svenska för tekniska dokument till kund
- Engelska för kod, kommentarer och GitHub-content
- Microsoft-dokumentationsstil: tydlig, konsultativ, lösningsorienterad

---

## Healthcare Analytics – projektspecifika regler

### Datamodell
- Azure SQL → Fabric Lakehouse (Bronze/Silver/Gold) → ML → Power BI
- OMOP CDM-kompatibla tabeller i Silver-lagret
- ICD-10 kodning för diagnoser (I50.x = hjärtsvikt, J44.x = KOL, E11.x = diabetes typ 2, I63.x = stroke, N18.x = CKD)

### ML-modeller
- LOS (Length of Stay) prediction – regressionsproblem
- Readmission prediction – binär klassificering
- SynapseML / Scikit-learn / LightGBM med MLflow-tracking
- Alla experiment loggas i MLflow med metriker, parametrar och artefakter

### Purview Governance
- 188 glossary terms organiserade i domäner
- Sensitivity labels: Confidential, Internal, Public
- Lineage spåras automatiskt från Bronze → Silver → Gold

### Copilot-beteende
- Föreslå alltid parameteriserade SQL-queries (aldrig string-concatenation)
- Inkludera docstrings och type hints i all genererad Python-kod
- Referera till medallion-lager korrekt: `bronze_*`, `silver_*`, `gold_*`
- Respektera befintliga namnkonventioner i `src/notebooks/`

---

## MCP-servrar – bekräftade konfigurationer

```
draw.io:          npx @modelcontextprotocol/server-drawio
azure:            npx @azure/mcp server start
github:           Docker ghcr.io/github/github-mcp-server
microsoft-docs:   SSE https://learn.microsoft.com/api/mcp
```

---

## Projektspecifik kontext

Se `.claude/PROJECT_OVERVIEW.md` för fullständig projektkontext.
Se `.claude/QUICK_REFERENCE.md` för snabbreferens.
