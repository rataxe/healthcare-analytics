---
name: neo4j-knowledge-graph
description: >
  Neo4j knowledge graph development for deterministic medical AI retrieval systems.
  Use this skill whenever the task involves Neo4j graph databases, Cypher queries,
  knowledge graph design, ontology modelling, graph traversal algorithms (PPR,
  template-traversal), node ingestning with hash-tracking, or connecting Neo4j to
  LangChain / Azure OpenAI pipelines. Also use for graph schema design, MERGE
  patterns, relationship modelling, and graph data science (GDS) operations such
  as Personalized PageRank. Always trigger this skill when the user mentions
  Neo4j, Cypher, kunskapsgraf, knowledge graph, graph traversal, PPR, graph
  database, node, edge, relationship, ontologi, or triple extraction.
triggers:
  - neo4j
  - cypher
  - kunskapsgraf
  - knowledge graph
  - graph traversal
  - PPR
  - personalized pagerank
  - MERGE
  - graph database
  - ontologi
  - triple extraction
  - entity linking
  - subgraf
  - hub-explosion
  - graph data science
  - GDS
---

# neo4j-knowledge-graph

Skill för Neo4j-kunskapsgrafer med deterministisk retrieval.
Primär domän: medicinsk AI, men mönstren gäller för alla kunskapsgrafer.

**Grundprincipen som aldrig får brytas:**
LLM genererar ALDRIG Cypher. Alla graph-queries är hårdkodade konstanter.
LLM resonerar ALDRIG kliniskt – den formaterar text från verifierad subgraf.

---

## Beslutsträd

```
Vad ska göras?
├── Designa schema / ontologi         → ONTOLOGIDESIGN
├── Skapa / uppdatera noder           → INGESTNING (MERGE + hash)
├── Hämta subgraf (retrieval)
│   ├── Känd frågetyp                 → TEMPLATE-TRAVERSAL
│   └── Okänd / komplex fråga         → PPR-FALLBACK
├── Graph Data Science (GDS)          → GDS
└── LangChain-integration             → LANGCHAIN
```

---

## ONTOLOGIDESIGN

### Namnkonventioner

```cypher
// Nodtyper: PascalCase
(:Drug), (:DoseInfo), (:RenalLevel)

// Kanter: SCREAMING_SNAKE_CASE
-[:HAS_DOSE]->
-[:ADJUSTED_FOR]->

// Properties: snake_case
node_id, egfr_min, content_hash
```

---

## INGESTNING – MERGE + hash-tracking

```python
from hashlib import sha256
from dataclasses import dataclass, field
import json

@dataclass
class OntologyNode:
    node_id:    str
    node_type:  str
    properties: dict
    domain:     str
    _hash: str = field(init=False)

    def __post_init__(self):
        content = json.dumps(
            {"id": self.node_id, "type": self.node_type, "props": self.properties},
            sort_keys=True
        )
        self._hash = sha256(content.encode()).hexdigest()[:12]


def sync_node(session, node: OntologyNode) -> str:
    existing = session.run(
        "MATCH (n {node_id: $id}) RETURN n.content_hash AS h",
        id=node.node_id
    ).single()

    if existing and existing["h"] == node._hash:
        return "skipped"

    session.run(f"""
        MERGE (n:{node.node_type} {{node_id: $id}})
        SET n += $props,
            n.content_hash = $hash,
            n.updated_at   = datetime()
    """, id=node.node_id, props=node.properties, hash=node._hash)

    return "updated"
```

---

## TEMPLATE-TRAVERSAL (deterministisk retrieval)

```python
TEMPLATES = {
    "DOSE": """
        MATCH (d:Drug {name: $drug})-[:HAS_DOSE]->(dose:DoseInfo)
        RETURN dose.amount, dose.unit, dose.frequency, dose.route, dose.phase
        ORDER BY dose.amount ASC
    """,
    "DOSE_RENAL": """
        MATCH (d:Drug {name: $drug})-[:HAS_DOSE]->(dose:DoseInfo)
              -[:ADJUSTED_FOR]->(r:RenalLevel)
        RETURN dose.amount, dose.unit,
               r.egfr_min, r.egfr_max, r.adjustment_factor, r.note
        ORDER BY r.egfr_min DESC
    """,
    "CONTRAINDICATION": """
        MATCH (d:Drug {name: $drug})-[:CONTRAINDICATED_IN]->(c:Condition)
        RETURN c.name, c.severity, c.active
        ORDER BY c.severity DESC
    """,
    "INTERACTION": """
        MATCH (d1:Drug {name: $drug1})-[i:INTERACTS_WITH]-(d2:Drug {name: $drug2})
        RETURN i.type, i.severity, i.mechanism, i.clinical_consequence
    """,
    "ENTITY_LOOKUP": """
        MATCH (n)
        WHERE toLower(n.name) CONTAINS toLower($search_term)
           OR toLower(n.node_id) = toLower($search_term)
        RETURN n.node_id AS node_id, labels(n)[0] AS node_type, n.name AS name
        LIMIT 10
    """,
}
```

### Router – regelbaserad klassificering

```python
import re
from enum import Enum

class QueryType(Enum):
    DOSE_RENAL     = "dose_renal"
    CONTRAIND      = "contraindication"
    INTERACTION    = "interaction"
    DOSE           = "dose"
    GUIDELINE      = "guideline"
    FALLBACK       = "fallback"

PATTERNS = {
    QueryType.DOSE_RENAL:  r"(njure|egfr|njursvikt|renal|kreatinin|dialys)",
    QueryType.CONTRAIND:   r"(kontraindik|undvika|farligt|inte ge|förbjud|kan man ge)",
    QueryType.INTERACTION: r"(interager|kombinera|samtidigt|ihop med|tillsammans med)",
    QueryType.GUIDELINE:   r"(riktlinje|guideline|esc|rekommend|förstahand)",
    QueryType.DOSE:        r"(dose?r|dos |mg |startdos|underhåll|titrering|administrer)",
}

def classify_query(question: str) -> QueryType:
    q = question.lower()
    for qtype, pattern in PATTERNS.items():
        if re.search(pattern, q):
            return qtype
    return QueryType.FALLBACK
```

---

## PPR-FALLBACK (Personalized PageRank)

```python
import networkx as nx

def ppr_networkx(driver, start_node_id: str,
                 alpha: float = 0.85,
                 threshold: float = 0.20,
                 max_nodes: int = 50) -> list[dict]:
    with driver.session() as s:
        neighbors = list(s.run(
            "MATCH (start {node_id: $id})-[*1..3]-(n) "
            "RETURN DISTINCT n.node_id AS node_id, "
            "       labels(n)[0] AS node_type, properties(n) AS props",
            id=start_node_id
        ))

    G = nx.DiGraph()
    G.add_node(start_node_id)
    for n in neighbors:
        G.add_node(n["node_id"])
        G.add_edge(start_node_id, n["node_id"])

    scores = nx.pagerank(G, alpha=alpha, personalization={start_node_id: 1.0}, max_iter=100)

    return sorted(
        [{"node_id": nid, "ppr_score": s,
          **next((dict(n) for n in neighbors if n["node_id"] == nid), {})}
         for nid, s in scores.items()
         if nid != start_node_id and s >= threshold],
        key=lambda x: x["ppr_score"], reverse=True
    )[:max_nodes]
```

---

## INDEX – skapa för prestanda

```cypher
CREATE INDEX drug_node_id   IF NOT EXISTS FOR (n:Drug)           ON (n.node_id)
CREATE INDEX drug_name      IF NOT EXISTS FOR (n:Drug)           ON (n.name)
CREATE INDEX disease_name   IF NOT EXISTS FOR (n:Disease)        ON (n.name)
CREATE INDEX content_hash   IF NOT EXISTS FOR (n)                ON (n.content_hash)
```

---

## ANSLUTNING

```python
from neo4j import GraphDatabase, Driver
from functools import lru_cache

@lru_cache(maxsize=1)
def get_driver() -> Driver:
    return GraphDatabase.driver(
        settings.NEO4J_URI,
        auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
        max_connection_pool_size=50,
        connection_timeout=15,
    )
```

---

## Vad du alltid gör

- `MERGE` – aldrig `CREATE` för noder som kan finnas
- `content_hash` på varje nod för inkrementell ingestning
- Parametrisera ALLA Cypher-queries – aldrig strängkonkatenering
- Returnera `node_id` explicit – aldrig intern Neo4j-id
- Index på `node_id` och `name` vid setup

## Vad du aldrig gör

- Låt LLM generera eller modifiera Cypher
- `CREATE` när `MERGE` fungerar
- Hårda strings i Cypher: `WHERE n.name = "` + variable + `"`
- `MATCH (n) RETURN n` i produktion (hela grafen)
- Skippa parametrisering – risk för Cypher injection
