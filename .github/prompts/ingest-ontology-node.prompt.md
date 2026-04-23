---
description: "Ingestning av ontologinod till Neo4j"
mode: ask
---

Skapa en ny ontologinod-typ i kunskapsgrafen.

## Input

| Fält | Värde |
|------|-------|
| Nodtyp | `${input:nodeType:Nodtyp i PascalCase (t.ex. Drug, Disease, Biomarker)}` |
| Layer | `${input:layer:core / domain / instance}` |
| Egenskaper | `${input:properties:Kommaseparerade properties (t.ex. name, atc, category)}` |

## Generera

1. Python `OntologyNode`-klass med `content_hash`
2. Cypher MERGE-mönster (aldrig CREATE)
3. `sync_node`-wrapper som hoppar över oförändrade noder
4. Unit test med pytest

## Krav

- `content_hash` ska baseras på `sha256` av sorterad JSON
- `node_id` ska vara obligatorisk
- Parametriserad Cypher – aldrig strängkonkatenering
