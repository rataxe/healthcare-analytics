---
description: "Ny Neo4j-query med template-traversal"
mode: ask
---

Skapa en ny Neo4j-query med template-traversal-mönstret.

## Input

| Fält | Värde |
|------|-------|
| Template-namn | `${input:templateName:Template-namn i snake_case (t.ex. drug_interactions)}` |
| Starttyp | `${input:startType:Startnodtyp (t.ex. Drug, Disease)}` |
| Fråga | `${input:question:Vilken fråga ska queryn besvara?}` |

## Generera

1. **Cypher-template** (hårdkodad, parametriserad – LLM genererar ALDRIG Cypher)
2. **Python-wrapper** med session-hantering
3. **Textformateringsfunktion** som LLM kan använda
4. **Unit test** med pytest

## Krav

- Parametriserad Cypher med `$`-variabler – aldrig f-strängar
- Template registreras i template-registret
- Felhantering: returnera tomt resultat (aldrig exception) vid 0 träffar
- Logga alltid query-tid
