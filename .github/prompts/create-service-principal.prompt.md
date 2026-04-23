---
description: "Skapa en Service Principal med rätt RBAC"
mode: ask
---

Skapa en Service Principal för projektet med minsta möjliga behörighet.

## Input

| Fält | Värde |
|------|-------|
| SP-namn | `${input:spName:Service Principal-namn (t.ex. sp-healthcare-analytics-dev)}` |
| Syfte | `${input:purpose:Vad ska SP:n användas till? (t.ex. Fabric notebooks, CI/CD)}` |
| Miljö | `${input:environment:dev / test / prod}` |

## Steg att generera

1. `az ad sp create-for-rbac` med rätt scope
2. RBAC-rolltilldelning (minsta behörighet)
3. Spara credentials i Key Vault (aldrig i kod)
4. Fabric tenant-inställningar som krävs (om Fabric-åtkomst)

## Regler

- Aldrig Owner på subscription-nivå
- `--years 1` max för icke-prod
- Visa alltid vilka tenant-inställningar som måste aktiveras manuellt
