---
description: "Provisionera ett nytt Azure-projekt med Bicep/azd"
mode: ask
---

Sätt upp infrastruktur för ett nytt Azure-projekt.

## Input

| Fält | Värde |
|------|-------|
| Projektnamn | `${input:projectName:Kortnamn för projektet (t.ex. healthcare-analytics)}` |
| Miljö | `${input:environment:dev / test / prod}` |
| Resursgrupp | `${input:rgName:Resource Group-namn (lämna tomt för auto)}` |

## Generera

### 1. Bicep-moduler
- `main.bicep` med parametrar
- Key Vault (RBAC-aktiverad)
- Managed Identity
- Log Analytics Workspace

### 2. Deployment
- `what-if` output
- Deployment-kommando

### 3. Post-deployment
- RBAC-rolltilldelningar
- Diagnostikloggar

## Absoluta regler

- Region: `swedencentral` alltid
- Managed Identity i produktion
- RBAC på Key Vault – aldrig Access Policies
- Tags: `environment`, `project`, `managedBy`
- what-if innan varje deployment
