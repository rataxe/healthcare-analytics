---
name: azure-provisioning
description: >
  Azure resource provisioning, setup och konfiguration för Microsoft Fabric
  och AI-projekt. Använd denna skill för ALLA uppgifter som rör att sätta upp
  Azure-resurser: Bicep-mallar, Azure CLI-skript, azd (Azure Developer CLI),
  Service Principal-skapande, Managed Identity, private endpoints, RBAC/roll-
  tilldelningar, Key Vault, nätverksisolering, Entra ID-integration och
  Fabric-kapacitet. Trigga alltid vid ord som: provision, deploy, sätt upp,
  skapa resurser, resource group, bicep, terraform, azd, service principal,
  managed identity, private endpoint, RBAC, roll, key vault, entra, tenant,
  subscription, nätverksisolering, capacity, F64, SKU, azureresurser.
triggers:
  - provision
  - bicep
  - terraform
  - azure cli
  - azd
  - "service principal"
  - "managed identity"
  - "private endpoint"
  - RBAC
  - "key vault"
  - "resource group"
  - "sätt upp"
  - "skapa resurser"
  - capacity
  - "tenant settings"
  - "entra id"
---

# azure-provisioning

Skill för att sätta upp och konfigurera Azure-resurser för Fabric- och AI-projekt.
Täcker hela kedjan: infrastruktur, identitet, nätverk och behörigheter.

**Grundprinciper som aldrig bryts:**
- Managed Identity i produktion – aldrig Service Principal med lösenord i kod
- Sweden Central alltid för GDPR-känslig data (SU, Region Halland, kommuner)
- Minsta möjliga behörighet (least privilege) – aldrig Owner på subscription-nivå
- Alla hemligheter i Key Vault – aldrig i kod, config-filer eller environment variables i produktion

---

## Beslutsträd – vilket verktyg?

```
Vad ska göras?
├── Nytt komplett projekt från scratch     → azd (Azure Developer CLI)
├── Infrastruktur som kod (IaC)            → Bicep (avsnittet BICEP)
├── Snabb enstaka resurs / test            → Azure CLI (avsnittet CLI)
├── Service Principal / identitet          → Avsnittet IDENTITET
├── Behörigheter / RBAC                    → Avsnittet RBAC
├── Private endpoint / nätverksisolering   → Avsnittet NÄTVERK
├── Key Vault / hemligheter                → Avsnittet KEY VAULT
└── Fabric-kapacitet och workspace         → Avsnittet FABRIC SETUP
```

---

## AZURE DEVELOPER CLI (azd)

```bash
winget install microsoft.azuredeveloper
azd version
azd init --template azure-fabric-starter
azd up
```

### Miljöhantering

```bash
azd env new dev
azd env new prod
azd env select prod
azd env get-values
```

---

## BICEP – Infrastruktur som kod

### Standardstruktur

```
infra/
├── main.bicep
├── main.bicepparam
└── modules/
    ├── fabric.bicep
    ├── keyvault.bicep
    ├── networking.bicep
    └── identity.bicep
```

### main.bicep – grundstruktur

```bicep
targetScope = 'resourceGroup'

@allowed(['dev', 'test', 'prod'])
param environment string
param location string = 'swedencentral'
param projectName string

var prefix = '${projectName}-${environment}'
var tags = {
  environment: environment
  project: projectName
  managedBy: 'bicep'
}

module kv 'modules/keyvault.bicep' = {
  name: 'keyvault'
  params: { name: '${prefix}-kv', location: location, tags: tags }
}

module identity 'modules/identity.bicep' = {
  name: 'identity'
  params: { name: '${prefix}-id', location: location, tags: tags }
}
```

### modules/keyvault.bicep

```bicep
param name string
param location string
param tags object

resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: name
  location: location
  tags: tags
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enablePurgeProtection: true
    publicNetworkAccess: 'Disabled'
    networkAcls: { defaultAction: 'Deny', bypass: 'AzureServices' }
  }
}

output keyVaultId string = kv.id
output keyVaultUri string = kv.properties.vaultUri
```

### Deploya Bicep

```bash
az group create --name rg-PROJECT-dev --location swedencentral

az deployment group what-if \
  --resource-group rg-PROJECT-dev \
  --template-file infra/main.bicep \
  --parameters infra/main.bicepparam

az deployment group create \
  --resource-group rg-PROJECT-dev \
  --template-file infra/main.bicep \
  --parameters infra/main.bicepparam
```

---

## IDENTITET

### Managed Identity (föredragen i produktion)

```bash
az identity create \
  --name id-PROJECT \
  --resource-group rg-PROJECT-dev \
  --location swedencentral

PRINCIPAL_ID=$(az identity show --name id-PROJECT --resource-group rg-PROJECT-dev --query principalId -o tsv)
```

### Service Principal (bara för CI/CD och dev)

```bash
az ad sp create-for-rbac \
  --name "sp-PROJECT-github" \
  --role Contributor \
  --scopes /subscriptions/SUB_ID/resourceGroups/rg-PROJECT-dev \
  --sdk-auth
```

---

## RBAC – Behörigheter

```bash
# Managed Identity → Key Vault Secrets User
az role assignment create \
  --assignee $PRINCIPAL_ID \
  --role "Key Vault Secrets User" \
  --scope $(az keyvault show --name kv-PROJECT-dev --query id -o tsv)

# SP → Storage Blob Data Contributor (för OneLake/ADLS)
az role assignment create \
  --assignee $SP_OBJECT_ID \
  --role "Storage Blob Data Contributor" \
  --scope /subscriptions/SUB/resourceGroups/RG/providers/Microsoft.Storage/storageAccounts/SA_NAME
```

### Python – Managed Identity i kod

```python
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

credential = DefaultAzureCredential()
kv_client = SecretClient(
    vault_url="https://kv-PROJECT-dev.vault.azure.net/",
    credential=credential,
)
secret = kv_client.get_secret("neo4j-password")
```

---

## FABRIC SETUP – Workspace och kapacitet

```python
import requests

def create_fabric_workspace(token: str, display_name: str, capacity_id: str) -> dict:
    resp = requests.post(
        "https://api.fabric.microsoft.com/v1/workspaces",
        headers={"Authorization": f"Bearer {token}"},
        json={"displayName": display_name, "capacityId": capacity_id},
    )
    resp.raise_for_status()
    return resp.json()
```

---

## Vad du alltid gör

- `swedencentral` som region för GDPR-känslig kunddata
- Managed Identity i produktionskod – aldrig hårdkodade credentials
- RBAC-baserad Key Vault – `--enable-rbac-authorization true`
- Tags på ALLA resurser: `environment`, `project`, `managedBy`
- `what-if` innan varje Bicep-deployment i produktion
- Minsta möjliga behörighet – börja med Reader, eskalera vid behov

## Vad du aldrig gör

- Ge `Owner` på subscription-nivå
- Lagra hemligheter i `.env`-filer i produktion
- Skapa resurser manuellt utan att dokumentera i Bicep
- Använda East US som region för svenska kundprojekt
- Dela Service Principal-credentials via mail eller Teams
