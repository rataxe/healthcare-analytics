---
applyTo: "**/*.py"
---

# Python-kodstil

Dessa regler gäller automatiskt för alla `.py`-filer i projektet.

## Obligatoriskt

- **Type hints alltid** – `def func(x: str) -> list[dict]:`
- **pathlib.Path** för filsökvägar – aldrig `os.path`
- **f-strängar** alltid – aldrig `.format()` eller `%`
- **logging** alltid – aldrig `print()` (undantag: CLI-verktyg med `typer`/`click`)
- **dataclasses** för datamodeller – **Pydantic** för API-modeller

## Import-ordning

```python
# 1. Standardbibliotek
import logging
from pathlib import Path

# 2. Tredjepartsbibliotek
from pydantic import BaseModel

# 3. Interna moduler
from src.config import Settings
```

## Namnkonventioner

| Element | Stil | Exempel |
|---------|------|---------|
| Funktioner | `snake_case` | `load_patients()` |
| Klasser | `PascalCase` | `PatientModel` |
| Konstanter | `SCREAMING_SNAKE` | `MAX_RETRIES` |
| Privata | `_prefix` | `_internal_helper()` |

## Felhantering

```python
# ✅ RÄTT – specifik exception med logging
try:
    result = client.query(cypher, params)
except ServiceUnavailable as e:
    logger.error("Neo4j connection failed: %s", e)
    raise

# ❌ FEL – bred exception utan logging
try:
    result = client.query(cypher, params)
except Exception:
    pass
```

## Azure-anslutningar

```python
# ✅ RÄTT – DefaultAzureCredential (testar MI automatiskt i Azure)
from azure.identity import DefaultAzureCredential
credential = DefaultAzureCredential()

# ❌ FEL – hårdkodade credentials
client = SecretClient(vault_url=url, credential=ClientSecretCredential(...))
```
