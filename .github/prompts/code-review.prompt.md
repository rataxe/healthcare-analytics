---
description: "Kod-review med projektspecifika regler"
mode: ask
---

Gör en kod-review av det markerade kodblocket.

Kontrollera:
1. **Type hints** – saknas det?
2. **Loggning** – används `print()` istället för `logging`?
3. **Säkerhet** – finns hårdkodade credentials?
4. **Pathlib** – används `os.path` istället för `pathlib.Path`?
5. **f-strängar** – används `.format()` eller `%`?
6. **Felhantering** – saknas try/except för I/O-operationer?

Rapportera:
- ✅ OK eller ❌ Problem + förslag på fix
- Sortera efter allvarlighetsgrad: 🔴 Kritisk → 🟡 Varning → 🟢 Förbättring

#selection
