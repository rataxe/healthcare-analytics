"""Patch 04_omop_diag.py: replace 'raise' with 'pass' in except blocks."""
import re

with open("src/notebooks/04_omop_diag.py", "r", encoding="utf-8") as f:
    content = f.read()

# Replace "    raise\n" with "    pass  # continue\n"
count = content.count("    raise\n")
content = content.replace("    raise\n", "    pass  # continue\n")
print(f"Replaced {count} raise statements with pass")

with open("src/notebooks/04_omop_diag.py", "w", encoding="utf-8") as f:
    f.write(content)
