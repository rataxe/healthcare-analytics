"""Preview the Fabric notebook content for silver features."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "."))
from fix_notebooks import py_to_fabric_notebook, LAKEHOUSES
from pathlib import Path

nb_path = Path(__file__).resolve().parent.parent / "src" / "notebooks" / "02_silver_features.py"
content = py_to_fabric_notebook(nb_path, "silver_lakehouse", ["bronze_lakehouse"])
print(content[:3000])
print("\n... (truncated) ...")
print(f"\nTotal length: {len(content)} chars")
print(f"Cell count: {content.count('# CELL ********************')}")
