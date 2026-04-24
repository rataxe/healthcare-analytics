"""
Fix audience enum values in DATA_PRODUCTS.
Valid values: DataEngineer, DataScientist, DataAnalyst, BIEngineer
"""
import ast, re
from pathlib import Path

# Valid audience values
VALID = {'DataEngineer', 'DataScientist', 'DataAnalyst', 'BIEngineer'}

# Mapping from invalid to valid
MAPPING = {
    'DataSteward': 'DataAnalyst',
    'SecurityOfficer': 'DataAnalyst',
    'OperationsManager': 'BIEngineer',
    'Pharmacist': 'DataScientist',
    'ClinicalManager': 'BIEngineer',
    'Researcher': 'DataScientist',
    'Bioinformatician': 'DataScientist',
    'Radiologist': 'DataAnalyst',
    'LabManager': 'DataAnalyst',
    'Architect': 'DataEngineer',
    'IntegrationDeveloper': 'DataEngineer',
    'IntegrationArchitect': 'DataEngineer',
    'ClinicalInformatician': 'DataScientist',
    'PlatformEngineer': 'DataEngineer',
    'PublicHealthLead': 'BIEngineer',
    'RiskOfficer': 'DataAnalyst',
    'ComplianceOfficer': 'DataAnalyst',
    'Auditor': 'DataAnalyst',
    'MLPlatformEngineer': 'DataEngineer',
}

file_path = Path('scripts/purview_data_products.py')
content = file_path.read_text(encoding='utf-8')

# Parse to find DATA_PRODUCTS
module = ast.parse(content)
products = None
for node in module.body:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if getattr(target, 'id', None) == 'DATA_PRODUCTS':
                products = ast.literal_eval(node.value)
                break
    if products is not None:
        break

# Normalize audience for each product
changes = 0
for product in products:
    if 'audience' in product:
        old = product['audience']
        new = []
        for aud in old:
            if aud in VALID:
                new.append(aud)
            elif aud in MAPPING:
                new.append(MAPPING[aud])
                changes += 1
            else:
                print(f"Unknown audience: {aud}")
        product['audience'] = new

print(f"Fixed {changes} invalid audience values")

# Now rewrite the file with normalized audiences
# Use regex to replace audience lists
for product in products:
    old_list = str(product['audience'])  # e.g. ['DataEngineer', 'BIEngineer']
    # Find this exact list in the file and replace it
    # This is tricky, so let's regenerate the entire DATA_PRODUCTS dict

import json
new_data_products = "DATA_PRODUCTS = [\n"
for product in products:
    new_data_products += "    {\n"
    for key, value in product.items():
        if key == 'audience':
            # Format audience as a list literal
            aud_str = str(value).replace("'", '"')
            new_data_products += f'        "{key}": {aud_str},\n'
        elif isinstance(value, str):
            # Escape quotes
            escaped = value.replace('"', '\\"')
            new_data_products += f'        "{key}": "{escaped}",\n'
        elif isinstance(value, list):
            # Format list
            formatted = json.dumps(value)
            new_data_products += f'        "{key}": {formatted},\n'
        elif value is None:
            new_data_products += f'        "{key}": None,\n'
        else:
            new_data_products += f'        "{key}": {value},\n'
    new_data_products += "    },\n"
new_data_products += "]\n"

print("\nScript would regenerate DATA_PRODUCTS, but manual regex replacement is safer.")
print(f"Total products to fix: {len(products)}")
print("\nAudience changes:")
for product in products:
    if product['audience']:
        print(f"  {product['name']}: {product['audience']}")
