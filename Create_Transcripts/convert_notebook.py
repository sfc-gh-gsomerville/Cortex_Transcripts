import json

# Parse the original notebook
with open('create_cursor_demo_tables_with_code_cells.ipynb', 'r') as f:
    notebook = json.load(f)

# Convert all code cells to SQL cells
for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        # Change the cell metadata to specify SQL language
        cell['metadata'] = {
            'language': 'sql',
            'trusted': True
        }

# Update the notebook metadata to include SQL kernel
notebook['metadata']['kernelspec'] = {
    'display_name': 'SQL',
    'language': 'sql',
    'name': 'sql'
}

# Write the updated notebook
with open('create_cursor_demo_tables_with_sql_cells.ipynb', 'w') as f:
    json.dump(notebook, f, indent=2) 