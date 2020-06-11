import json
old_json_filepath = '/home/natasha/mycode/python/npm_npadb_admin/data/table-list.json'
new_json_filepath = '/home/natasha/CloudStation/npadb/all-the-stations/data/metadata.json'

with open(old_json_filepath, 'r') as f:
    data = json.load(f)

for table in data.values():
    table['indexable_names'] = []

with open(new_json_filepath, 'w') as f:
    json.dump(data, f, indent=4)
