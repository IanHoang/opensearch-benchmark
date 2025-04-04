import yaml
import json

def load_config(config_path):
    if config_path.endswith('.yml') or config_path.endswith('.yaml'):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    else:
        return {}
    
def write_chunk(data, file_path):
    with open(file_path, 'a') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')
    return len(data)