import yaml

def load_config(config_path):
    if config_path.endswith('.yml') or config_path.endswith('.yaml'):
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    else:
        return {}