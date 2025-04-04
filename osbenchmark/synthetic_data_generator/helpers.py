import yaml
import json

from osbenchmark.synthetic_data_generator.types import DEFAULT_GENERATION_SETTINGS

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

def get_generation_settings(input_config: dict) -> dict:
    '''
    Grabs the user's config's generation settings and compares it with the default generation settings.
    If there are missing fields in the user's config, it populates it with the default values
    '''
    generation_settings = DEFAULT_GENERATION_SETTINGS
    user_generation_settings = input_config.get('settings', {})

    if user_generation_settings == {}:
        return generation_settings
    else:
        # Traverse and update valid settings that user specified.
        for k, v in generation_settings.items():
            if k in user_generation_settings and user_generation_settings[k] is not None:
                generation_settings[k] = user_generation_settings[k]
            else:
                continue

        return generation_settings
