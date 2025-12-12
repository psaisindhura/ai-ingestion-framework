import yaml
import os

def load_config():
    base_path = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_path, "application.yaml")

    with open(config_path) as f:
        return yaml.safe_load(f)