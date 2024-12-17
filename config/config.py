import yaml
from pathlib import Path

def load_config(config_path: str = "config.yaml") -> dict:
    CONFIG_PATH = Path(__file__).parent / config_path
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
            return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Plik konfiguracyjny nie został znaleziony: {CONFIG_PATH}")
    except yaml.YAMLError as e:
        raise ValueError(f"Błąd podczas parsowania pliku YAML: {e}")

CONFIG = load_config()
