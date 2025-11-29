from pathlib import Path
import yaml
from etl.etl import etl


def load_config(config_path: str = "configs/config.yml") -> dict:
    """Load configuration from YAML file."""
    config_file = Path(config_path)
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    required_keys = []
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required config key: '{key}' in {config_file}")

    # Validate base_directory is a string and create if it doesn't exist
    # base_dir = Path(config["base_directory"])
    # base_dir.mkdir(parents=True, exist_ok=True)
    # config["base_directory"] = str(base_dir.absolute())

    return config


config = load_config()
etl(config)
