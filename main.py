import argparse
from pathlib import Path
import yaml
from etl.config import Config
from etl.etl import etl


def load_config(
    operation: str,
    config_path: str = "configs/dev.yml",
    selected: list[str] | None = None,
) -> Config:
    config_file = Path(config_path)
    with open(config_file, "r") as f:
        raw = yaml.safe_load(f)

    if selected is not None:
        raw["selected"] = selected

    return Config(operation=operation, **raw)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Zecca ETL pipeline")
    parser.add_argument(
        "verb",
        choices=["injest", "transform", "test", "full"],
        help="Pipeline stage to run",
    )
    parser.add_argument(
        "--select",
        type=lambda s: [name.strip() for name in s.split(",")],
        default=None,
        metavar="NAMES",
        help="Comma-separated list of model/source names to include",
    )
    parser.add_argument(
        "--config",
        default="configs/dev.yml",
        metavar="PATH",
        help="Path to the YAML config file (default: configs/config.yml)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    config = load_config(args.verb, args.config, args.select)
    etl(config)
