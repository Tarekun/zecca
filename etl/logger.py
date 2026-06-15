import logging
import sys
from datetime import datetime
from pathlib import Path


def _setup() -> None:
    started_at = datetime.now().strftime("%Y%m%d_%H%M%S")

    logs_dir = Path(__file__).parent.parent / "logs"
    logs_dir.mkdir(exist_ok=True)

    fmt = logging.Formatter("%(asctime)s [%(filename)s:%(lineno)d] %(message)s")

    file_handler = logging.FileHandler(logs_dir / f"startedat_{started_at}.log")
    file_handler.setFormatter(fmt)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)

    app_logger = logging.getLogger("etl")
    app_logger.setLevel(logging.INFO)
    app_logger.addHandler(file_handler)
    app_logger.addHandler(stream_handler)
    app_logger.propagate = False


_setup()


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
