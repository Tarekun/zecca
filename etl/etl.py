from contextlib import contextmanager
import os
from etl.config import Config
from etl.logger import get_logger
from etl.sources import ingest_ticker_daily, ingest_ticker_hourly
from etl.telegrambot import send_message_to_group
from etl.transformation.builder import build_everything

logger = get_logger(__name__)


@contextmanager
def temporary_working_directory(path):
    """Context manager to temporarily change working directory."""
    original_cwd = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(original_cwd)


def etl(config: Config):
    try:
        logger.info("Loaded configuration:\n%s", config)

        if config.operation in ["injest", "full"]:
            logger.info("Starting ticker daily ingestion...")
            ingest_ticker_daily(
                base_dir=config.ingestion_dir, incremental=config.incremental
            )
            logger.info("Starting ticker hourly ingestion...")
            ingest_ticker_hourly(
                base_dir=config.ingestion_dir, incremental=config.incremental
            )
            logger.info("Ingestion completed successfully")

        if config.operation in ["transform", "full"]:
            logger.info("Starting model build pipeline...")
            build_everything(config)
            logger.info("All models built correctly!")

        if config.operation in ["full"]:
            # send this message only on daily full runs
            send_message_to_group("Another day another dolla")
    except Exception as e:
        logger.error("Job failed with error: %s", e)
        send_message_to_group(f"Errore nell'ELT giornaliero: {e}")
        raise e
