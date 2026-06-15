from contextlib import contextmanager
from dbt.cli.main import dbtRunner, dbtRunnerResult
import os
from etl.logger import get_logger
from etl.sources import ingest_ticker_daily, ingest_ticker_hourly
from etl.telegrambot import send_message_to_group

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


def run_dbt_build(incremental: bool):
    """Execute 'dbt build' command."""
    with temporary_working_directory("./etl/dbt"):
        dbt = dbtRunner()
        cli_args = ["build"] if incremental else ["build", "--full-refresh"]
        res: dbtRunnerResult = dbt.invoke(cli_args)
        if not res.success:
            raise Exception(f"Error during DBT build: {res.exception}")


def etl(config: dict):
    try:
        logger.info("Loaded configuration:\n%s", config)

        logger.info("Starting ticker daily ingestion...")
        ingest_ticker_daily(
            base_dir=config["ingestion_dir"], incremental=config["incremental"]
        )
        logger.info("Starting ticker hourly ingestion...")
        ingest_ticker_hourly(
            base_dir=config["ingestion_dir"], incremental=config["incremental"]
        )
        logger.info("Ingestion completed successfully")

        logger.info("Triggering dbt build...")
        run_dbt_build(config["incremental"])
        logger.info("Job completed successfully!")

        send_message_to_group("Another day another dolla")
    except Exception as e:
        logger.error("Job failed with error: %s", e)
        send_message_to_group(f"Errore nell'ELT giornaliero: {e}")
        raise e
