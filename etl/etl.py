from contextlib import contextmanager
from dbt.cli.main import dbtRunner, dbtRunnerResult
import os
import sys
from etl.sources import ingest_ticker_daily, ingest_ticker_hourly
from etl.telegrambot import send_message_to_group


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
        print(f"Loaded configuration:\n{config}")

        print("Starting ticker daily ingestion...")
        ingest_ticker_daily(
            base_dir=config["ingestion_dir"], incremental=config["incremental"]
        )
        print("Starting ticker hourly ingestion...")
        ingest_ticker_hourly(
            base_dir=config["ingestion_dir"], incremental=config["incremental"]
        )
        print("Ingestion completed successfully")

        print("Triggering dbt build...")
        run_dbt_build(config["incremental"])
        print("Job completed successfully!")

        print("Another day another dolla")
    except Exception as e:
        print(f"Job failed with error: {e}", file=sys.stderr)
        send_message_to_group(f"Errore nell'ELT giornaliero: {e}")
        raise e
