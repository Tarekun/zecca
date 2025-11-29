# Zecca

This project contains code to download, process, and analyze financial data of (hopefully in the future) various assets, starting from stock prices. It is a monorepo project containing different subprojects all together

## Get started to develop

```bash
# creates an isolated python installation in the .venv directory
python -m venv .venv
# linux/mac
source .venv/bin/activate
# windows
.venv/Scripts/activate
# install project dependencies
pip install -r requirements.txt
```

## Run any python script

```bash
python ./path/to/file.py
```

## Project overview

Currently the project contains 2 main modules

- etl: contains the data project including a custom python script to ingest stock prices from Yahoo finance and store it as parquet files and a dbt project that processes the ingested data and computes derivate indicators from the info we have
- analysis: contains the data science project that processes the data produced by the etl project doing statistical analyses over it and testing the different trading/investment strategies

To know more about these subprojects read the README in their respective directories or the proper documentation (WIP). Other than the subprojects the root directory will also contain the following files and directories

- dataplatform: this is where all the data will be stored by the etl process. File formats used are parquet and internal duckdb format
- configs: this directory contains .yml configurations used by all of our python code
- launch.sh: a shell script file that is used as the entrypoint called by our server to start the etl process daily
- requirements.txt: project dependencies with proper versioning
- .vscode: project settings for the vscode editor such as autoformatting or python type checking
