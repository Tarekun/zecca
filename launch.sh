#!/bin/bash

cd /home/danie/codice/zecca || exit
git pull
source .venv/bin/activate
uv sync
python main.py full
