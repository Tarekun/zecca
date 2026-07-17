#!/bin/bash

cd /home/danie/codice/zecca || exit
source .venv/bin/activate
python main.py full --config configs/prod.yml
