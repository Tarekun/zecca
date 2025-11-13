#!/bin/bash

cd /home/danie/codice/zecca || exit
git pull
source .venv/bin/activate
pip install -r requirements.txt
python main.py
