#!/bin/bash

git pull
source .venv/bin/activate
pip install -r requirements.txt
python main.py
