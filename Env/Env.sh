#!/bin/bash

# Create Python virtual environment
python -m venv project_env

# Activate virtual environment
source project_env/Scripts/activate

# Write installed packages to requirements.txt
pip freeze > requirements.txt

# Install packages from requirements.txt
pip install -r requirements.txt

# Deactivate virtual environment
deactivate

# Delete virtual environment
# rmdir /s /q myfirstproject

