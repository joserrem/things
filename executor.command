#!/bin/bash

# Move to the specific folder
cd Desktop/Query_Generator

# Pip installation
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3 get-pip.py

# Installing Azure Storage libraries
pip install azure-storage-blob

# Clear Terminal
clear

# Execute the script
python3 query_generator.py