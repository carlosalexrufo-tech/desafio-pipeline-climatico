#!/usr/bin/env bash
set -e

echo "Iniciando execução do Monitoramento Socioeconômico e Geoclimático"
python -m pip install -r requirements.txt
python main.py
echo "Execução sucesso!"