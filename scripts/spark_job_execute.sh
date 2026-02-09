#!/bin/bash

echo "Executing CSV ingestion"

# Read arguments
config_path="$1"
csv_path="$2"

echo "Arguments passed: $config_path $csv_path"

# Check if both arguments are provided
if [[ -n "$config_path" && -n "$csv_path" ]]; then
    echo "Executing CSV ingestion job..."

    spark-submit \
    --master local[*] \
    --py-files /opt/ai-ingestion-framework/ai_ingestion_framework.zip \
     /opt/ai-ingestion-framework/configs/pipeline_executor.py  "$config_path"

else
    echo "Error: Two arguments required."
    echo "Usage: ./script.sh <csv_name> <csv_path>"
    echo "Example: ./script.sh csv /path/to/myfile.csv"
fi
