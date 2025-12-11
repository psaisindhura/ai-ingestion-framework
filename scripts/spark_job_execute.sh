#!/bin/bash

echo "Executing CSV ingestion"

# Read arguments
csv_name="$1"
csv_path="$2"

echo "Arguments passed: $csv_name $csv_path"

# Check if both arguments are provided
if [[ -n "$csv_name" && -n "$csv_path" ]]; then
    echo "Executing CSV ingestion job..."

    spark-submit \
    --py-files /opt/ai-ingestion-framework/ai-ingestion-framework/dist/ai_ingestion_framework-1.0.0.zip \
     /opt/ai-ingestion-framework/ai-ingestion-framework/src/main/python/framework/core/pipeline_executor.py
    "$csv_path"

else
    echo "Error: Two arguments required."
    echo "Usage: ./script.sh <csv_name> <csv_path>"
    echo "Example: ./script.sh csv /path/to/myfile.csv"
fi
