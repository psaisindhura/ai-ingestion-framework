from framework.readers.csv_reader import read_csv
from framework.utils.json_loader import JsonLoader
from pyspark.sql import SparkSession
from framework.readers.json_reader import read_json_and_flatten

spark = SparkSession.builder \
    .appName("CSVReader") \
    .getOrCreate()

def get_config():
    config_path = "/opt/ai-ingestion-framework/ai-ingestion-framework/configs/job_config.json"
    loader = JsonLoader(config_path)
    print(loader.source_path)
    return loader

if __name__ == "__main__":
    config = get_config()

    # Debug print (helps you see what file type is coming)
    print("Loaded config:", config)
    print("File type:", config.input_file_type)

    # Normalize input file type
    file_type = str(config.input_file_type).lower()
    path = config.source_path
    df = None
    # Read CSV
    if file_type == "csv":
        df = read_csv(spark, path)

    # Read JSON
    elif file_type == "json":
        df = read_json_and_flatten(spark, path)        
    
    # Invalid type
    else:
        raise ValueError(f"Unsupported file type: {config.input_file_type}")
    
    if df is None:
        raise RuntimeError("df was not created. Check config and reader functions.")

    df.show(truncate=False)
    