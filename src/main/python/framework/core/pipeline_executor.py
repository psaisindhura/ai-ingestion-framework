from framework.readers.csv_reader import read_csv
from framework.utils.json_loader import JsonLoader
from pyspark.sql import SparkSession
from framework.readers.json_reader import read_json_and_flatten
from framework.writers.file_write import write_file
from configs.config_loader import load_config

def get_spark_config():
    config = load_config()
    spark_config =  config["spark"]
    return spark_config

spark = SparkSession.builder \
    .appName("CSVReader") \
    .getOrCreate()

def get_config():
    try:
        config_path = "/opt/ai-ingestion-framework/ai-ingestion-framework/configs/job_config.json"
        loader = JsonLoader(config_path)
        print(loader.source_path)
    except FileNotFoundError as e:
        print("Configuration file not found:", e)
        raise e
    return loader

if __name__ == "__main__":
    
    try:

        config = get_config()

        # Debug print (helps you see what file type is coming)
        print("Loaded config:", config)
        print("File type:", config.input_file_type)

        # Normalize input file type
        file_type = str(config.input_file_type).lower()
        #path = config.source_path
        df = None
        # Read CSV
        if file_type == "csv":
            df = read_csv(spark)

        # Read JSON
        elif file_type == "json":
            df = read_json_and_flatten(spark)        
    
        # Invalid type
        else:
            raise ValueError(f"Unsupported file type: {config.input_file_type}")
    
        if df is None:
            raise RuntimeError("df was not created. Check config and reader functions.")

        df.show(5)

        # Write output
        df_write = write_file(df)
        df_write.show(5)
    except Exception as e:
        print("Error during pipeline execution:", e)
        raise e
    