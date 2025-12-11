from framework.readers.csv_reader import read_csv
from framework.utils.json_loader import JsonLoader
from pyspark.sql import SparkSession

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
    if config.input_file_type == "csv":
        path = config.source_path
        df = read_csv(spark, path)
        df.show(15)
    