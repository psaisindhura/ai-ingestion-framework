from framework.utils.json_loader import JsonLoader
from configs.global_config  import json_path

def get_config():
    config_path = json_path
    loader = JsonLoader(config_path)
    print(loader.source_path)
    return loader

def read_parquet(spark,path: str = None):
    try:
        config = get_config()
        df = spark.read.parquet(path)
    except Exception as e:
        print("Error reading Parquet file:", e)
        raise e
    return df