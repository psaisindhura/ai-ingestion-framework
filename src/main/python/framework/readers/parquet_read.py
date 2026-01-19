from framework.utils.json_loader import JsonLoader

def get_config():
    config_path = "/opt/ai-ingestion-framework/ai-ingestion-framework/configs/job_config.json"
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