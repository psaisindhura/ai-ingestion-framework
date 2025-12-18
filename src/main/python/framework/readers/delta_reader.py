
from framework.utils.json_loader import JsonLoader
from configs.global_config  import json_path

def get_config():
    config_path = json_path
    loader = JsonLoader(config_path)
    print(loader.source_path)
    return loader

def read_delta(spark, path: str = None):
    """
    Reads a Delta Lake table and returns a DataFrame.
    """
    try:
        config = get_config()
        df = spark.read\
            .format("delta")\
            .load(config.source_path)
    except Exception as e:
        print("Error during Delta read:", e)
        raise e
    return df
