
from framework.utils.json_loader import JsonLoader
from configs.global_config  import json_path

def get_config():
    config_path = json_path
    loader = JsonLoader(config_path)
    print(loader.source_path)
    return loader

def read_xml(spark, path: str = None):
    """
    Reads an XML file and returns a DataFrame.
    """
    try:
        config = get_config()
        df = spark.read\
            .format("com.databricks.spark.xml")\
            .option("rowTag", config.input_rowTag)\
            .option("mode", config.input_mode)\
            .load(config.source_path)
    except Exception as e:
        print("Error during XML read:", e)
        raise e
    return df