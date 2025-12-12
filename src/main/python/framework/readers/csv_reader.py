from framework.utils.json_loader import JsonLoader
from pyspark.sql.utils import AnalysisException

def get_config():
    config_path = "/opt/ai-ingestion-framework/ai-ingestion-framework/configs/job_config.json"
    loader = JsonLoader(config_path)
    print(loader.source_path)
    return loader

def read_csv(spark,pathh: str = None):

    try:
        config = get_config()
        header = config.input_header
        infer_schema = config.input_infer_schema
        path = config.source_path

        df = spark.read\
          .format(path)\
            .option("header", header)\
            .option("inferSchema", infer_schema)\
            .option("delimiter", config.input_delimitter)\
            .option("multiLine", config.input_multiLine)\
            .option("quote", config.input_qoute)\
            .option("escape", config.input_escape)\
            .option("sep", config.input_delimitter)\
            .oprtion("mode",config.input_mode)\
            .option("ignoreLeadingWhiteSpace", config.input_ignoreLeadingWhiteSpace)\
            .option("ignoreTrailingWhiteSpace", config.input_ignoreTrailingWhiteSpace)\
            .load("/path/file.csv")
    except AnalysisException as e:
        print("Analysis Exception:", e)
        raise e
        
    return df
