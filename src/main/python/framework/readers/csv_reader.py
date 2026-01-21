from pyspark.sql.utils import AnalysisException

def read_csv(spark,config):
    try:        
        header = config.input_header
        infer_schema = config.input_infer_schema
        path = config.source_path

        df = spark.read\
            .option("header", header)\
            .option("inferSchema", infer_schema)\
            .option("delimiter", config.input_delimitter)\
            .option("multiLine", config.input_multiLine)\
            .option("quote", config.input_qoute)\
            .option("escape", config.input_escape)\
            .option("sep", config.input_delimitter)\
            .option("mode",config.input_mode)\
            .option("ignoreLeadingWhiteSpace", config.input_ignoreLeadingWhiteSpace)\
            .option("ignoreTrailingWhiteSpace", config.input_ignoreTrailingWhiteSpace)\
            .csv(path)
    except AnalysisException as e:
        print("Analysis Exception:", e)
        raise e
        
    return df
