
def read_xml(spark, config):
    """
    Reads an XML file and returns a DataFrame.
    """
    try:      
        df = spark.read\
            .format("com.databricks.spark.xml")\
            .option("rowTag", config.input_rowTag)\
            .option("mode", config.input_mode)\
            .load(config.source_path)
    except Exception as e:
        print("Error during XML read:", e)
        raise e
    return df