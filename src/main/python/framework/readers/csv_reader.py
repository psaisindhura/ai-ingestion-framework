def read_csv(spark,config):
    try:        
        header = config.input_header
        infer_schema = config.input_infer_schema
        dataset_name = config.dataset_name
        
        path = config.source_path.format(dataset_name=dataset_name)

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
    except Exception as e:
        print("Exception while reading CSV:", e)
        raise e
        
    return df
