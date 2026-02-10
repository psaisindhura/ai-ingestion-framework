def read_csv(spark, input_cfg):
    """
    Read CSV file based on metadata input configuration.
    
    :param spark: SparkSession
    :param input_cfg: dictionary from JSON `inputs[]`
    :return: Spark DataFrame
    """
    try:
        cfg = input_cfg.get("source_config", {})

        path = cfg.get("source_path")
        if path is None:
            raise ValueError(f"source_path not defined for input: {input_cfg.get('input_name')}")

        # Read options from config
        header = cfg.get("header", True)
        infer_schema = cfg.get("infer_schema", True)
        delimiter = cfg.get("delimiter", ",")
        multiLine = cfg.get("multiLine", False)
        quote = cfg.get("quote", "\"")
        escape = cfg.get("escape", "\\")
        mode = cfg.get("mode", "PERMISSIVE")
        ignoreLeadingWhiteSpace = cfg.get("ignoreLeadingWhiteSpace", True)
        ignoreTrailingWhiteSpace = cfg.get("ignoreTrailingWhiteSpace", True)

        # Read CSV
        df = spark.read \
            .option("header", header) \
            .option("inferSchema", infer_schema) \
            .option("delimiter", delimiter) \
            .option("multiLine", multiLine) \
            .option("quote", quote) \
            .option("escape", escape) \
            .option("mode", mode) \
            .option("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace) \
            .option("ignoreTrailingWhiteSpace", ignoreTrailingWhiteSpace) \
            .csv(path)

    except Exception as e:
        print(f"Exception while reading CSV for input {input_cfg.get('input_name')}: {e}")
        raise e

    return df
