
def read_parquet(spark,config):
    try:
     df = spark.read.parquet(config.source_path)
    except Exception as e:
        print("Error reading Parquet file:", e)
        raise e
    return df