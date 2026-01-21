
def read_delta(spark, config):
    """
    Reads a Delta Lake table and returns a DataFrame.
    """
    try:
        df = spark.read\
            .format("delta")\
            .load(config.source_path)
    except Exception as e:
        print("Error during Delta read:", e)
        raise e
    return df
