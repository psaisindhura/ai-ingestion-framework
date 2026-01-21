def read_iceburg(spark, config):
    """
    Reads a Delta Lake table and returns a DataFrame.
    """
    try:     
        df = spark.read\
            .format("iceberg")\
            .option("snapshotId", config.iceburg_snapshot_id)\
            .option("as-Of-Timestamp", config.iceburg_asOfTimestamp)\
            .load(config.source_path)
    except Exception as e:
        print("Error during Iceburg read:", e)
        raise e
    return df