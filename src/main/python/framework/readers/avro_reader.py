def read_avro(spark, config):
    """
    Reads an avro file and returns a DataFrame.
    """
    try:
    
        df = spark.read\
            .format("avro")\
            .option("avroSchema", open("employee.avsc").read()) \
            .load(config.source_path)
    except Exception as e:
        print("Error during avro read:", e)
        raise e
    return df