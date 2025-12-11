
def read_csv(spark, path):
    df = spark.read.csv(path, header=True, inferSchema=True)
    return df
