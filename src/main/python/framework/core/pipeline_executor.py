import sys
from pyspark.sql import SparkSession
from framework.utils.json_loader import JsonLoader
from framework.readers.csv_reader import read_csv
from framework.readers.json_reader import read_json_and_flatten
from framework.readers.xml_reader import read_xml
from framework.readers.parquet_read import read_parquet
from framework.writers.file_write import write_file
from framework.scd.scd2_processor import apply_scd2
from framework.utils.hash_utils import generate_hash_column

# ------------------ Spark session ------------------
spark = SparkSession.builder \
    .appName("MetadataDrivenETL") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ------------------ Helpers ------------------
def read_input(input_cfg):
    """Reads an input based on its type and config."""
    source_type = input_cfg.get("source_type")
    source_format = input_cfg.get("source_format", "").lower()

    if source_type == "file":
        if source_format == "csv":
            return read_csv(spark, input_cfg)
        elif source_format == "json":
            return read_json_and_flatten(spark, input_cfg)
        elif source_format == "parquet":
            return read_parquet(spark, input_cfg)
        elif source_format == "xml":
            return read_xml(spark, input_cfg)
        else:
            raise ValueError(f"Unsupported file format: {source_format}")
    else:
        # TODO: Add DB/Kafka readers
        raise ValueError(f"Unsupported source type: {source_type}")

def apply_transformations(df_registry, transformations):
    """Apply transformations sequentially as defined in metadata."""
    for t in transformations:
        t_type = t.get("transformation_type")
        t_config = t.get("config", {})
        input_id = t_config.get("input_name")  # name/id of input DataFrame
        output_alias = t_config.get("output_alias")  # alias after transformation

        if input_id not in df_registry:
            raise ValueError(f"Input DataFrame not found for transformation: {input_id}")

        df = df_registry[input_id]

        if t_type == "filter":
            condition = t_config.get("filter_condition")
            df_transformed = df.filter(condition)
        elif t_type == "flatten_json":
            # Assuming read_json_and_flatten already flattens
            df_transformed = df
        elif t_type == "add_date_partition":
            import pyspark.sql.functions as F
            df_transformed = df.withColumn("year", F.year(F.current_date())) \
                               .withColumn("month", F.month(F.current_date())) \
                               .withColumn("day", F.dayofmonth(F.current_date()))
        elif t_type == "scd2":
            source_hashed = generate_hash_column(df, t_config.get("tracked_columns", []))
            target_df = read_parquet(spark, t_config.get("scd2_target_path"))
            target_hashed = generate_hash_column(target_df, t_config.get("tracked_columns", []))
            df_transformed = apply_scd2(source_hashed, target_hashed,
                                        business_key=t_config.get("business_key", []),
                                        tracked_columns=t_config.get("tracked_columns", []))
            df_transformed.write.mode("overwrite").parquet(t_config.get("scd2_target_path"))
        else:
            raise ValueError(f"Unsupported transformation type: {t_type}")

        # Register transformed DF in registry
        df_registry[output_alias] = df_transformed
        df_transformed.createOrReplaceTempView(output_alias)

    return df_registry

def write_outputs(df_registry, outputs):
    """Write DataFrames to destination as per metadata."""
    for out in outputs:
        input_source = out.get("input_source")
        if input_source not in df_registry:
            raise ValueError(f"Output input_source not found in registry: {input_source}")
        df = df_registry[input_source]
        write_file(df, out)

# ------------------ Main pipeline ------------------
if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            raise ValueError("Config path argument is required.")
        config_path = sys.argv[1]

        config = JsonLoader(config_path)

        # ------------------ Step 1: Read all inputs ------------------
        df_registry = {}
        for inp in config.get_inputs():
            input_name = inp.get("input_name")
            df = read_input(inp)
            df_registry[input_name] = df
            df.createOrReplaceTempView(input_name)  # register as temp view

        # ------------------ Step 2: Apply transformations ------------------
        df_registry = apply_transformations(df_registry, config.get_transformations())

        # ------------------ Step 3: Write outputs ------------------
        write_outputs(df_registry, config.get_outputs())

        print("Pipeline execution completed successfully!")

    except Exception as e:
        print("Error during pipeline execution:", e)
        raise e
