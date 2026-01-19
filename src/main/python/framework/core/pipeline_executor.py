from framework.readers.csv_reader import read_csv
from framework.utils.json_loader import JsonLoader
from pyspark.sql import SparkSession
from framework.readers.json_reader import read_json_and_flatten
from framework.readers.xml_reader import read_xml
from framework.readers.delta_reader import read_delta
from framework.readers.iceburg_reader import read_iceburg
from framework.readers.avro_reader import read_avro
from framework.readers.parquet_read import read_parquet
from framework.writers.file_write import write_file
from framework.scd.scd2_processor import apply_scd2
from framework.utils.hash_utils import generate_hash_column
from framework.readers.parquet_read import read_parquet

spark = SparkSession.builder \
    .appName("CSVReader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog")\
    .config("spark.sql.catalog.hadoop.type", "hadoop")\
    .config("spark.sql.catalog.hadoop.warehouse", "s3://my-bucket/warehouse")\
    .getOrCreate()

def get_config():
    try:
        config_path = "/opt/ai-ingestion-framework/ai-ingestion-framework/configs/job_config.json"
        loader = JsonLoader(config_path)
        print(loader.source_path)
    except FileNotFoundError as e:
        print("Configuration file not found:", e)
        raise e
    return loader
    
def process_scd2(spark):
     try:
        config = get_config()   
        business_key = config.business_key
        source_df = read_parquet(spark, config.scd2_source_path)
        target_df = read_parquet(spark, config.scd2_target_path)
        business_key = config.business_key
        tracked_columns = config.scd2_tracked_columns

        source_hashed = generate_hash_column(source_df, tracked_columns)
        target_hashed = generate_hash_column(target_df, tracked_columns)

        final_df = apply_scd2(source_hashed, target_hashed, business_key = business_key, tracked_columns = tracked_columns)

        final_df.write.mode("overwrite").parquet(config.scd2_target_path)
        
     except Exception as e:
        print("Error during SCD Type 2 processing:", e)
        raise e

if __name__ == "__main__":
    
    try:

        config = get_config()

        # Debug print (helps you see what file type is coming)
        print("Loaded config:", config)
        print("File type:", config.input_file_type)

        # Normalize input file type
        file_type = str(config.input_file_type).lower()
        #path = config.source_path
        df = None
        # Read CSV
        if file_type == "csv":
            df = read_csv(spark)

        # Read JSON
        elif file_type == "json":
            df = read_json_and_flatten(spark)    
        elif file_type == "parquet":
            df = read_parquet(spark, config.source_path)
        elif file_type == "scd2":
            process_scd2(spark)
            df = None
        elif file_type == "xml":            
            df = read_xml(spark)
        elif file_type == "delta":
            df = read_delta(spark, config.source_path)
        elif file_type == "iceburg":
            df = read_iceburg(spark, config.source_path)
        elif file_type == "avro":
            df = read_avro(spark, config.source_path)

           
    
        # Invalid type
        else:
            raise ValueError(f"Unsupported file type: {config.input_file_type}")
    
        if df is None:
            raise RuntimeError("df was not created. Check config and reader functions.")

        df.show(5)

        # Write output
        df_write = write_file(df)
        df_write.show(5)
    except Exception as e:
        print("Error during pipeline execution:", e)
        raise e
    