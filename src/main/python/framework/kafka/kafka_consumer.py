from configs.global_config  import json_path
from framework.utils.json_loader import JsonLoader
from framework.kafka.kafka_reader import kafka_reader
from framework.kafka.avro_serializer import AvroHandler
from pyspark.sql.functions import col

def read_from_kafka(spark):
    """
    read from kafka topic
    """
    try:
        config_json_path = json_path
        config = JsonLoader(config_json_path)

        kafka_df = kafka_reader(
            config.kafka_bootstrap_servers,
            config.kafka_topic_name,
            config.kafka_starting_offsets,
            config.kafka_consumer_group_id
        ).read_batch(spark)

        with open(config.avro_schema_path) as schema_file:
            avro_schema = schema_file.read()

        avro_handler = AvroHandler(avro_schema)
        df = kafka_reader(
            config.kafka_bootstrap_servers,
            config.kafka_topic_name,
            config.kafka_starting_offsets,
            config.kafka_consumer_group_id
        ).read_batch(spark).select(
            avro_handler.deserialize(col("value")).alias("data")
        ).select("data.*")
        
        spark.stop()

        return df
    except Exception as e:
        print("Error loading Avro schema:", e)
        raise e
