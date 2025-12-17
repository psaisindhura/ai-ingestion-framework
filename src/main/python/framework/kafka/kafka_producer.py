from framework.utils.json_loader import JsonLoader
from framework.kafka.kafka_writer import kafka_writer
from framework.kafka.avro_serializer import AvroHandler
from pyspark.sql.functions import col
from configs.global_config  import json_path

config_json_path = json_path
config = JsonLoader(config_json_path)

def write_to_kafka(spark,df, topic: str = None):
    """
    write to kafka topic
    """
    try:
        with open(config.avro_schema_path) as schema_file:
            avro_schema = schema_file.read()

        avro_handler = AvroHandler(avro_schema)
        kafka_df = df.select(
            col(config.business_key[0]).alias("key"),
            avro_handler.serialize(df).alias("value")
        )
        kafka_writer(
            config.kafka_bootstrap_servers,
            config.kafka_topic_name,
        ).write(kafka_df)
        spark.stop()
    except Exception as e:
        print("Error loading Avro schema:", e)
        raise e
        