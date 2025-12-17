from pyspark.sql.avro.functions import to_avro,from_avro
from pyspark.sql.functions import struct

class AvroHandler:

    def __init__(self,schema: str):
        self.schema = schema

    def serialize(self, df):
        """
        Serializes a DataFrame to Avro format.
        """
        try:
            return to_avro(struct(*df.columns), self.schema)
        except Exception as e:
            print("Error during Avro serialization:", e)
            raise e
    def deserialize(self, column_name: str):
        """
        Deserializes a DataFrame from Avro format.
        """
        try:
            return from_avro(column_name, self.schema)
        except Exception as e:
            print("Error during Avro deserialization:", e)
            raise e