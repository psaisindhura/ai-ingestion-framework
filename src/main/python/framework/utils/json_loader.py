import os
import json

class JsonLoader:
    def __init__(self,file_path):
            self.file_path = file_path
            self.config_data = self._load_json()
    
    def _load_json(self):
          """load json file and return dictionary object"""
          try:

             if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"JSON file not found: {self.file_path}")
             with open(self.file_path, 'r') as json_file:
                data = json.load(json_file)
          
          except json.JSONDecodeError as e: 
                  print(f"Error decoding JSON file: {e}")
                  raise e
                  
          return data
    
    @property
    def source_path(self):
          return self.config_data.get("inputs", {}).get("source_path")
    @property
    def input_file_type(self):
          return self.config_data.get("inputs", {}).get("file_type")
    @property
    def input_delimitter(self):
          return self.config_data.get("inputs", {}).get("delimitter", ",")
    @property
    def input_header(self):
          return self.config_data.get("inputs", {}).get("header", True)
    @property
    def input_infer_schema(self):
          return self.config_data.get("inputs", {}).get("infer_schema", True)
    @property 
    def input_multiLine(self):
              return self.config_data.get("inputs", {}).get("multiLine", False)
    @property
    def input_mode(self):
            return self.config_data.get("inputs", {}).get("mode", "PERMISSIVE")
    @property
    def input_qoute(self):
            return self.config_data.get("inputs", {}).get("qoute", "\"")
    @property
    def input_escape(self):
            return self.config_data.get("inputs", {}).get("escape", "\\")
    @property
    def input_ignoreLeadingWhiteSpace(self):
            return self.config_data.get("inputs", {}).get("ignoreLeadingWhiteSpace", True)
    @property
    def input_ignoreTrailingWhiteSpace(self):
            return self.config_data.get("inputs", {}).get("ignoreTrailingWhiteSpace", True)
    @property
    def business_key(self):
            return self.config_data.get("inputs", {}).get("business_key", [])
    @property
    def transform(self):
            return self.config_data.get("transformations", {}).get("filter_column")
    @property
    def is_flatten_json(self):
            return self.config_data.get("transformations", {}).get("is_flatten_json", False)
    @property
    def add_date_partition(self):
            return self.config_data.get("transformations", {}).get("add_date_partition", False)
    @property
    def destination_path(self):
          return self.config_data.get("output", {}).get("destination_path")
    
    @property
    def output_file_type(self):
          return self.config_data.get("output", {}).get("file_type")
    @property
    def output_table_name(self):
          return self.config_data.get("output", {}).get("table_name")
    @property
    def output_partition_columns(self):
         return self.config_data.get("output", {}).get("partition_columns", [])
    @property
    def output_mode(self):
        return self.config_data.get("output", {}).get("mode", "overwrite")
    @property
    def scd2_source_path(self):
        return self.config_data.get("scd2_settings", {}).get("source_path")
    @property
    def scd2_target_path(self):
        return self.config_data.get("scd2_settings", {}).get("target_path")
    @property
    def scd2_tracked_columns(self):
        return self.config_data.get("scd2_settings", {}).get("tracked_columns", [])
    @property
    def kafka_bootstrap_servers(self):
        return self.config_data.get("kafka", {}).get("bootstrap_servers")
    @property
    def kafka_topic_name(self):
        return self.config_data.get("kafka", {}).get("topic_name")
    @property
    def kafka_checkpoint_location(self):
        return self.config_data.get("kafka", {}).get("checkpoint_location")
    @property
    def avro_schema_path(self):
        return self.config_data.get("avro", {}).get("schema_path")
    @property
    def kafka_starting_offsets(self):
        return self.config_data.get("kafka", {}).get("kafka_starting_offsets")
    @property
    def kafka_consumer_group_id(self):
        return self.config_data.get("kafka", {}).get("consumer_group_id")
    @property
    def iceburg_snapshot_id(self):
        return self.config_data.get("iceberg_settings", {}).get("snapshot_id")