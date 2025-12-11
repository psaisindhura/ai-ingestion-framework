import os
import json

class JsonLoader:
    def __init__(self,file_path):
            self.file_path = file_path
            self.config_data = self._load_json()
    
    def _load_json(self):
          """load json file and return dictionary object"""
          if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"JSON file not found: {self.file_path}")
          with open(self.file_path, 'r') as json_file:
                data = json.load(json_file)
          return data
    
    @property
    def source_path(self):
          return self.config_data.get("inputs", {}).get("source_path")
    @property
    def input_file_type(self):
          return self.config_data.get("inputs", {}).get("file_type")
    @property
    def transform(self):
            return self.config_data.get("transformations", {}).get("filter_column")
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
