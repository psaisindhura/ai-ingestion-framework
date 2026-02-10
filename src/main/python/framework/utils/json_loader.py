import os
import json

class JsonLoader:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.config_data = self._load_json()

    def _load_json(self):
        """Load JSON file and return dictionary"""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"JSON file not found: {self.file_path}")

        try:
            with open(self.file_path, "r") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {e}")

    
    @property
    def metadata(self):
        return self.config_data.get("metadata", {})

    @property
    def job_name(self):
        return self.metadata.get("job_name")

    # ---------------- INPUTS ----------------
    def get_inputs(self):
        return self.config_data.get("inputs", [])

    def get_input_by_name(self, input_name: str):
        for inp in self.get_inputs():
            if inp.get("input_name") == input_name:
                return inp
        raise ValueError(f"Input not found: {input_name}")

    def get_file_input_config(self, input_name: str):
        input_cfg = self.get_input_by_name(input_name)
        return input_cfg.get("source_config", {})

    # Common input helpers
    def get_source_path(self, input_name):
        return self.get_file_input_config(input_name).get("source_path")

    def get_input_format(self, input_name):
        return self.get_input_by_name(input_name).get("source_format")

    def get_input_mode(self, input_name):
        return self.get_file_input_config(input_name).get("mode", "PERMISSIVE")

    def get_input_multiline(self, input_name):
        return self.get_file_input_config(input_name).get("multiLine", False)

    # ---------------- TRANSFORMATIONS ----------------
    def get_transformations(self):
        return self.config_data.get("transformations", [])

    def get_transformation_by_name(self, transformation_name: str):
        for t in self.get_transformations():
            if t.get("transformation_name") == transformation_name:
                return t
        raise ValueError(f"Transformation not found: {transformation_name}")

    def get_filter_condition(self, transformation_name):
        transformation = self.get_transformation_by_name(transformation_name)
        return transformation.get("config", {}).get("filter_condition")

    def get_transformation_input(self, transformation_name):
        transformation = self.get_transformation_by_name(transformation_name)
        return transformation.get("config", {}).get("input_id")

    def get_transformation_output_alias(self, transformation_name):
        transformation = self.get_transformation_by_name(transformation_name)
        return transformation.get("config", {}).get("output_alias")

    # ---------------- OUTPUTS ----------------
    def get_outputs(self):
        return self.config_data.get("outputs", [])

    def get_output_by_name(self, output_name: str):
        for out in self.get_outputs():
            if out.get("output_name") == output_name:
                return out
        raise ValueError(f"Output not found: {output_name}")

    def get_output_path(self, output_name):
        output = self.get_output_by_name(output_name)
        return output.get("destination_config", {}).get("destination_path")

    def get_output_format(self, output_name):
        output = self.get_output_by_name(output_name)
        return output.get("destination_format")

    def get_output_mode(self, output_name):
        output = self.get_output_by_name(output_name)
        return output.get("destination_config", {}).get("mode", "overwrite")

    def get_output_partitions(self, output_name):
        output = self.get_output_by_name(output_name)
        return output.get("destination_config", {}).get("partition_columns", [])
