from pyspark.sql import DataFrame
from pyspark.sql.functions import col,explode_outer
from pyspark.sql.types import StructType,ArrayType
from framework.utils.json_loader import JsonLoader
from pyspark.sql.utils import AnalysisException

def get_config():
    try:
        config_path = "/opt/ai-ingestion-framework/ai-ingestion-framework/configs/job_config.json"
        loader = JsonLoader(config_path)
        print(loader.source_path)
    except FileNotFoundError as e:
        print("Configuration file not found:", e)
        raise e
    return loader


def flatten_json(df: DataFrame,prefix:str = "") -> DataFrame:
    """
    Flattens a nested JSON DataFrame.
    """
    flat_cols =[]
    explode_cols  =[]
    try:
        for field in df.schema.fields:
            field_name = field.name
            new_name = f"{prefix}{field_name}"

            #struncture type
            if isinstance(field.dataType, StructType):
                for nested_field in field.dataType.fields:
                    flat_cols.append(col(f"{field_name}.{nested_field.name}").alias(f"{new_name}_{nested_field.name}"))
                
         #array type
            elif isinstance(field.dataType, ArrayType):
                if(isinstance(field.dataType.elementType, StructType)):
                    explode_cols.append(field_name)
                else:
                    flat_cols.append(col(field_name).alias(new_name))
            else:
                flat_cols.append(col(field_name).alias(new_name))

        #Apply  explode for struct type
        for c in explode_cols:
            df = df.withColumn(c,explode_outer(col(c)))

        df = df.select(flat_cols)

        #check again for nested json and resource
        complex_fields_exists = any(
            isinstance(f.dataType,StructType) or isinstance(f.dataType,ArrayType)
            and isinstance(f.dataType.elementType,StructType)
            for f in df.schema.fields)
    
        if complex_fields_exists:
            return flatten_json(df)
    except AnalysisException as e:
        print("Analysis Exception during flattening:", e)
        raise e
    return df

def read_json_and_flatten(spark, path: str = None) -> DataFrame:
    """
    Reads a JSON file and flattens it.
    """
    try:
        config = get_config()
        df = spark.read\
            .option("multiLine", config.input_multiLine)\
            .option("mode", config.input_mode)\
            .json(config.source_path)
        if get_config().is_flatten_json:
            flat_df = flatten_json(df)
    except AnalysisException as e:
        print("Analysis Exception during JSON read:", e)
        raise e
    return flat_df

