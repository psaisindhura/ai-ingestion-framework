from pyspark.sql import DataFrame
from pyspark.sql.functions import col,explode_outer
from pyspark.sql.types import StructType,ArrayType

def flatten_json(df: DataFrame,prefix:str = "") -> DataFrame:
    """
    Flattens a nested JSON DataFrame.
    """
    flat_cols =[]
    explode_cols  =[]

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
    
    return df

def read_json_and_flatten(spark, path: str) -> DataFrame:
    """
    Reads a JSON file and flattens it.
    """
    df = spark.read.option("multiLine", True).json(path)
    flat_df = flatten_json(df)
    return flat_df

