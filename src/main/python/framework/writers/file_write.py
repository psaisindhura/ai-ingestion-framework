from framework.utils.json_loader import JsonLoader
from configs.global_config  import json_path 

def get_config():
    try:
        config_path = json_path
        loader = JsonLoader(config_path)
        print(loader.source_path)
    
    except FileNotFoundError as e:
        print("Configuration file not found:", e)
        raise e
    
    return loader

def write_file(df):
    """
    Writes a DataFrame to a specified file format.
    """
    try:
        config = get_config()   
        path = config.destination_path
        format = config.output_file_type
        mode = config.output_mode
        partiton_cols = config.output_partition_columns
        df.write\
            .format(format)\
            .mode(mode) \
            .save(path)
       # .partitionBy(partiton_cols)\ 
    except Exception as e:
        print("Error during file writing:", e)
        raise e
    
    return df