def write_file(df, config):
    """
    Writes a DataFrame to a specified file format.
    """
    try:
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