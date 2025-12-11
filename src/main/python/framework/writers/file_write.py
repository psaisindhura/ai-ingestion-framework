
def write_file(df,path:str,format:str="parquet",mode:str="overwrite"):
    """
    Writes a DataFrame to a specified file format.
    """
    df.write.format(format).mode(mode).save(path)
    return df