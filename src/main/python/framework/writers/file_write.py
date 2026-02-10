def write_file(df, output_cfg):
    """
    Writes a DataFrame to a specified file format based on metadata output config.

    :param df: Spark DataFrame to write
    :param output_cfg: dictionary from JSON `outputs[]`
    :return: same DataFrame after writing
    """
    try:
        dest_cfg = output_cfg.get("destination_config", {})

        path = dest_cfg.get("destination_path")
        if not path:
            raise ValueError(f"destination_path not defined for output: {output_cfg.get('output_name')}")

        file_format = output_cfg.get("destination_format", "parquet")  # default to parquet
        mode = dest_cfg.get("mode", "overwrite")
        partition_cols = dest_cfg.get("partition_columns", [])

        writer = df.write.format(file_format).mode(mode)
        if partition_cols:
            writer = writer.partitionBy(partition_cols)

        writer.save(path)

        print(f"Output written successfully: {output_cfg.get('output_name')} -> {path}")

    except Exception as e:
        print(f"Error during file writing for output {output_cfg.get('output_name')}: {e}")
        raise e

    return df
