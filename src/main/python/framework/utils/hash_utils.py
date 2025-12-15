from pyspark.sql.functions import sha2, concat_ws, col

def generate_hash_column(df, columns, hash_column_name="hash_value"):
    """
    Generates a SHA-256 hash column based on the specified columns.

    :param df: Input DataFrame
    :param columns: List of column names to include in the hash
    :param hash_column_name: Name of the resulting hash column
    :return: DataFrame with an additional hash column
    """
    concatenated_cols = concat_ws("||", *[col(c).cast("string") for c in columns])
    return df.withColumn(hash_column_name, sha2(concatenated_cols, 256))