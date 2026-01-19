from pyspark.sql.functions import col,length
from pyspark.sql import DataFrame

def not_null(df:DataFrame, column:str):
    return df.filter(col(column).isnull())

def is_null(df:DataFrame, column:str):
    return df.filter(col(column).isNotNull())

def rang_check(df, column, min_val= None, max_val = None):
    cond =None
    if min_val is not None:
        cond = col(column) >= min_val
    if max_val is not None:
        cond = cond & col(column) <= max_val
    return df.filter(cond)

def length_check(df,column, size):
    return df.filter(length(col(column)) == size)

def allowed_values(df, column, values):
    return df.filter(col(column).isin(values))
    
