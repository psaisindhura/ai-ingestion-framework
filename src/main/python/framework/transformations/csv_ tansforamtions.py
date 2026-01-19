from pyspark.sql.window import Window
from pyspark.sql.functions import *



def rank_latest_records(customer_df):
    window_spec = window.partitionBy("business_key").orderBy(col("effective_date").desc())
    lastest_customer_df = (customer_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    return lastest_customer_df    
    

# running total of Customer spend
def running_total(customer_df):
    window_spec = (
        Window
        .partitionBy("business_key")
        .orderBy("effective_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)        
    )
    
    customer_df = customer_df.withColumn("running_total", sum("amount").over(window_spec))
    return customer_df

def cumulative_avg(customer_df):
    window_spec = (
        window
        .partitionBy("business_key")
        .orderBy("effective_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    customer_df = customer_df.withColumn("cumulative_avg", avg("amount").over(window_spec))
    return customer_df

def lag_lead_analysis(customer_df):
    window_spec = Window.partitionBy("business_key").orderBy("effective_date")
    
    customer_df = customer_df.withColumn("previous_amount", lag("amount").over(window_spec))
    customer_df = customer_df.withColumn("next_amount", lead("amount").over(window_spec))
    
    return customer_df

#rank customer by spending 
def rank_customers(customer_df):
    window_spec = Window.partitionBy("city").orderBy(desc("amount"))
    raanked_customer_df = customer_df.withColumn("rank", rank().over(window_spec)
    )

#   dense rank
    dense_ranked_customer_df = customer_df.withColumn("dense_rank", dense_rank().over(window_spec)
    )   
    return raanked_customer_df, dense_ranked_customer_df

#  FIRST and LAST transaction per customer
def first_last_transaction(customer_df):
    window_spec = window.partitionBy("customer_Id")

    df_first_last = customer_df.select(
        "customer_Id",
        first("amount").over(window_spec).alias("first_transaction"),
        last("amount").over(window_spec).alias("last_transaction")
    )
    return df_first_last