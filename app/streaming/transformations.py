from pyspark.sql.functions import *

def clean_transactions(df):

    return (
        df
        .withWatermark("timestamp", "10 minutes")
        .dropDuplicates(["transaction_id"])
        .filter(col("amount") > 0)
    )

def user_aggregation(df):

    return (
        df.groupBy(
            col("user_id"),
            window(col("timestamp"), "10 minutes", "5 minutes")
        )
        .agg(
            sum("amount").alias("total_amount"),
            count("*").alias("txn_count")
        )
    )
