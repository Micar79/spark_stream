"""Data transformation module for transaction processing."""
from pyspark.sql.functions import *

def clean_transactions(df):
    """Clean and deduplicate transaction data.
    
    Args:
        df: Input DataFrame with transaction data
        
    Returns:
        Cleaned DataFrame with watermark, deduplication, and filters applied
    """
    return (
        df
        .withWatermark("timestamp", "10 minutes")
        .dropDuplicates(["transaction_id"])
        .filter(col("amount") > 0)
    )

def user_aggregation(df):
    """Aggregate transactions by user in 10-minute windows.
    
    Args:
        df: Input DataFrame with transaction data
        
    Returns:
        Aggregated DataFrame with user totals and transaction counts per window
    """
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
