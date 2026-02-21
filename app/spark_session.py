"""Spark session initialization module."""
from pyspark.sql import SparkSession

def create_spark_session(app_name: str):
    """Create and configure a Spark session for streaming.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        Configured SparkSession instance
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
