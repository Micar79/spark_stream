"""Kafka stream reader module."""
from pyspark.sql.functions import col, from_json
from app.schemas.transaction_schema import transaction_schema

def read_kafka_stream(spark, config):
    """Read streaming data from Kafka topic.
    
    Args:
        spark: SparkSession instance
        config: Configuration dictionary with Kafka settings
        
    Returns:
        DataFrame with parsed transaction data
    """
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka"]["bootstrap_servers"])
        .option("subscribe", config["kafka"]["input_topic"])
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON messages and flatten schema
    return (
        raw_stream
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), transaction_schema).alias("data"))
        .select("data.*")
    )
