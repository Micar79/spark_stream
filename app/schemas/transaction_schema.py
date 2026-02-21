"""Transaction schema definition for Kafka messages."""
from pyspark.sql.types import *

# Define the schema for incoming transaction messages from Kafka
transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("country", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])
