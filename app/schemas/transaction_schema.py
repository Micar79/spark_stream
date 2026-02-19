from pyspark.sql.types import *

transaction_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("country", StringType(), False),
    StructField("timestamp", TimestampType(), False)
])
