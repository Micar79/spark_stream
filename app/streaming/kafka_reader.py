from pyspark.sql.functions import col, from_json
from app.schemas.transaction_schema import transaction_schema

def read_kafka_stream(spark, config):

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka"]["bootstrap_servers"])
        .option("subscribe", config["kafka"]["input_topic"])
        .option("startingOffsets", "latest")
        .load()
    )

    return (
        raw_stream
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), transaction_schema).alias("data"))
        .select("data.*")
    )
