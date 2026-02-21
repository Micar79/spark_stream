"""ETL module: placeholder Spark batch job using PySpark."""
import os
from pyspark.sql import SparkSession


def run_spark_etl():
    """Run a simple Spark job that reads CSVs from /data/raw and writes Parquet to /data/processed.
    This is a lightweight example — expand with real transformations.
    """
    spark = (
        SparkSession.builder
        .appName("de_etl_job")
        .master("local[*]")
        .getOrCreate()
    )

    raw_dir = "/data/raw"
    out_dir = "/data/processed"
    os.makedirs(out_dir, exist_ok=True)

    if not os.path.exists(raw_dir):
        spark.stop()
        return

    df = spark.read.option("header", True).csv(raw_dir)
    # Example transform: cast amount if present
    if "amount" in df.columns:
        from pyspark.sql.functions import col
        df = df.withColumn("amount", col("amount").cast("double"))

    df.coalesce(1).write.mode("append").parquet(out_dir)
    spark.stop()
