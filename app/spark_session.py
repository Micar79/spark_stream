from pyspark.sql import SparkSession

def create_spark_session(app_name: str):

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
