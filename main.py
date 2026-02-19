from app.config import load_config
from app.spark_session import create_spark_session
from app.streaming.kafka_reader import read_kafka_stream
from app.streaming.transformations import clean_transactions, user_aggregation
from app.streaming.sinks import write_delta, write_kafka_alerts

def main():

    config = load_config("configs/dev.yaml")

    spark = create_spark_session(config["app_name"])

    raw_stream = read_kafka_stream(spark, config)

    cleaned = clean_transactions(raw_stream)

    aggregated = user_aggregation(cleaned)

    write_delta(cleaned, config)
    write_kafka_alerts(aggregated, config)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
