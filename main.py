"""Main entry point for real-time fraud detection streaming application."""
from app.config import load_config
from app.spark_session import create_spark_session
from app.streaming.kafka_reader import read_kafka_stream
from app.streaming.transformations import clean_transactions, user_aggregation
from app.streaming.sinks import write_delta, write_kafka_alerts

def main():
    """Execute the fraud detection pipeline:
    
    1. Load configuration from YAML
    2. Create Spark session with streaming support
    3. Read transactions from Kafka
    4. Clean and deduplicate transaction data
    5. Aggregate by user in sliding windows
    6. Write cleaned data to Delta Lake
    7. Write fraud alerts to Kafka
    """
    # Load configuration
    config = load_config("configs/dev.yaml")

    # Initialize Spark session
    spark = create_spark_session(config["app_name"])

    # Read from Kafka input topic
    raw_stream = read_kafka_stream(spark, config)

    # Transform and clean data
    cleaned = clean_transactions(raw_stream)

    # Aggregate transactions by user
    aggregated = user_aggregation(cleaned)

    # Write to persistent storage
    write_delta(cleaned, config)
    
    # Write fraud alerts downstream
    write_kafka_alerts(aggregated, config)

    # Keep the application running
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
