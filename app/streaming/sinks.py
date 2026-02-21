"""Sink module for writing streaming data to Delta and Kafka."""

def write_delta(df, config):
    """Write cleaned transactions to Delta Lake.
    
    Args:
        df: Input DataFrame with transaction data
        config: Configuration dictionary with paths
        
    Returns:
        StreamingQuery object
    """
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            f"{config['paths']['checkpoints']}/silver"
        )
        .start(config["paths"]["silver_delta"])
    )

def write_kafka_alerts(df, config):
    """Write fraud alerts to Kafka topic.
    
    Args:
        df: Input DataFrame with aggregated data (must contain risk_flag column)
        config: Configuration dictionary with Kafka and checkpoint paths
        
    Returns:
        StreamingQuery object
    """
    # Filter for high-risk transactions
    alerts = df.filter("risk_flag = true")

    return (
        alerts
        .selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka"]["bootstrap_servers"])
        .option("topic", config["kafka"]["alert_topic"])
        .option(
            "checkpointLocation",
            f"{config['paths']['checkpoints']}/alerts"
        )
        .start()
    )
