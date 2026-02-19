def write_delta(df, config):

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
