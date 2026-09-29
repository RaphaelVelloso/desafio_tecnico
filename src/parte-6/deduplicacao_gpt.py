from pyspark.sql import functions as F

schema_sensor = """
    sensor_id STRING,
    event_id STRING,
    event_timestamp TIMESTAMP,
    temperature DOUBLE,
    pressure DOUBLE
"""

df_sensor = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .schema(schema_sensor)
    .load("/mnt/raw/sensores_iot/")
)

df_dedup = (
    df_sensor

    # Metadados de ingestão
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )

    .withColumn(
        "source_file",
        F.input_file_name()
    )

    # Event time + controle do estado
    .withWatermark(
        "event_timestamp",
        "30 minutes"
    )

    # Deduplicação
    .dropDuplicates(
        ["sensor_id", "event_id"]
    )
)

query = (
    df_dedup
    .writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        "/mnt/checkpoints/sensores_iot"
    )
    .toTable(
        "niometal.silver.sensores_iot"
    )
)