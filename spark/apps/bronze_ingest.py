### ici la target est de capturer tt le flux brute kafka avec 0 transformation afin de pouvoir rejouer le Silver depuis le bronze sans recommencer kafka

from pyspark.sql import functions as F

from common import (
    BRONZE_PATH,
    CHECKPOINT_DIR,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    build_spark,
)

def main():
    spark = build_spark("bronze-ingest")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    bronze = kafka_stream.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload_json"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ts_ingestion"),
        F.to_date(F.current_timestamp()).alias("date_ingestion"),
    )

    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/bronze")
        # micro-batch 10 s : compromis latence / petits fichiers
        .trigger(processingTime="10 seconds")
        .partitionBy("date_ingestion")
        .start(BRONZE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()