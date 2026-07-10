"""Bronze Layer: Ingestion brute depuis Kafka.

OBJECTIF:
Capturer tout le flux Kafka brut avec 0 transformation afin de pouvoir
rejouer le Silver depuis le Bronze sans recommencer Kafka.

ADAPTATION DATABRICKS:
- Écrit dans une table Unity Catalog (bronze_sensor_data) au lieu d'un path
- Les checkpoints sont sur DBFS
- Nécessite une configuration Kafka valide pour fonctionner
- Utilise trigger(availableNow=True) au lieu de processingTime (serverless)

NOTE: Ce job nécessite que Kafka soit accessible depuis Databricks.
Pour Confluent Cloud: configurez KAFKA_BOOTSTRAP, KAFKA_API_KEY, KAFKA_API_SECRET
"""
from pyspark.sql import functions as F
import os

from common import (
    BRONZE_TABLE,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    CHECKPOINT_DIR,
    build_spark,
)

def main():
    spark = build_spark("bronze-ingest")
    
    # Utilise les checkpoints définis dans common.py pour la cohérence
    checkpoint_path = f"{CHECKPOINT_DIR}/bronze_v2"
    
    print(f">>> Starting Bronze Layer ingestion...")
    print(f">>> Kafka Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f">>> Kafka Topic: {KAFKA_TOPIC}")
    print(f">>> Target Table: {BRONZE_TABLE}")
    print(f">>> Checkpoint: {checkpoint_path}")
    
    # Configuration Kafka avec authentication (Confluent Cloud ou autres)
    kafka_options = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
        "subscribe": KAFKA_TOPIC,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
    }
    
    # Ajouter l'authentification SASL/PLAIN si configurée (Confluent Cloud, Event Hubs, etc.)
    kafka_api_key = os.environ.get("KAFKA_API_KEY")
    kafka_api_secret = os.environ.get("KAFKA_API_SECRET")
    
    if kafka_api_key and kafka_api_secret:
        print(f">>> Using SASL/PLAIN authentication")
        kafka_options.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_api_key}" password="{kafka_api_secret}";',
        })
    else:
        print(f">>> No authentication configured (using plaintext)")
    
    # Lecture du flux Kafka
    kafka_stream_builder = spark.readStream.format("kafka")
    for key, value in kafka_options.items():
        kafka_stream_builder = kafka_stream_builder.option(key, value)
    
    kafka_stream = kafka_stream_builder.load()
    
    # Extraction des colonnes Kafka + métadonnées d'ingestion
    bronze = kafka_stream.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload_json"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ts_ingestion"),
        F.to_date(F.current_timestamp()).alias("date_ingestion"),
    )
    
    # Écriture streaming vers Unity Catalog
    print(f">>> Starting streaming query with availableNow=True...")
    
    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)  # Traite toutes les données disponibles puis s'arrête
        .partitionBy("date_ingestion")
        .toTable(BRONZE_TABLE)
    )
    
    print(f">>> Query started. Processing available data (max 60 seconds)...")
    
    # Timeout rapide: 60 secondes
    query.awaitTermination(timeout=60)
    
    # Vérifier si la requête est toujours active
    if query.isActive:
        print(">>> WARNING: Query still active after 60s, stopping...")
        query.stop()
        print(">>> Query stopped. Check if Kafka is reachable or if there's too much data.")
    else:
        print(">>> Bronze ingestion completed successfully!")
    
    # Afficher les statistiques
    try:
        row_count = spark.table(BRONZE_TABLE).count()
        print(f">>> Total rows in {BRONZE_TABLE}: {row_count}")
    except Exception as e:
        print(f">>> Could not count rows: {e}")


if __name__ == "__main__":
    main()
