"""Silver : parsing, validation, deduplication, marquage des anomalies selon
le contrat INFRA.md §6 : plage nominale du capteur (capteurs.csv) surchargee
par le seuil machine (seuils_machine.csv), precedence par COALESCE.
On MARQUE les anomalies, on ne supprime JAMAIS. Nommage : francais sans accents."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

from common import (
    BRONZE_PATH, CAPTEURS_CSV, CHECKPOINT_DIR, SEUILS_MACHINE_CSV,
    SILVER_PATH, build_spark,
)

# Schema du JSON, fixe par l'enonce (timestamp encore en String ici)
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("capteur_id", StringType()),
    StructField("machine_id", StringType()),
    StructField("site_id", StringType()),
    StructField("type_mesure", StringType()),
    StructField("valeur", DoubleType()),
    StructField("unite", StringType()),
    StructField("qualite_signal", DoubleType()),
    StructField("batterie_pourcentage", IntegerType()),
    StructField("timestamp", StringType()),
])


def load_referentiels(spark):
    """Plages nominales par capteur + seuils par machine (INFRA.md §6)."""
    capteurs = (
        spark.read.option("header", "true").csv(CAPTEURS_CSV)
        .select(
            "capteur_id",
            F.col("plage_nominale_min").cast("double").alias("plage_min"),
            F.col("plage_nominale_max").cast("double").alias("plage_max"),
        )
    )
    seuils_machine = (
        spark.read.option("header", "true").csv(SEUILS_MACHINE_CSV)
        .select(
            "machine_id",
            "type_mesure",
            F.col("seuil_min").cast("double").alias("seuil_min_machine"),
            F.col("seuil_max").cast("double").alias("seuil_max_machine"),
        )
    )
    return capteurs, seuils_machine


def main():
    spark = build_spark("silver-clean")
    capteurs, seuils_machine = load_referentiels(spark)

    bronze = spark.readStream.format("delta").load(BRONZE_PATH)

    parsed = bronze.select(
        F.from_json(F.col("payload_json"), EVENT_SCHEMA).alias("evt")
    ).select("evt.*")

    typed = (
        parsed
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("date_event", F.to_date("timestamp"))
    )

    # Validation : on flague, on ne filtre pas
    flagged = typed.withColumn(
        "est_valide",
        F.col("event_id").isNotNull()
        & F.col("capteur_id").isNotNull()
        & F.col("valeur").isNotNull()
        & F.col("timestamp").isNotNull(),
    )

    # Dedup par event_id, etat borne par le watermark (Spark 3.5+)
    deduped = (
        flagged.withWatermark("timestamp", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )

    # Chaine de jointures du contrat :
    # flux -> capteurs (capteur_id) -> seuils_machine (machine_id, type_mesure)
    # puis precedence : le seuil machine SURCHARGE la plage nominale.
    with_seuils = (
        deduped
        .join(capteurs, ["capteur_id"], "left")
        .join(seuils_machine, ["machine_id", "type_mesure"], "left")
        .withColumn("seuil_min", F.coalesce("seuil_min_machine", "plage_min"))
        .withColumn("seuil_max", F.coalesce("seuil_max_machine", "plage_max"))
        .drop("plage_min", "plage_max", "seuil_min_machine", "seuil_max_machine")
    )

    silver = with_seuils.withColumn(
        "est_anomalie",
        F.col("est_valide")
        & F.col("seuil_max").isNotNull()
        & ((F.col("valeur") > F.col("seuil_max"))
           | (F.col("valeur") < F.col("seuil_min"))),
    ).withColumn(
        "type_anomalie",
        F.when(~F.col("est_valide"), F.lit("evenement_incomplet"))
        .when(F.col("valeur") > F.col("seuil_max"), F.lit("depassement_seuil_max"))
        .when(F.col("valeur") < F.col("seuil_min"), F.lit("sous_seuil_min"))
        .otherwise(F.lit(None).cast("string")),
    ).select(
        "event_id", "capteur_id", "machine_id", "site_id", "type_mesure",
        "valeur", "unite", "qualite_signal", "batterie_pourcentage",
        "timestamp", "date_event", "seuil_min", "seuil_max",
        "est_valide", "est_anomalie", "type_anomalie",
    )

    query = (
        silver.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/silver")
        .trigger(processingTime="15 seconds")
        .partitionBy("date_event")
        .start(SILVER_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
