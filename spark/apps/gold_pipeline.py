"""Gold : star-schema + les 2 mecanismes Delta exiges par l'enonce.

Entree  : table Silver (Delta, streaming) — nettoyee, dedupliquee, anomalies flaguees.
Sorties : tables Delta sous /opt/lakehouse/gold/ + recopie Postgres (schema gold)
          lue par Metabase.

Modelisation en etoile :
  1. Dimensions (batch, OVERWRITE)          : dim_capteur, dim_machine, dim_site
     — construites depuis le referentiel CSV, quasi statiques.
  2. Table de faits (streaming, APPEND)     : fait_mesures — 1 ligne par mesure
     valide, cles etrangeres vers les dimensions.
  3. Agregation fenetree (streaming, APPEND): agg_machine_5min — moyenne/max
     glissants par machine (fenetre 5 min, pas 1 min) — justifie le streaming.
  4. Etat courant capteur (MERGE INTO)      : etat_courant_capteur — derniere
     valeur / dernier statut par capteur, mis a jour par MERGE a chaque
     micro-batch (PAS un simple append).

Les mecanismes APPEND (2, 3) et MERGE INTO (4) sont volontairement separes
pour rester visibles et justifiables dans le README.
"""

import os

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (
    CHECKPOINT_DIR, DATA_DIR, GOLD_PATH, SILVER_PATH, build_spark,
)

FAIT_MESURES_PATH = f"{GOLD_PATH}/fait_mesures"
AGG_MACHINE_PATH = f"{GOLD_PATH}/agg_machine_5min"
ETAT_COURANT_PATH = f"{GOLD_PATH}/etat_courant_capteur"

PG_URL = "jdbc:postgresql://postgres:5432/warehouse"
PG_PROPS = {
    "user": "warehouse",
    "password": "warehouse",
    "driver": "org.postgresql.Driver",
}


def write_postgres(df, table, mode, truncate=False):
    """Recopie vers le serving layer Postgres (schema gold) lu par Metabase."""
    (
        df.write.option("truncate", str(truncate).lower())
        .jdbc(PG_URL, f"gold.{table}", mode=mode, properties=PG_PROPS)
    )


# ---------------------------------------------------------------------------
# 1. DIMENSIONS — batch depuis le referentiel CSV (overwrite : quasi statique)
# ---------------------------------------------------------------------------
def build_dimensions(spark):
    def read_csv(name):
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{DATA_DIR}/{name}.csv")
        )

    dims = {
        "dim_capteur": read_csv("capteurs"),
        "dim_machine": read_csv("machines"),
        "dim_site": read_csv("sites"),
    }
    for table, df in dims.items():
        df.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}/{table}")
        write_postgres(df, table, mode="overwrite")
        print(f"Dimension {table} ecrite (Delta + Postgres) : {df.count()} lignes")


# ---------------------------------------------------------------------------
# 2. FAIT_MESURES — append streaming (mecanisme Delta n°1 : append)
# ---------------------------------------------------------------------------
def start_fait_mesures(silver):
    fait = silver.filter(F.col("est_valide")).select(
        "event_id", "capteur_id", "machine_id", "site_id", "type_mesure",
        "valeur", "unite", "qualite_signal", "batterie_pourcentage",
        "timestamp", "date_event", "est_anomalie", "type_anomalie",
    )

    def write_batch(batch_df, batch_id):
        # Delta d'abord (source de verite), Postgres ensuite (serving layer).
        batch_df.write.format("delta").mode("append") \
            .partitionBy("date_event").save(FAIT_MESURES_PATH)
        write_postgres(batch_df, "fait_mesures", mode="append")

    return (
        fait.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold_fait_mesures")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ---------------------------------------------------------------------------
# 3. AGG_MACHINE_5MIN — agregation glissante par machine (append streaming)
#    Fenetre 5 min / pas 1 min, watermark 2 min : une fenetre n'est emise
#    qu'une fois finalisee (mode append) => pas de doublons de fenetres.
# ---------------------------------------------------------------------------
def start_agg_machine(silver):
    agg = (
        silver.filter(F.col("est_valide"))
        .withWatermark("timestamp", "2 minutes")
        .groupBy(
            F.window("timestamp", "5 minutes", "1 minute"),
            "machine_id",
            "type_mesure",
        )
        .agg(
            F.avg("valeur").alias("valeur_moyenne"),
            F.max("valeur").alias("valeur_max"),
            F.count("event_id").alias("nb_mesures"),
            F.sum(F.col("est_anomalie").cast("int")).alias("nb_anomalies"),
        )
        .select(
            F.col("window.start").alias("fenetre_debut"),
            F.col("window.end").alias("fenetre_fin"),
            "machine_id", "type_mesure",
            F.round("valeur_moyenne", 2).alias("valeur_moyenne"),
            "valeur_max", "nb_mesures", "nb_anomalies",
        )
    )

    def write_batch(batch_df, batch_id):
        batch_df.write.format("delta").mode("append").save(AGG_MACHINE_PATH)
        write_postgres(batch_df, "agg_machine_5min", mode="append")

    return (
        agg.writeStream.foreachBatch(write_batch)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold_agg_machine_5min")
        .trigger(processingTime="1 minute")
        .start()
    )


# ---------------------------------------------------------------------------
# 4. ETAT_COURANT_CAPTEUR — MERGE INTO (mecanisme Delta n°2, exige tel quel :
#    "mise a jour par MERGE INTO a chaque nouvel evenement, PAS un append")
# ---------------------------------------------------------------------------
def start_etat_courant(spark, silver):
    def merge_batch(batch_df, batch_id):
        # a) Un MERGE refuse 2 lignes source pour la meme cle : on ne garde
        #    que la mesure la plus recente par capteur dans le micro-batch.
        w = Window.partitionBy("capteur_id").orderBy(F.col("timestamp").desc())
        derniers = (
            batch_df.filter(F.col("est_valide"))
            .withColumn("rang", F.row_number().over(w))
            .filter(F.col("rang") == 1)
            .select(
                "capteur_id", "machine_id", "site_id", "type_mesure",
                F.col("valeur").alias("derniere_valeur"),
                "unite",
                F.col("timestamp").alias("dernier_timestamp"),
                F.when(F.col("est_anomalie"), F.lit("anomalie"))
                 .otherwise(F.lit("ok")).alias("statut"),
                "type_anomalie", "seuil_min", "seuil_max",
                "qualite_signal", "batterie_pourcentage",
            )
        )
        if derniers.isEmpty():
            return

        # b) MERGE INTO en SQL : update si le capteur existe, insert sinon.
        #    SQL pur (jars Delta) : le module Python delta.tables n'est pas
        #    installe dans l'image — et la syntaxe MERGE INTO exigee par
        #    l'enonce reste visible telle quelle.
        if not os.path.exists(f"{ETAT_COURANT_PATH}/_delta_log"):
            derniers.write.format("delta").mode("overwrite").save(ETAT_COURANT_PATH)
        else:
            # La vue temporaire vit dans la session du micro-batch (isolee
            # par foreachBatch) : le MERGE doit etre soumis via cette meme
            # session, pas via la session principale.
            derniers.createOrReplaceTempView("maj_etat_capteur")
            # La condition sur dernier_timestamp protege des micro-batches
            # rejoues apres un redemarrage sur checkpoint (at-least-once).
            derniers.sparkSession.sql(f"""
                MERGE INTO delta.`{ETAT_COURANT_PATH}` AS cible
                USING maj_etat_capteur AS source
                ON cible.capteur_id = source.capteur_id
                WHEN MATCHED AND source.dernier_timestamp >= cible.dernier_timestamp
                    THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

        # c) Serving layer : la table est petite (1 ligne/capteur), on la
        #    recopie entierement (truncate + overwrite) a chaque batch.
        etat_complet = spark.read.format("delta").load(ETAT_COURANT_PATH)
        write_postgres(etat_complet, "etat_courant_capteur",
                       mode="overwrite", truncate=True)

    return (
        silver.writeStream.foreachBatch(merge_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold_etat_courant")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main():
    spark = build_spark("gold-pipeline")

    build_dimensions(spark)

    silver = spark.readStream.format("delta").load(SILVER_PATH)

    start_fait_mesures(silver)
    start_agg_machine(silver)
    start_etat_courant(spark, silver)

    print("Pipeline Gold demarre : fait_mesures, agg_machine_5min, "
          "etat_courant_capteur (+ dimensions ecrites).")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
