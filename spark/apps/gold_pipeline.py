"""Gold Layer: Modélisation en étoile + mécanismes Delta.

MODÉLISATION:
1. Dimensions (batch, OVERWRITE): dim_capteur, dim_machine, dim_site
   - Construites depuis les CSV référentiels, quasi statiques
2. Table de faits (streaming, APPEND): fait_mesures
   - 1 ligne par mesure valide, clés étrangères vers les dimensions
3. Agrégation fenêtrée (streaming, APPEND): agg_machine_5min
   - Moyenne/max glissants par machine (fenêtre 5 min, pas 1 min)
4. État courant capteur (MERGE INTO): etat_courant_capteur
   - Dernière valeur/statut par capteur, mis à jour par MERGE

MÉCANISMES DELTA:
- APPEND (tables 2 et 3): streaming classique
- MERGE INTO (table 4): upsert sur clé primaire

ADAPTATION DATABRICKS:
- Toutes les tables sont en Unity Catalog
- Postgres JDBC supprimé (utiliser Data Lakehouse pour BI ou Delta Sharing)
- spark.catalog.tableExists au lieu de os.path.exists
- trigger(availableNow=True) au lieu de processingTime (serverless)
- Les CSV doivent être dans /Volumes/main/default/data_ref
"""

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (
    CHECKPOINT_DIR, CAPTEURS_CSV, MACHINES_CSV, SITES_CSV,
    GOLD_SCHEMA, SILVER_TABLE, build_spark,
)

# Tables Gold en Unity Catalog
FAIT_MESURES_TABLE = f"{GOLD_SCHEMA}_fait_mesures"
AGG_MACHINE_TABLE = f"{GOLD_SCHEMA}_agg_machine_5min"
ETAT_COURANT_TABLE = f"{GOLD_SCHEMA}_etat_courant_capteur"

DIM_CAPTEUR_TABLE = f"{GOLD_SCHEMA}_dim_capteur"
DIM_MACHINE_TABLE = f"{GOLD_SCHEMA}_dim_machine"
DIM_SITE_TABLE = f"{GOLD_SCHEMA}_dim_site"


# ---------------------------------------------------------------------------
# 1. DIMENSIONS — batch depuis les CSV référentiels (overwrite: quasi statique)
# ---------------------------------------------------------------------------
def build_dimensions(spark):
    """Construit les tables de dimensions depuis les CSV.
    
    IMPORTANT: Les fichiers CSV doivent être uploadés dans:
    - /Volumes/main/default/data_ref/capteurs.csv
    - /Volumes/main/default/data_ref/machines.csv
    - /Volumes/main/default/data_ref/sites.csv
    """
    def read_csv(path):
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )

    dims = {
        DIM_CAPTEUR_TABLE: read_csv(CAPTEURS_CSV),
        DIM_MACHINE_TABLE: read_csv(MACHINES_CSV),
        DIM_SITE_TABLE: read_csv(SITES_CSV),
    }
    
    for table, df in dims.items():
        df.write.format("delta").mode("overwrite").saveAsTable(table)
        print(f"Dimension {table} écrite: {df.count()} lignes")


# ---------------------------------------------------------------------------
# 2. FAIT_MESURES — append streaming (mécanisme Delta n°1: append)
# ---------------------------------------------------------------------------
def start_fait_mesures(silver):
    """Table de faits: toutes les mesures valides."""
    fait = silver.filter(F.col("est_valide")).select(
        "event_id", "capteur_id", "machine_id", "site_id", "type_mesure",
        "valeur", "unite", "qualite_signal", "batterie_pourcentage",
        "timestamp", "date_event", "est_anomalie", "type_anomalie",
    )

    return (
        fait.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold_fait_mesures")
        .trigger(availableNow=True)  # Serverless: availableNow au lieu de processingTime
        .partitionBy("date_event")
        .toTable(FAIT_MESURES_TABLE)
    )


# ---------------------------------------------------------------------------
# 3. AGG_MACHINE_5MIN — agrégation glissante par machine (append streaming)
#    Fenêtre 5 min / pas 1 min, watermark 2 min: une fenêtre n'est émise
#    qu'une fois finalisée (mode append) => pas de doublons de fenêtres.
# ---------------------------------------------------------------------------
def start_agg_machine(silver):
    """Agrégations par machine avec fenêtres glissantes."""
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

    return (
        agg.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold_agg_machine_5min")
        .trigger(availableNow=True)  # Serverless: availableNow au lieu de processingTime
        .toTable(AGG_MACHINE_TABLE)
    )


# ---------------------------------------------------------------------------
# 4. ETAT_COURANT_CAPTEUR — MERGE INTO (mécanisme Delta n°2)
#    Mise à jour par MERGE INTO à chaque nouvel événement, PAS un append
# ---------------------------------------------------------------------------
def start_etat_courant(spark, silver):
    """État courant de chaque capteur avec MERGE INTO."""
    
    def merge_batch(batch_df, batch_id):
        # a) Un MERGE refuse 2 lignes source pour la même clé: on ne garde
        #    que la mesure la plus récente par capteur dans le micro-batch.
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

        # b) MERGE INTO: update si le capteur existe, insert sinon
        #    Utilisation de spark.catalog.tableExists au lieu de os.path.exists
        if not spark.catalog.tableExists(ETAT_COURANT_TABLE):
            # Première écriture: créer la table
            derniers.write.format("delta").mode("overwrite").saveAsTable(ETAT_COURANT_TABLE)
        else:
            # MERGE INTO pour les mises à jour suivantes
            # La vue temporaire vit dans la session du micro-batch
            derniers.createOrReplaceTempView("maj_etat_capteur")
            
            # La condition sur dernier_timestamp protège des micro-batches
            # rejoués après un redémarrage sur checkpoint (at-least-once).
            derniers.sparkSession.sql(f"""
                MERGE INTO {ETAT_COURANT_TABLE} AS cible
                USING maj_etat_capteur AS source
                ON cible.capteur_id = source.capteur_id
                WHEN MATCHED AND source.dernier_timestamp >= cible.dernier_timestamp
                    THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

    return (
        silver.writeStream.foreachBatch(merge_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold_etat_courant")
        .trigger(availableNow=True)  # Serverless: availableNow au lieu de processingTime
        .start()
    )


def main():
    spark = build_spark("gold-pipeline")

    # Construire les dimensions (batch)
    build_dimensions(spark)

    # Lire le flux Silver
    silver = spark.readStream.table(SILVER_TABLE)

    # Démarrer les 3 flux streaming
    query_fait = start_fait_mesures(silver)
    query_agg = start_agg_machine(silver)
    query_etat = start_etat_courant(spark, silver)

    print(f"Pipeline Gold démarré:")
    print(f"  - Dimensions écrites: {DIM_CAPTEUR_TABLE}, {DIM_MACHINE_TABLE}, {DIM_SITE_TABLE}")
    print(f"  - Fait mesures: {FAIT_MESURES_TABLE}")
    print(f"  - Agrégations machine: {AGG_MACHINE_TABLE}")
    print(f"  - État courant: {ETAT_COURANT_TABLE}")
    print(f">>> Mode: availableNow (traite toutes les données disponibles puis s'arrête)")
    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
