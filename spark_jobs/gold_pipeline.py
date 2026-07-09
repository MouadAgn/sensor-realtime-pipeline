import os
import sys

# Fix pour la compatibilité Java 17+ / Java 21+ avec Spark (lors de l'exécution hors Docker)
os.environ["JAVA_TOOL_OPTIONS"] = (
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED"
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, mean, max, count, row_number
from pyspark.sql.window import Window

def main():
    # Définition des chemins absolus
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    referentiels_dir = os.path.join(base_dir, "data", "referentiels")
    silver_path = os.path.join(base_dir, "data", "delta", "silver")
    
    fact_gold_path = os.path.join(base_dir, "data", "delta", "gold", "fact_machine_aggregations")
    dim_gold_path = os.path.join(base_dir, "data", "delta", "gold", "dim_sensor_state")
    
    chk_fact_path = os.path.join(base_dir, "data", "checkpoints", "fact_machine_aggregations")
    chk_dim_path = os.path.join(base_dir, "data", "checkpoints", "dim_sensor_state")

    # Initialisation de la Session Spark avec support Delta Lake
    spark = SparkSession.builder \
        .appName("GoldPipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", os.path.join(base_dir, "spark-warehouse")) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    print("Spark Session initialisée pour la couche GOLD.")

    # 1. Chargement des référentiels statiques (CSVs)
    print("Chargement des référentiels statiques...")
    df_capteurs = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(os.path.join(referentiels_dir, "capteurs.csv"))
        
    df_machines = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(os.path.join(referentiels_dir, "machines.csv"))
        
    df_sites = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(os.path.join(referentiels_dir, "sites.csv"))

    # 2. Lecture en streaming de la table Delta Silver
    print(f"Lecture du flux Silver depuis : {silver_path}")
    df_silver = spark.readStream \
        .format("delta") \
        .load(silver_path)

    # =========================================================================
    # JOB 1 : Table de Faits (Agrégations Temporelles Glissantes)
    # =========================================================================
    print("Configuration du Job 1 : Agrégations temporelles glissantes...")
    
    # Agrégation fenêtrée glissante (Fenêtre 5 minutes, pas de 1 minute)
    # avec un Watermark de 10 minutes pour gérer le retard
    df_fact_aggregations = df_silver \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            col("machine_id"),
            col("type_mesure")
        ) \
        .agg(
            mean(col("valeur")).alias("valeur_moyenne"),
            max(col("valeur")).alias("valeur_maximale"),
            count(col("event_id")).alias("nombre_mesures")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("machine_id"),
            col("type_mesure"),
            col("valeur_moyenne"),
            col("valeur_maximale"),
            col("nombre_mesures")
        )

    # Écriture en mode streaming append
    query_fact = df_fact_aggregations.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", chk_fact_path) \
        .start(fact_gold_path)

    # =========================================================================
    # JOB 2 : Table de Dimension (État Courant Capteur via MERGE INTO)
    # =========================================================================
    print("Configuration du Job 2 : État courant des capteurs (MERGE)...")

    def merge_sensor_state(batch_df, batch_id):
        """
        Fonction exécutée sur chaque micro-batch pour dédupliquer et fusionner (MERGE) 
        les données dans la table Delta Gold de dimension.
        """
        print(f"Début du traitement du micro-batch {batch_id}...")
        
        # 1. Déduplication au sein du batch : garder uniquement la mesure la plus récente par capteur
        window_spec = Window.partitionBy("capteur_id").orderBy(col("timestamp").desc())
        latest_sensor_records = batch_df \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")

        # 2. Enrichissement avec les référentiels statiques
        # Nous renommons les colonnes de jointure pour éviter les conflits et clarifier le schéma
        enriched_batch = latest_sensor_records \
            .join(df_capteurs.drop("type_mesure"), "capteur_id", "left") \
            .join(df_machines, "machine_id", "left") \
            .join(df_sites, "site_id", "left") \
            .select(
                col("capteur_id"),
                col("machine_id"),
                col("site_id"),
                col("nom").alias("site_nom"),
                col("région").alias("site_region"),
                col("responsable_site"),
                col("type_machine"),
                col("ligne_production"),
                col("criticité").alias("machine_criticite"),
                col("responsable_technique").alias("machine_responsable_technique"),
                col("fabricant").alias("capteur_fabricant"),
                col("precision_capteur"),
                col("type_mesure"),
                col("valeur").alias("derniere_valeur"),
                col("unite"),
                col("qualite_signal"),
                col("batterie_pourcentage"),
                col("timestamp").alias("dernier_timestamp"),
                col("is_anomaly"),
                col("anomaly_reason"),
                col("seuil_min"),
                col("seuil_max"),
                col("est_valide")
            )

        # 3. MERGE INTO avec Delta Lake API
        from delta.tables import DeltaTable
        
        # Si la table Gold cible n'existe pas encore, on la crée à partir du premier batch
        if not DeltaTable.isDeltaTable(spark, dim_gold_path):
            print(f"Initialisation de la table Gold Delta à l'emplacement : {dim_gold_path}")
            enriched_batch.write.format("delta").mode("overwrite").save(dim_gold_path)
        else:
            delta_target = DeltaTable.forPath(spark, dim_gold_path)
            
            # Exécution de la fusion Delta (MERGE INTO)
            delta_target.alias("target") \
                .merge(
                    enriched_batch.alias("source"),
                    "target.capteur_id = source.capteur_id"
                ) \
                .whenMatchedUpdate(set={
                    "machine_id": "source.machine_id",
                    "site_id": "source.site_id",
                    "site_nom": "source.site_nom",
                    "site_region": "source.site_region",
                    "responsable_site": "source.responsable_site",
                    "type_machine": "source.type_machine",
                    "ligne_production": "source.ligne_production",
                    "machine_criticite": "source.machine_criticite",
                    "machine_responsable_technique": "source.machine_responsable_technique",
                    "capteur_fabricant": "source.capteur_fabricant",
                    "precision_capteur": "source.precision_capteur",
                    "type_mesure": "source.type_mesure",
                    "derniere_valeur": "source.derniere_valeur",
                    "unite": "source.unite",
                    "qualite_signal": "source.qualite_signal",
                    "batterie_pourcentage": "source.batterie_pourcentage",
                    "dernier_timestamp": "source.dernier_timestamp",
                    "is_anomaly": "source.is_anomaly",
                    "anomaly_reason": "source.anomaly_reason",
                    "seuil_min": "source.seuil_min",
                    "seuil_max": "source.seuil_max",
                    "est_valide": "source.est_valide"
                }) \
                .whenNotMatchedInsert(values={
                    "capteur_id": "source.capteur_id",
                    "machine_id": "source.machine_id",
                    "site_id": "source.site_id",
                    "site_nom": "source.site_nom",
                    "site_region": "source.site_region",
                    "responsable_site": "source.responsable_site",
                    "type_machine": "source.type_machine",
                    "ligne_production": "source.ligne_production",
                    "machine_criticite": "source.machine_criticite",
                    "machine_responsable_technique": "source.machine_responsable_technique",
                    "capteur_fabricant": "source.capteur_fabricant",
                    "precision_capteur": "source.precision_capteur",
                    "type_mesure": "source.type_mesure",
                    "derniere_valeur": "source.derniere_valeur",
                    "unite": "source.unite",
                    "qualite_signal": "source.qualite_signal",
                    "batterie_pourcentage": "source.batterie_pourcentage",
                    "dernier_timestamp": "source.dernier_timestamp",
                    "is_anomaly": "source.is_anomaly",
                    "anomaly_reason": "source.anomaly_reason",
                    "seuil_min": "source.seuil_min",
                    "seuil_max": "source.seuil_max",
                    "est_valide": "source.est_valide"
                }) \
                .execute()
            print(f"Merge complété pour le batch {batch_id}.")

    # Écriture du flux avec foreachBatch
    query_dim = df_silver.writeStream \
        .foreachBatch(merge_sensor_state) \
        .option("checkpointLocation", chk_dim_path) \
        .start()

    print("Le pipeline Gold a démarré et attend des données...")
    
    # Attente de la fin des streams
    query_fact.awaitTermination()
    query_dim.awaitTermination()

if __name__ == "__main__":
    main()
