import os
import sys

# Fix pour la compatibilité Java 17+ / Java 21+ avec Spark (exécution hors Docker)
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
    # 1. Détection dynamique des chemins (Docker Container vs Local Host)
    if os.path.exists("/opt/data"):
        print(">>> Détection de l'environnement DOCKER.")
        referentiels_dir = "/opt/data"
        silver_path = "/opt/lakehouse/silver"
        fact_gold_path = "/opt/lakehouse/gold/fact_machine_aggregations"
        dim_gold_path = "/opt/lakehouse/gold/dim_sensor_state"
        chk_fact_path = "/opt/checkpoints/fact_machine_aggregations"
        chk_dim_path = "/opt/checkpoints/dim_sensor_state"
        postgres_host = "postgres"
        warehouse_dir = "/opt/spark-warehouse"
    else:
        print(">>> Détection de l'environnement LOCAL HOST.")
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        referentiels_dir = os.path.join(base_dir, "data")
        silver_path = os.path.join(base_dir, "data", "delta", "silver")
        fact_gold_path = os.path.join(base_dir, "data", "delta", "gold", "fact_machine_aggregations")
        dim_gold_path = os.path.join(base_dir, "data", "delta", "gold", "dim_sensor_state")
        chk_fact_path = os.path.join(base_dir, "data", "checkpoints", "fact_machine_aggregations")
        chk_dim_path = os.path.join(base_dir, "data", "checkpoints", "dim_sensor_state")
        postgres_host = "localhost"
        warehouse_dir = os.path.join(base_dir, "spark-warehouse")

    postgres_url = f"jdbc:postgresql://{postgres_host}:5432/warehouse"

    # Initialisation de la Session Spark avec support Delta Lake
    spark = SparkSession.builder \
        .appName("GoldPipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    print("Spark Session initialisée pour la couche GOLD.")

    # 2. Chargement des référentiels statiques (CSVs) depuis /opt/data ou data/
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

    # 3. Lecture en streaming de la table Delta Silver
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

    # Écriture double destinations : Delta Lake & Postgres
    def write_fact_batch(batch_df, batch_id):
        print(f"Début écriture Fact batch {batch_id}...")
        # A. Delta (Source de vérité)
        batch_df.write.format("delta").mode("append").save(fact_gold_path)
        
        # B. Postgres (Serving Layer pour Metabase)
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", "gold.fact_machine_aggregations") \
                .option("user", "warehouse") \
                .option("password", "warehouse") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"Fact batch {batch_id} écrit dans Postgres.")
        except Exception as e:
            print(f"Erreur d'écriture Fact batch {batch_id} dans Postgres (JDBC) : {e}")

    query_fact = df_fact_aggregations.writeStream \
        .foreachBatch(write_fact_batch) \
        .option("checkpointLocation", chk_fact_path) \
        .start()

    # =========================================================================
    # JOB 2 : Table de Dimension (État Courant Capteur via MERGE INTO)
    # =========================================================================
    print("Configuration du Job 2 : État courant des capteurs (MERGE)...")

    def merge_sensor_state(batch_df, batch_id):
        print(f"Début du traitement du micro-batch {batch_id} (merge)...")
        
        # 1. Déduplication au sein du batch : garder uniquement la mesure la plus récente par capteur
        window_spec = Window.partitionBy("capteur_id").orderBy(col("timestamp").desc())
        latest_sensor_records = batch_df \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")

        # 2. Jointure avec référentiels statiques (en enlevant type_mesure du CSV pour éviter l'ambiguïté)
        # Note : region et criticite sont sans accent dans machines.csv et sites.csv officiels.
        enriched_batch = latest_sensor_records \
            .join(df_capteurs.drop("type_mesure"), "capteur_id", "left") \
            .join(df_machines, "machine_id", "left") \
            .join(df_sites, "site_id", "left") \
            .select(
                col("capteur_id"),
                col("machine_id"),
                col("site_id"),
                col("nom").alias("site_nom"),
                col("region").alias("site_region"),
                col("responsable_site"),
                col("type_machine"),
                col("ligne_production"),
                col("criticite").alias("machine_criticite"),
                col("responsable_technique").alias("machine_responsable_technique"),
                col("fabricant").alias("capteur_fabricant"),
                col("precision_capteur"),
                col("type_mesure"),
                col("valeur").alias("derniere_valeur"),
                col("unite"),
                col("qualite_signal"),
                col("batterie_pourcentage"),
                col("timestamp").alias("dernier_timestamp"),
                col("est_anomalie"),
                col("type_anomalie"),
                col("seuil_min"),
                col("seuil_max"),
                col("est_valide")
            )

        # 3. MERGE INTO avec Delta Lake API dans la table de dimension
        from delta.tables import DeltaTable
        
        if not DeltaTable.isDeltaTable(spark, dim_gold_path):
            print(f"Initialisation de la table Gold Delta à l'emplacement : {dim_gold_path}")
            enriched_batch.write.format("delta").mode("overwrite").save(dim_gold_path)
        else:
            delta_target = DeltaTable.forPath(spark, dim_gold_path)
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
                    "est_anomalie": "source.est_anomalie",
                    "type_anomalie": "source.type_anomalie",
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
                    "est_anomalie": "source.est_anomalie",
                    "type_anomalie": "source.type_anomalie",
                    "seuil_min": "source.seuil_min",
                    "seuil_max": "source.seuil_max",
                    "est_valide": "source.est_valide"
                }) \
                .execute()
            print(f"Merge Delta complété pour le batch {batch_id}.")

        # 4. Écriture / Synchronisation avec Postgres (Serving Layer pour Metabase)
        try:
            df_full_dim = spark.read.format("delta").load(dim_gold_path)
            df_full_dim.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", "gold.dim_sensor_state") \
                .option("user", "warehouse") \
                .option("password", "warehouse") \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
            print(f"Dim sensor state écrit avec succès dans Postgres (overwrite).")
        except Exception as e:
            print(f"Erreur de synchronisation Dim sensor state dans Postgres (JDBC) : {e}")

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
