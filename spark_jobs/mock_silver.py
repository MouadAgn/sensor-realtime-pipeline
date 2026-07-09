import os
import sys
from datetime import datetime, timedelta
from shutil import rmtree

# Fix pour la compatibilité Java 17+ / Java 21+ avec Spark (exécution hors Docker)
os.environ["JAVA_TOOL_OPTIONS"] = (
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED"
)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, BooleanType, DateType

def create_mock_silver_table():
    # Détection dynamique des chemins (Docker vs Local)
    if os.path.exists("/opt/data"):
        silver_path = "/opt/lakehouse/silver"
        warehouse_dir = "/opt/spark-warehouse"
    else:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        silver_path = os.path.join(base_dir, "data", "delta", "silver")
        warehouse_dir = os.path.join(base_dir, "spark-warehouse")
    
    # Configuration SparkSession avec Delta
    spark = SparkSession.builder \
        .appName("MockSilverGenerator") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .getOrCreate()
        
    print(f"Spark Session initialisée. Écriture de la table Silver dans : {silver_path}")

    # Schéma de la table Silver (Conforme à INFRA.md)
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("capteur_id", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("type_mesure", StringType(), True),
        StructField("valeur", DoubleType(), True),
        StructField("unite", StringType(), True),
        StructField("qualite_signal", DoubleType(), True),
        StructField("batterie_pourcentage", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("est_anomalie", BooleanType(), True),
        StructField("type_anomalie", StringType(), True),
        StructField("date_event", DateType(), True),
        StructField("seuil_min", DoubleType(), True),
        StructField("seuil_max", DoubleType(), True),
        StructField("est_valide", BooleanType(), True)
    ])

    # Génération de données de test
    now = datetime.now()
    data = []
    
    # cpt-042 (temperature, seuil 10.0 à 85.0)
    for i in range(20):
        evt_time = now - timedelta(seconds=30 * i)
        val = 20.0 + (i * 2.5) % 60.0
        est_anom = False
        type_anom = None
        
        if i == 5:
            val = 92.5
            est_anom = True
            type_anom = "Température supérieure au seuil max (85.0°C)"
        elif i == 12:
            val = 8.0
            est_anom = True
            type_anom = "Température inférieure au seuil min (10.0°C)"
            
        data.append((
            f"evt-temp-{i}", "cpt-042", "m-07", "site-lyon", "temperature",
            val, "celsius", 0.98 - (i * 0.01) % 0.1, 80 - i % 10,
            evt_time, est_anom, type_anom, evt_time.date(), 10.0, 85.0, not est_anom
        ))

    # cpt-017 (vibration, seuil 0.0 à 12.0)
    for i in range(20):
        evt_time = now - timedelta(seconds=30 * i + 10)
        val = 2.0 + (i * 0.8) % 8.0
        est_anom = False
        type_anom = None
        
        if i == 8:
            val = 14.5
            est_anom = True
            type_anom = "Vibration supérieure au seuil max (12.0)"
            
        data.append((
            f"evt-vib-{i}", "cpt-017", "m-07", "site-lyon", "vibration",
            val, "hz", 0.95, 75 - i % 5,
            evt_time, est_anom, type_anom, evt_time.date(), 0.0, 12.0, not est_anom
        ))

    # cpt-088 (pression, seuil 50.0 à 150.0)
    for i in range(20):
        evt_time = now - timedelta(seconds=30 * i + 15)
        val = 90.0 + (i * 4.0) % 50.0
        est_anom = False
        type_anom = None
        
        if i == 15:
            val = 165.0
            est_anom = True
            type_anom = "Pression supérieure au seuil max (150.0)"
            
        data.append((
            f"evt-pres-{i}", "cpt-088", "m-12", "site-nantes", "pression",
            val, "bar", 0.97, 90 - i % 8,
            evt_time, est_anom, type_anom, evt_time.date(), 50.0, 150.0, not est_anom
        ))

    # Nettoyage
    if os.path.exists(silver_path):
        print(f"Nettoyage de l'ancienne table Silver : {silver_path}")
        rmtree(silver_path)

    # Création du DataFrame et écriture Delta
    df = spark.createDataFrame(data, schema)
    print(f"Écriture de {df.count()} lignes...")
    df.write.format("delta").save(silver_path)
    print("Table Delta Silver générée !")
    
    spark.read.format("delta").load(silver_path).show(5, truncate=False)
    spark.stop()

if __name__ == "__main__":
    create_mock_silver_table()
