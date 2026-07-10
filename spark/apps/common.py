"""Configuration partagée pour les jobs Bronze, Silver et Gold.

ADAPTATION DATABRICKS:
- Chemins Docker (/opt/...) remplacés par Unity Catalog et Volumes
- Les référentiels CSV doivent être uploadés dans le volume data_ref
- Les checkpoints utilisent un volume UC (DBFS root désactivé sur serverless)
- Kafka: si non configuré, les jobs streaming échoueront (normal pour les tests)
- Utilise le catalog/schema courant au lieu de hardcoder 'main.default'
"""
import os
from pyspark.sql import SparkSession

def build_spark(app_name):
    """Crée une SparkSession pour Databricks.
    
    Sur Databricks, la session est déjà configurée avec Delta Lake.
    Pas besoin de configuration supplémentaire pour serverless compute.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def get_catalog_schema():
    """Retourne le catalog et schema courants."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = build_spark("get-catalog-schema")
    
    current_catalog = spark.catalog.currentCatalog()
    current_schema = spark.catalog.currentDatabase()
    return current_catalog, current_schema

# Get current catalog and schema dynamically
CURRENT_CATALOG, CURRENT_SCHEMA = get_catalog_schema()

# === LAKEHOUSE LAYERS (Unity Catalog tables) ===
# Les tables sont créées dans le schéma courant de l'utilisateur
BRONZE_TABLE = f"{CURRENT_CATALOG}.{CURRENT_SCHEMA}.bronze_sensor_data"
SILVER_TABLE = f"{CURRENT_CATALOG}.{CURRENT_SCHEMA}.silver_sensor_data"

# Préfixes pour les tables gold (dimensions + faits)
GOLD_PREFIX = "gold"

# Tables Gold spécifiques
GOLD_DIM_CAPTEUR = f"{CURRENT_CATALOG}.{CURRENT_SCHEMA}.gold_dim_capteur"
GOLD_DIM_MACHINE = f"{CURRENT_CATALOG}.{CURRENT_SCHEMA}.gold_dim_machine"
GOLD_DIM_SITE = f"{CURRENT_CATALOG}.{CURRENT_SCHEMA}.gold_dim_site"
GOLD_FAIT_MESURES = f"{CURRENT_CATALOG}.{CURRENT_SCHEMA}.gold_fait_mesures"
GOLD_AGG_MACHINE_5MIN = f"{CURRENT_CATALOG}.{CURRENT_SCHEMA}.gold_agg_machine_5min"
GOLD_ETAT_COURANT_CAPTEUR = f"{CURRENT_CATALOG}.{CURRENT_SCHEMA}.gold_etat_courant_capteur"

# === PATHS POUR CHECKPOINTS STREAMING ===
# Les checkpoints doivent être dans un volume UC (DBFS root désactivé sur serverless)
# Le volume doit être créé avec: CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.checkpoints;
CHECKPOINT_DIR = f"/Volumes/{CURRENT_CATALOG}/{CURRENT_SCHEMA}/checkpoints"

# === RÉFÉRENTIELS CSV (Unity Catalog Volumes) ===
# Ces fichiers doivent être uploadés dans un volume Unity Catalog
# Créez le volume avec: CREATE VOLUME IF NOT EXISTS <catalog>.<schema>.data_ref;
# Puis uploadez les fichiers via l'UI ou dbutils.fs.cp
VOLUME_PATH = f"/Volumes/{CURRENT_CATALOG}/{CURRENT_SCHEMA}/data_ref"
CAPTEURS_CSV = f"{VOLUME_PATH}/capteurs.csv"
SEUILS_MACHINE_CSV = f"{VOLUME_PATH}/seuils_machine.csv"
MACHINES_CSV = f"{VOLUME_PATH}/machines.csv"
SITES_CSV = f"{VOLUME_PATH}/sites.csv"

# === KAFKA CONFIGURATION ===
# Pour les tests sans Kafka, ces valeurs restent mais les jobs streaming échoueront
# Pour production: configurez avec votre Event Hub, Confluent Cloud, ou Kafka cluster
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "your-kafka-server:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensors-data")

# Print configuration for debugging
def print_config():
    """Affiche la configuration actuelle."""
    print("=" * 80)
    print("PIPELINE CONFIGURATION")
    print("=" * 80)
    print(f"Catalog: {CURRENT_CATALOG}")
    print(f"Schema: {CURRENT_SCHEMA}")
    print(f"\nTables:")
    print(f"  Bronze: {BRONZE_TABLE}")
    print(f"  Silver: {SILVER_TABLE}")
    print(f"  Gold Prefix: {GOLD_PREFIX}")
    print(f"\nPaths:")
    print(f"  Checkpoints: {CHECKPOINT_DIR}")
    print(f"  Data Ref: {VOLUME_PATH}")
    print(f"\nKafka:")
    print(f"  Bootstrap: {KAFKA_BOOTSTRAP}")
    print(f"  Topic: {KAFKA_TOPIC}")
    print("=" * 80)
