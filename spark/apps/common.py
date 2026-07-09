### constantes share des jobs Bronze et silver et concernant tout le reste vient de spark-defaults.conf
import os

from pyspark.sql import SparkSession

LAKEHOUSE = "/opt/lakehouse"
BRONZE_PATH = f"{LAKEHOUSE}/bronze"
SILVER_PATH = f"{LAKEHOUSE}/silver"
CHECKPOINT_DIR = "/opt/checkpoints"

DATA_DIR = "/opt/data" # referentiel CSV
CAPTEURS_CSV = f"{DATA_DIR}/capteurs.csv"
SEUILS_MACHINE_CSV = f"{DATA_DIR}/seuils_machine.csv"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensors-data")

def build_spark(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()