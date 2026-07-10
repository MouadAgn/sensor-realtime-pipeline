"""
Smoke-test INFRA pour Databricks (version adaptée pour serverless compute).

Prouve que :
  - la SparkSession démarre sur Databricks
  - Delta Lake est bien configuré (natif sur Databricks)
  - l'écriture Delta fonctionne (permissions OK)
  - DESCRIBE HISTORY (time travel) répond

Exécuter ce fichier directement dans Databricks.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("infra-smoke").getOrCreate()

print(">>> Spark version :", spark.version)
print(">>> Databricks Runtime actif ✓")

# 1. Création de données de test (simule capteurs.csv)
data = [
    ("CAPT001", "Température", "Salle A"),
    ("CAPT002", "Humidité", "Salle B"),
    ("CAPT003", "Pression", "Salle C"),
    ("CAPT004", "CO2", "Salle D"),
    ("CAPT005", "Luminosité", "Salle E")
]
columns = ["id_capteur", "type", "localisation"]

capteurs = spark.createDataFrame(data, columns)
print(">>> Données test créées :", capteurs.count(), "lignes")
capteurs.show(truncate=False)

# 2. Écriture Delta en table temporaire
table_name = "_infra_smoke_test"
capteurs.write.format("delta").mode("overwrite").saveAsTable(table_name)
print(f">>> écriture Delta OK -> table '{table_name}'")

# 3. Relecture + time travel
back = spark.table(table_name)
print(">>> relecture Delta :", back.count(), "lignes")

print(">>> DESCRIBE HISTORY :")
spark.sql(f"DESCRIBE HISTORY {table_name}").select("version", "operation", "timestamp").show(truncate=False)

# 4. Nettoyage
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
print(f">>> table '{table_name}' nettoyée")

print(">>> SMOKE TEST OK ✅")
print(">>> Databricks Spark + Delta Lake opérationnels")
