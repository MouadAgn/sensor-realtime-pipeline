"""
Smoke-test INFRA (appartient au lot infra, supprimable).

Prouve, sans dépendre des jobs des collègues, que :
  - la SparkSession démarre sur le cluster (spark://spark-master:7077)
  - Delta Lake est bien configuré (extensions + catalog)
  - le référentiel CSV est lisible sous /opt/data
  - l'écriture Delta dans /opt/lakehouse fonctionne (permissions OK)
  - DESCRIBE HISTORY (time travel) répond

Lancer :
    docker compose exec spark-master spark-submit /opt/spark-apps/_infra_smoke.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("infra-smoke").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(">>> Spark master :", spark.conf.get("spark.master"))

# 1. Lecture du référentiel
capteurs = spark.read.option("header", True).csv("/opt/data/capteurs.csv")
print(">>> capteurs.csv :", capteurs.count(), "lignes")
capteurs.show(3, truncate=False)

# 2. Écriture Delta
path = "/opt/lakehouse/_smoke"
capteurs.write.format("delta").mode("overwrite").save(path)
print(">>> écriture Delta OK ->", path)

# 3. Relecture + time travel
back = spark.read.format("delta").load(path)
print(">>> relecture Delta :", back.count(), "lignes")

print(">>> DESCRIBE HISTORY :")
spark.sql(f"DESCRIBE HISTORY delta.`{path}`").select("version", "operation", "timestamp").show(truncate=False)

print(">>> SMOKE TEST OK ✅")
spark.stop()
