# spark/apps — Jobs Spark des collègues

Déposez ici vos scripts PySpark (Bronze / Silver / Gold). Ce dossier est monté
dans `spark-master` **et** `spark-worker` sous `/opt/spark-apps`.

## Soumettre un job

```bash
docker compose exec spark-master spark-submit /opt/spark-apps/<votre_job>.py
```

Les jars Delta + Kafka + Postgres et la config Delta sont **déjà** dans l'image :
pas besoin de `--packages`. Une `SparkSession` classique suffit :

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("bronze").getOrCreate()  # config Delta héritée
```

## Contrats fournis par l'infra (chemins DANS les conteneurs)

| Ressource                     | Valeur                                                        |
|-------------------------------|--------------------------------------------------------------|
| Kafka (bootstrap)             | `kafka:9092`                                                  |
| Topic                         | `sensors-data`                                                |
| Référentiel CSV               | `/opt/data/capteurs.csv`, `machines.csv`, `sites.csv`, `seuils_machine.csv` |
| Tables Delta (Bronze/Silver/Gold) | sous `/opt/lakehouse/...` (ex. `/opt/lakehouse/bronze`)  |
| Checkpoints streaming         | sous `/opt/checkpoints/<nom_du_job>`                         |
| Postgres serving (Gold → BI)  | `jdbc:postgresql://postgres:5432/warehouse` — `warehouse`/`warehouse`, schéma `gold` |

### Lire le flux Kafka (Bronze)

```python
df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "sensors-data")
      .option("startingOffsets", "earliest")
      .load())
```

### Écrire le Gold vers Postgres (pour Metabase)

```python
(df.write.format("jdbc")
   .option("url", "jdbc:postgresql://postgres:5432/warehouse")
   .option("dbtable", "gold.fait_mesures")
   .option("user", "warehouse").option("password", "warehouse")
   .mode("overwrite").save())
```
