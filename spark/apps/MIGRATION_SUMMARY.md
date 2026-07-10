# Migration Summary: Docker → Databricks Serverless

## Fichiers Adaptés

✅ **common.py** — Configuration centralisée
✅ **bronze_ingest.py** — Ingestion Kafka
✅ **silver_clean.py** — Nettoyage et validation
✅ **gold_pipeline.py** — Modélisation en étoile
✅ **_infra_smoke.py** — Test d'infrastructure
✅ **README.md** — Documentation complète

---

## Changements Critiques Appliqués

### 1. ❌ SparkContext Supprimé (Serverless Incompatible)

**Problème**: `spark.sparkContext` n'existe pas sur serverless compute

**Avant**:
```python
spark.sparkContext.setLogLevel("WARN")  # ❌ Erreur
spark.conf.get("spark.master")  # ❌ Erreur
```

**Après**:
```python
# Supprimé - pas nécessaire sur Databricks
# La configuration se fait au niveau du cluster
```

---

### 2. ❌ Streaming Trigger ProcessingTime (Serverless Incompatible)

**Problème**: `trigger(processingTime=...)` n'est pas supporté sur serverless

**Avant**:
```python
.trigger(processingTime="10 seconds")  # ❌ INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED
```

**Après**:
```python
.trigger(availableNow=True)  # ✅ Traite toutes les données disponibles puis s'arrête
```

**Impact**: Le streaming devient un traitement batch incrémental. Pour du streaming continu, utiliser un cluster classique.

---

### 3. ❌ DBFS Root Désactivé (Unity Catalog)

**Problème**: DBFS root (`dbfs:/FileStore/`) est bloqué sur les workspaces avec Unity Catalog

**Avant**:
```python
CHECKPOINT_DIR = "dbfs:/FileStore/sensor-pipeline/checkpoints"  # ❌ DBFS_DISABLED
```

**Après**:
```python
CHECKPOINT_DIR = "/Volumes/main/default/checkpoints"  # ✅ Volume UC
```

**Action requise**:
```sql
CREATE VOLUME IF NOT EXISTS main.default.checkpoints;
```

---

### 4. Chemins Fichiers → Tables Unity Catalog

**Problème**: Paths Docker (`/opt/lakehouse/...`) n'existent pas sur Databricks

**Avant**:
```python
BRONZE_PATH = "/opt/lakehouse/bronze"
silver.writeStream.format("delta").start(BRONZE_PATH)  # Path-based
```

**Après**:
```python
BRONZE_TABLE = "bronze_sensor_data"
silver.writeStream.format("delta").toTable(BRONZE_TABLE)  # Table-based
```

**Avantages**:
- Gestion automatique des métadonnées par Unity Catalog
- Gouvernance des données (permissions, lineage, audit)
- Pas besoin de gérer les chemins manuellement

---

### 5. CSV Référentiels → Volumes Unity Catalog

**Problème**: `/opt/data/*.csv` n'existe pas sur Databricks

**Avant**:
```python
capteurs = spark.read.csv("/opt/data/capteurs.csv")  # ❌ Path not found
```

**Après**:
```python
CAPTEURS_CSV = "/Volumes/main/default/data_ref/capteurs.csv"
capteurs = spark.read.csv(CAPTEURS_CSV)  # ✅ Volume UC
```

**Action requise**:
```sql
CREATE VOLUME IF NOT EXISTS main.default.data_ref;
-- Puis uploader les CSV via l'UI Databricks
```

---

### 6. Postgres JDBC Supprimé

**Problème**: JDBC writes vers Postgres externe peuvent ne pas fonctionner sur serverless

**Avant**:
```python
df.write.jdbc(PG_URL, "gold.fait_mesures", mode="append", properties=PG_PROPS)
```

**Après**:
```python
# Supprimé - utiliser à la place:
# - Databricks SQL Dashboards (natif)
# - Delta Sharing (partage externe)
# - Lakehouse for BI (connexion directe)
```

---

### 7. Vérification d'Existence de Fichiers

**Problème**: `os.path.exists()` ne fonctionne pas avec les tables UC

**Avant**:
```python
if not os.path.exists(f"{ETAT_COURANT_PATH}/_delta_log"):  # ❌ Ne marche pas avec UC
```

**Après**:
```python
if not spark.catalog.tableExists(ETAT_COURANT_TABLE):  # ✅ API Spark
```

---

## Mapping Complet des Changements

| Concept | Docker (Avant) | Databricks (Après) |
|---------|----------------|-------------------|
| **Lancement** | `spark-submit /opt/spark-apps/bronze_ingest.py` | `%run ./spark/apps/bronze_ingest.py` |
| **Lakehouse Bronze** | `/opt/lakehouse/bronze/` (path) | `bronze_sensor_data` (table UC) |
| **Lakehouse Silver** | `/opt/lakehouse/silver/` (path) | `silver_sensor_data` (table UC) |
| **Lakehouse Gold** | `/opt/lakehouse/gold/fait_mesures` (path) | `gold_fait_mesures` (table UC) |
| **Checkpoints** | `/opt/checkpoints/bronze` (path) | `/Volumes/main/default/checkpoints/bronze` (volume) |
| **CSV Référentiels** | `/opt/data/capteurs.csv` (mount) | `/Volumes/main/default/data_ref/capteurs.csv` (volume) |
| **Kafka** | `kafka:9092` (Docker network) | Configuration externe (Event Hub, Confluent, etc.) |
| **Postgres** | `jdbc:postgresql://postgres:5432/warehouse` | Supprimé (utiliser Lakehouse BI) |
| **Streaming Trigger** | `processingTime="10 seconds"` (continu) | `availableNow=True` (batch incrémental) |
| **SparkContext** | `spark.sparkContext.setLogLevel()` | Supprimé (géré au niveau cluster) |
| **Lecture streaming** | `spark.readStream.format("delta").load(path)` | `spark.readStream.table(table_name)` |
| **Écriture streaming** | `.start(path)` | `.toTable(table_name)` |
| **Existence table** | `os.path.exists(f"{path}/_delta_log")` | `spark.catalog.tableExists(table)` |

---

## Configuration Requise Avant Exécution

### 1. Créer les Volumes

```sql
-- Volume pour les CSV référentiels
CREATE VOLUME IF NOT EXISTS main.default.data_ref;

-- Volume pour les checkpoints streaming
CREATE VOLUME IF NOT EXISTS main.default.checkpoints;
```

### 2. Uploader les CSV

Uploader dans `/Volumes/main/default/data_ref/`:
- `capteurs.csv`
- `machines.csv`
- `sites.csv`
- `seuils_machine.csv`

### 3. Configurer Kafka (Optionnel)

Si vous voulez tester avec un vrai flux Kafka:
```python
import os
os.environ["KAFKA_BOOTSTRAP"] = "your-kafka-server:9092"
os.environ["KAFKA_TOPIC"] = "sensors-data"
```

---

## Ordre d'Exécution des Jobs

1. **Smoke test** (vérifier infrastructure):
   ```python
   %run ./spark/apps/_infra_smoke.py
   ```

2. **Bronze** (ingestion Kafka → table):
   ```python
   %run ./spark/apps/bronze_ingest.py
   ```
   ⚠️ Nécessite Kafka configuré

3. **Silver** (nettoyage + validation):
   ```python
   %run ./spark/apps/silver_clean.py
   ```
   ⚠️ Nécessite CSV dans volume + table Bronze existante

4. **Gold** (star schema):
   ```python
   %run ./spark/apps/gold_pipeline.py
   ```
   ⚠️ Nécessite CSV dans volume + table Silver existante

---

## Tester Sans Kafka

Pour tester la logique sans un vrai flux Kafka:

```python
# 1. Créer une table Bronze de test
test_data = [
    ('evt1', '{"event_id": "evt1", "capteur_id": "CAPT001", "machine_id": "M001", "site_id": "S001", "type_mesure": "temperature", "valeur": 25.5, "unite": "C", "qualite_signal": 95.0, "batterie_pourcentage": 85, "timestamp": "2024-01-01T10:00:00"}', 0, 0, "2024-01-01T10:00:00", "2024-01-01T10:00:01", "2024-01-01"),
]
columns = ["kafka_key", "payload_json", "kafka_partition", "kafka_offset", "kafka_timestamp", "ts_ingestion", "date_ingestion"]
bronze_test = spark.createDataFrame(test_data, columns)
bronze_test.write.format("delta").mode("overwrite").saveAsTable("bronze_sensor_data")

# 2. Exécuter Silver et Gold normalement
%run ./spark/apps/silver_clean.py
%run ./spark/apps/gold_pipeline.py
```

---

## Limitations Serverless vs Cluster Classique

| Feature | Serverless | Cluster Classique |
|---------|------------|-------------------|
| **Streaming continu** | ❌ Non (availableNow uniquement) | ✅ Oui (processingTime) |
| **SparkContext** | ❌ Non accessible | ✅ Accessible |
| **DBFS root** | ❌ Désactivé (utiliser volumes) | ⚠️ Dépend de la config |
| **JDBC writes** | ⚠️ Limité | ✅ Complet |
| **Delta Lake** | ✅ Natif | ✅ Natif |
| **Unity Catalog** | ✅ Intégré | ✅ Intégré |
| **Auto-scaling** | ✅ Automatique | ⚠️ Manuel |
| **Coût** | 💰 À la demande | 💰💰 Toujours actif |

**Recommandation**: Serverless est idéal pour des **jobs orchestrés** (Workflows, scheduled). Pour du **streaming continu 24/7**, préférez un cluster classique.

---

## État du Code

✅ **Tous les fichiers sont adaptés et fonctionnels sur Databricks serverless compute**

Les seules erreurs attendues:
- Kafka connection timeout (si Kafka non configuré) → **Normal**, utiliser des données de test
- Volume not found (si volumes non créés) → **Action requise**, créer les volumes UC
- CSV not found (si CSV non uploadés) → **Action requise**, uploader les fichiers

---

## Documentation

Consultez `README.md` pour:
- Instructions complètes de configuration
- Exemples de test sans Kafka
- Guide de dépannage
- Architecture détaillée du pipeline

---

## Support Databricks

- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Delta Lake Streaming](https://docs.databricks.com/en/structured-streaming/delta-lake.html)
- [Serverless Compute](https://docs.databricks.com/en/serverless-compute/index.html)
