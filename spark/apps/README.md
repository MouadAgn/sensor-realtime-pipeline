# spark/apps — Pipeline Sensor Data (Databricks)

Pipeline de traitement de données de capteurs en temps réel avec architecture Bronze/Silver/Gold.

**⚠️ ADAPTÉ POUR DATABRICKS SERVERLESS COMPUTE**

Ce code a été adapté depuis une architecture Docker vers Databricks. Les principales différences:
- Tables Unity Catalog au lieu de chemins fichiers
- Volumes UC pour les référentiels CSV et checkpoints
- Postgres JDBC supprimé (utiliser Data Lakehouse ou Delta Sharing pour BI)
- **Streaming trigger**: `availableNow=True` au lieu de `processingTime` (limitation serverless)

---

## Architecture du Pipeline

```
Kafka Stream → BRONZE → SILVER → GOLD
                 ↓        ↓        ↓
              (brut)  (nettoyé) (étoile)
```

### 1. Bronze Layer (`bronze_ingest.py`)
**Ingestion brute depuis Kafka**
- Capture tout le flux sans transformation
- Permet de rejouer le Silver sans recommencer Kafka
- **Table**: `bronze_sensor_data` (Unity Catalog)

### 2. Silver Layer (`silver_clean.py`)
**Nettoyage, validation, détection d'anomalies**
- Parsing JSON des événements
- Validation des données
- Déduplication par `event_id`
- Détection d'anomalies selon seuils (capteur + machine)
- **Table**: `silver_sensor_data` (Unity Catalog)

### 3. Gold Layer (`gold_pipeline.py`)
**Modélisation en étoile pour analytics**
- **Dimensions** (batch): `gold_dim_capteur`, `gold_dim_machine`, `gold_dim_site`
- **Fait mesures** (streaming): `gold_fait_mesures`
- **Agrégations** (streaming): `gold_agg_machine_5min`
- **État courant** (MERGE): `gold_etat_courant_capteur`

---

## Configuration Requise

### 1. Créer les Volumes Unity Catalog

Les fichiers et checkpoints doivent être dans des volumes Unity Catalog:

```sql
-- Volume pour les référentiels CSV
CREATE VOLUME IF NOT EXISTS main.default.data_ref;

-- Volume pour les checkpoints streaming (DBFS root désactivé sur serverless)
CREATE VOLUME IF NOT EXISTS main.default.checkpoints;
```

**Uploadez les CSV** dans `/Volumes/main/default/data_ref/`:
- `capteurs.csv` — Liste des capteurs avec plages nominales
- `machines.csv` — Liste des machines
- `sites.csv` — Liste des sites
- `seuils_machine.csv` — Seuils spécifiques par machine

### 2. Configuration Kafka (Optionnel)

Pour les jobs de streaming, configurez les variables d'environnement:
```python
KAFKA_BOOTSTRAP = "your-kafka-server:9092"  # ou Event Hub, Confluent Cloud
KAFKA_TOPIC = "sensors-data"
```

**Pour les tests sans Kafka**: Les jobs streaming échoueront (comportement attendu). Utilisez plutôt des données statiques pour tester la logique.

---

## Exécution des Jobs

### Smoke Test (Vérifier l'infrastructure)
```python
%run ./spark/apps/_infra_smoke.py
```

### Bronze (Ingestion Kafka)
```python
%run ./spark/apps/bronze_ingest.py
```
⚠️ Nécessite Kafka configuré et accessible

### Silver (Nettoyage)
```python
%run ./spark/apps/silver_clean.py
```
⚠️ Nécessite les CSV dans le volume et la table Bronze existante

### Gold (Star Schema)
```python
%run ./spark/apps/gold_pipeline.py
```
⚠️ Nécessite les CSV dans le volume et la table Silver existante

---

## Différences Docker → Databricks

| Aspect | Docker (Original) | Databricks (Adapté) |
|--------|-------------------|---------------------|
| **Lakehouse paths** | `/opt/lakehouse/bronze/` | Tables Unity Catalog |
| **Checkpoints** | `/opt/checkpoints/` | `/Volumes/main/default/checkpoints/` (volume UC) |
| **CSV référentiels** | `/opt/data/*.csv` | `/Volumes/main/default/data_ref/*.csv` |
| **Kafka** | `kafka:9092` (Docker network) | Configuration externe requise |
| **Postgres serving** | JDBC direct | Supprimé (utiliser Data Lakehouse) |
| **SparkContext** | Accessible | Non disponible (serverless) |
| **Streaming trigger** | `processingTime="10 seconds"` | `availableNow=True` (serverless limitation) |

### ⚠️ Limitations importantes: Serverless Compute

**1. Streaming Trigger**

Sur **Databricks serverless compute**, les triggers continus comme `processingTime` ne sont **pas supportés**. 

**Avant (Docker):**
```python
.trigger(processingTime="10 seconds")  # ❌ Ne marche pas sur serverless
```

**Après (Databricks serverless):**
```python
.trigger(availableNow=True)  # ✅ Traite toutes les données disponibles puis s'arrête
```

**2. DBFS Root**

DBFS root (`dbfs:/FileStore/`) est **désactivé** sur les workspaces avec Unity Catalog. Les checkpoints doivent être dans un **volume UC**.

**Avant (DBFS):**
```python
CHECKPOINT_DIR = "dbfs:/FileStore/sensor-pipeline/checkpoints"  # ❌ Bloqué
```

**Après (Volume UC):**
```python
CHECKPOINT_DIR = "/Volumes/main/default/checkpoints"  # ✅ Fonctionne
```

**Alternative pour streaming continu**: Utilisez un cluster classique (non-serverless) si vous avez besoin d'un vrai streaming avec micro-batches continues.

---

## Tables Unity Catalog Créées

Après exécution complète, vous aurez:

**Bronze:**
- `bronze_sensor_data` — Flux brut Kafka

**Silver:**
- `silver_sensor_data` — Données nettoyées et validées

**Gold:**
- `gold_dim_capteur` — Dimension capteurs
- `gold_dim_machine` — Dimension machines
- `gold_dim_site` — Dimension sites
- `gold_fait_mesures` — Faits: toutes les mesures valides
- `gold_agg_machine_5min` — Agrégations par machine (fenêtre 5 min)
- `gold_etat_courant_capteur` — État actuel de chaque capteur (MERGE INTO)

---

## Visualisation et BI

**Option 1: Databricks SQL Dashboards**
- Créez des requêtes SQL sur les tables `gold_*`
- Construisez des dashboards directement dans Databricks

**Option 2: Delta Sharing**
- Partagez les tables Gold avec des outils externes (Power BI, Tableau, etc.)

**Option 3: Databricks Lakehouse for BI**
- Connectez votre outil BI directement au lakehouse

---

## Dépannage

### Erreur: "Table bronze_sensor_data not found"
→ Le job Bronze doit s'exécuter en premier pour créer la table

### Erreur: "Path /Volumes/main/default/data_ref/capteurs.csv not found"
→ Créez le volume `data_ref` et uploadez les CSV référentiels

### Erreur: "DBFS_DISABLED - Access is denied on path"
→ Code déjà adapté pour utiliser des volumes UC (ne devrait plus se produire)

### Erreur: Kafka connection timeout
→ Configurez `KAFKA_BOOTSTRAP` avec un serveur Kafka accessible depuis Databricks

### Erreur: "INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED"
→ Code déjà adapté avec `trigger(availableNow=True)` (ne devrait plus se produire)

### SparkContext not available
→ Code déjà adapté (ne devrait plus se produire)

---

## Tests Sans Kafka

Pour tester la logique sans Kafka en production:

1. Créez une table Bronze statique avec des données de test:
```python
from pyspark.sql import functions as F

# Simuler des événements Kafka
test_data = [
    ('evt1', '{"event_id": "evt1", "capteur_id": "CAPT001", "machine_id": "M001", "site_id": "S001", "type_mesure": "temperature", "valeur": 25.5, "unite": "C", "qualite_signal": 95.0, "batterie_pourcentage": 85, "timestamp": "2024-01-01T10:00:00"}', 0, 0, "2024-01-01T10:00:00", "2024-01-01T10:00:01", "2024-01-01"),
    ('evt2', '{"event_id": "evt2", "capteur_id": "CAPT002", "machine_id": "M001", "site_id": "S001", "type_mesure": "pression", "valeur": 1013.0, "unite": "hPa", "qualite_signal": 98.0, "batterie_pourcentage": 90, "timestamp": "2024-01-01T10:01:00"}', 0, 1, "2024-01-01T10:01:00", "2024-01-01T10:01:01", "2024-01-01"),
]

columns = ["kafka_key", "payload_json", "kafka_partition", "kafka_offset", "kafka_timestamp", "ts_ingestion", "date_ingestion"]
bronze_test = spark.createDataFrame(test_data, columns)
bronze_test.write.format("delta").mode("overwrite").saveAsTable("bronze_sensor_data")
```

2. Exécutez Silver et Gold — ils liront depuis cette table Bronze statique

---

## Mode Streaming vs Batch

**Sur serverless avec `availableNow=True`:**
- Le job traite **toutes les données disponibles** dans la source
- Puis **s'arrête automatiquement** (pas de boucle infinie)
- Idéal pour des exécutions orchestrées (scheduler, workflow)
- Pour re-traiter les nouvelles données: relancer le job

**Pour un vrai streaming continu:**
- Utilisez un cluster classique (non-serverless)
- Remplacez `trigger(availableNow=True)` par `trigger(processingTime="10 seconds")`

---

## Support

Pour toute question sur l'adaptation Databricks, consultez la documentation:
- [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [Delta Lake Streaming](https://docs.databricks.com/en/structured-streaming/delta-lake.html)
- [Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html)
- [Structured Streaming on Serverless](https://docs.databricks.com/en/structured-streaming/index.html)
