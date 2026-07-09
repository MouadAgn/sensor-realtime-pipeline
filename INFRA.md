# Contrat d'interface — Lot « Infra & ingestion »

> Ce document est **le contrat** entre le socle Docker (Kafka, Spark, Postgres,
> Metabase, générateur) et les jobs Spark Bronze/Silver/Gold des collègues.
> Tant que vous respectez les noms/chemins ci-dessous, vos jobs se branchent sans friction.

## 1. Démarrer le socle

```bash
docker compose up -d --build          # infra (kafka, spark, postgres, metabase, kafka-ui)
docker compose --profile generator up -d generator   # flux d'événements
```

Ordre de démarrage géré automatiquement (healthchecks + `depends_on`) :
`kafka` (healthy) → `kafka-init` crée le topic → `kafka-ui`, `spark`, `postgres`, `metabase`.
Le `generator` attend que le topic existe.

## 2. Ce que l'infra fournit

### Kafka
| Depuis…             | Bootstrap servers   |
|---------------------|---------------------|
| un conteneur (Spark, generator) | `kafka:9092`  |
| l'hôte (laptop)     | `localhost:29092`   |

- **Topic : `sensors-data`** — 3 partitions, réplication 1, clé = `capteur_id`.
- Auto-création de topics **désactivée** : le topic est créé par `kafka-init`.

### Schéma de l'événement (valeur JSON)
```json
{
  "event_id": "evt-9f3a1c2d", "capteur_id": "cpt-042", "machine_id": "m-07",
  "site_id": "site-lyon", "type_mesure": "temperature", "valeur": 78.4,
  "unite": "celsius", "qualite_signal": 0.97, "batterie_pourcentage": 63,
  "timestamp": "2026-07-08T10:15:32.104Z"
}
```

### Référentiel statique (CSV) — cohérent par construction avec le flux
Monté en lecture seule dans Spark sous `/opt/data/` :
- `capteurs.csv` : `capteur_id, type_mesure, plage_nominale_min, plage_nominale_max, fabricant, date_installation, precision_capteur`
- `machines.csv` : `machine_id, type_machine, ligne_production, criticite, date_mise_service, capacite_nominale, responsable_technique, site_id`
- `sites.csv` : `site_id, nom, region, capacite_site, fuseau_horaire, responsable_site`
- `seuils_machine.csv` : `machine_id, type_mesure, seuil_min, seuil_max` — **seuils d'anomalie PAR MACHINE** (surcharge la plage nominale du capteur, voir §6)

3 sites · 9 machines · 50 capteurs. **Tout identifiant du flux existe dans le référentiel**
(généré depuis `generator/topology.py`). Regénérer : `make referentiel`.

**Convention de nommage (toute l'équipe)** : `snake_case`, **sans accents**, termes métier
en **français** (comme l'énoncé). Colonnes dérivées Silver/Gold en français :
`est_anomalie`, `type_anomalie`, `seuil_min`, `seuil_max`, `statut`… (pas `is_anomaly`).

### Spark (avec Delta + Kafka + JDBC Postgres pré-installés)
- Cluster : `spark://spark-master:7077`. Soumission :
  ```bash
  docker compose exec spark-master spark-submit /opt/spark-apps/<job>.py
  ```
- Vos jobs vont dans `spark/apps/` (monté sous `/opt/spark-apps`).
- Delta est déjà configuré (extensions + catalog) : `SparkSession.builder…getOrCreate()` suffit,
  **pas de `--packages`** (jars bakés → fonctionne hors-ligne pour la démo).
- Chemins conventionnels (à vous de créer les tables) :
  - Delta : `/opt/lakehouse/bronze`, `/opt/lakehouse/silver`, `/opt/lakehouse/gold/...`
  - Checkpoints : `/opt/checkpoints/<nom_du_job>`

### Postgres de service (serving layer lu par Metabase)
Delta = source de vérité. Le **job Gold recopie le star-schema** ici via JDBC :
```
url  = jdbc:postgresql://postgres:5432/warehouse
user = warehouse   password = warehouse   schéma = gold
```

## 3. Interfaces web (preuves)
| Service        | URL                     |
|----------------|-------------------------|
| Kafka UI       | http://localhost:8080   |
| Spark master   | http://localhost:8081   |
| Spark worker   | http://localhost:8082   |
| Spark job (UI) | http://localhost:4040   |
| Metabase       | http://localhost:3000   |

## 4. Commandes utiles
```bash
make topics         # décrit le topic
make kafka-console  # voir les messages en direct
make psql           # shell Postgres
make logs           # logs de tous les services
docker compose logs -f generator   # débit du générateur
```

## 6. Détection d'anomalie — contrat Silver

Deux seuils coexistent (l'énoncé distingue « température > 85°C » du « seuil vibration **par machine** ») :

1. **Plage nominale par capteur** — `capteurs.csv` (`plage_nominale_min/max`). Couvre température & pression.
2. **Seuil par machine** — `seuils_machine.csv` (`seuil_min/max` par `machine_id`+`type_mesure`). Impose le seuil vibration exigé par l'énoncé.

**Règle de précédence** (le seuil machine SURCHARGE la plage nominale) :

```
seuil_min_effectif = COALESCE(seuils_machine.seuil_min, capteurs.plage_nominale_min)
seuil_max_effectif = COALESCE(seuils_machine.seuil_max, capteurs.plage_nominale_max)
est_anomalie       = (valeur < seuil_min_effectif) OR (valeur > seuil_max_effectif)
```

Chaîne de jointures Silver :
`flux → JOIN capteurs.csv ON capteur_id → LEFT JOIN seuils_machine.csv ON (machine_id, type_mesure)`.
Les événements hors seuil sont **marqués** (`est_anomalie = true`), **pas supprimés**.

## 7. Ce qui reste aux autres lots
- **Bronze** : lire `sensors-data` en streaming → écrire Delta `/opt/lakehouse/bronze` (append, sans perte).
- **Silver** : nettoyage + déduplication (`event_id`) + marquage anomalies selon §6.
- **Gold** : star-schema Delta + `MERGE INTO` (état courant capteur) + agrégation fenêtrée ;
  recopie vers Postgres `gold.*` pour Metabase.
