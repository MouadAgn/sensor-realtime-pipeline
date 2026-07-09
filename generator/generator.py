"""
Générateur d'événements capteurs -> Kafka.

⚠️  STAND-IN : l'énoncé précise que le générateur EXACT sera fourni. Ce script
    est un substitut fidèle au schéma imposé, à remplacer par le générateur
    officiel le moment venu (le point de contact reste le topic `sensors-data`
    et le schéma JSON ci-dessous).

Schéma de l'événement émis :
{
  "event_id": "evt-9f3a1c2d",
  "capteur_id": "cpt-042",
  "machine_id": "m-07",
  "site_id": "site-lyon",
  "type_mesure": "temperature",
  "valeur": 78.4,
  "unite": "celsius",
  "qualite_signal": 0.97,
  "batterie_pourcentage": 63,
  "timestamp": "2026-07-08T10:15:32.104Z"
}

Cohérence : n'émet que des identifiants présents dans le référentiel (topology.py).

Usage (dans le conteneur, valeurs par défaut via variables d'env) :
    python generator.py
Usage (depuis l'hôte) :
    python generator.py --bootstrap-servers localhost:29092 --anomaly-rate 0.05
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from topology import CAPTEURS

_running = True


def _stop(*_):
    global _running
    _running = False
    print("\nArrêt du générateur…", flush=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Générateur de mesures capteurs -> Kafka")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
                   help="Kafka bootstrap servers (défaut: kafka:9092 en conteneur, localhost:29092 depuis l'hôte)")
    p.add_argument("--topic", default=os.getenv("TOPIC", "sensors-data"))
    p.add_argument("--anomaly-rate", type=float, default=float(os.getenv("ANOMALY_RATE", "0.05")),
                   help="Probabilité qu'une mesure soit hors plage nominale (défaut 0.05)")
    p.add_argument("--min-interval", type=float, default=float(os.getenv("MIN_INTERVAL", "1")),
                   help="Intervalle min entre 2 mesures d'un même capteur (s)")
    p.add_argument("--max-interval", type=float, default=float(os.getenv("MAX_INTERVAL", "3")),
                   help="Intervalle max entre 2 mesures d'un même capteur (s)")
    p.add_argument("--sensors", type=int, default=int(os.getenv("SENSORS", "0")),
                   help="Limiter au N premiers capteurs (0 = tous)")
    p.add_argument("--duration", type=float, default=float(os.getenv("DURATION", "0")),
                   help="Durée max en secondes (0 = infini)")
    p.add_argument("--seed", type=int, default=int(os.getenv("SEED", "0")))
    return p.parse_args()


def connect(bootstrap: str, retries: int = 30, delay: float = 2.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=50,
            )
            print(f"Connecté à Kafka ({bootstrap}).", flush=True)
            return producer
        except NoBrokersAvailable:
            print(f"[{attempt}/{retries}] Kafka indisponible ({bootstrap}), nouvelle tentative dans {delay}s…",
                  flush=True)
            time.sleep(delay)
    print(f"Impossible de joindre Kafka ({bootstrap}) après {retries} tentatives.", file=sys.stderr)
    sys.exit(1)


def make_value(capteur, rng: random.Random, anomaly_rate: float) -> float:
    """Valeur dans la plage nominale, ou hors plage (anomalie) avec proba `anomaly_rate`."""
    span = capteur.plage_max - capteur.plage_min
    if rng.random() < anomaly_rate:
        # Dépassement franc au-delà de la borne haute (ex. température > 85°C)
        valeur = capteur.plage_max + rng.uniform(0.05, 0.35) * span
    else:
        centre = capteur.plage_min + 0.5 * span
        valeur = rng.gauss(centre, span * 0.15)
        valeur = max(capteur.plage_min, min(capteur.plage_max, valeur))
    return round(valeur, 2)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def main() -> None:
    args = parse_args()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    rng = random.Random(args.seed or None)
    capteurs = CAPTEURS[: args.sensors] if args.sensors > 0 else CAPTEURS

    producer = connect(args.bootstrap_servers)

    start = time.time()
    for c in capteurs:
        c.next_emit = start + rng.uniform(0, args.max_interval)

    print(f"Émission de {len(capteurs)} capteurs -> topic '{args.topic}' "
          f"(anomalies={args.anomaly_rate:.0%}, intervalle {args.min_interval}-{args.max_interval}s).",
          flush=True)

    sent = 0
    last_log = start
    while _running:
        now = time.time()
        if args.duration and (now - start) >= args.duration:
            break

        for c in capteurs:
            if now < c.next_emit:
                continue
            # batterie décroît lentement, plancher 5%
            c.batterie = max(5.0, c.batterie - rng.uniform(0.0, 0.05))
            event = {
                "event_id": f"evt-{uuid.uuid4().hex[:8]}",
                "capteur_id": c.capteur_id,
                "machine_id": c.machine_id,
                "site_id": c.site_id,
                "type_mesure": c.type_mesure,
                "valeur": make_value(c, rng, args.anomaly_rate),
                "unite": c.unite,
                "qualite_signal": round(min(1.0, max(0.4, rng.gauss(0.95, 0.05))), 2),
                "batterie_pourcentage": int(round(c.batterie)),
                "timestamp": now_iso(),
            }
            # clé = capteur_id -> ordre garanti par capteur au sein d'une partition
            producer.send(args.topic, key=c.capteur_id, value=event)
            sent += 1
            c.next_emit = now + rng.uniform(args.min_interval, args.max_interval)

        if now - last_log >= 5:
            print(f"  … {sent} événements envoyés (débit ~{sent / (now - start):.1f} msg/s)", flush=True)
            last_log = now

        time.sleep(0.1)

    producer.flush()
    producer.close()
    print(f"Terminé. {sent} événements envoyés au total.", flush=True)


if __name__ == "__main__":
    main()
