# Raccourcis reproductibles. `make help` pour la liste.
.PHONY: help build up down restart logs ps topics referentiel gen gen-stop \
        kafka-console psql clean

help:
	@echo "Cibles disponibles :"
	@echo "  make build        - build des images (spark, generator)"
	@echo "  make up           - démarre le socle infra (kafka, spark, postgres, metabase, kafka-ui)"
	@echo "  make down         - arrête tout (conserve les volumes)"
	@echo "  make restart      - down puis up"
	@echo "  make ps           - état des conteneurs"
	@echo "  make logs         - logs suivis de tous les services"
	@echo "  make topics       - liste/décrit le topic Kafka"
	@echo "  make referentiel  - (re)génère les CSV data/*.csv depuis la topologie"
	@echo "  make gen          - lance le générateur d'événements"
	@echo "  make gen-stop     - arrête le générateur"
	@echo "  make kafka-console- consomme le topic en direct (Ctrl+C pour quitter)"
	@echo "  make psql         - shell psql sur le Postgres de service"
	@echo "  make clean        - down + suppression des volumes ET des données locales (⚠)"

build:
	docker compose build

up:
	docker compose up -d --build

down:
	docker compose down

restart: down up

ps:
	docker compose ps

logs:
	docker compose logs -f

topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic sensors-data

referentiel:
	python generator/build_referentiel.py

gen:
	docker compose --profile generator up -d generator
	@echo "Générateur démarré. Logs : docker compose logs -f generator"

gen-stop:
	docker compose stop generator

kafka-console:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka:9092 --topic sensors-data --from-beginning

psql:
	docker compose exec postgres psql -U warehouse -d warehouse

clean:
	docker compose --profile generator down -v
	rm -rf lakehouse/* checkpoints/*
