"""
Génère le référentiel statique (CSV) à partir de la topologie.

    python generator/build_referentiel.py

Écrit :
    data/capteurs.csv
    data/machines.csv
    data/sites.csv

Ces fichiers sont COHÉRENTS par construction avec les événements émis par
generator.py (même source : topology.py). À committer dans le dépôt : le
référentiel ne change pas pendant l'exécution.
"""

from __future__ import annotations

import csv
import os

from topology import CAPTEURS, MACHINES, SEUILS_MACHINE, SITES

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")


def write_csv(path: str, header: list[str], rows: list[tuple]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  écrit {path}  ({len(rows)} lignes)")


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)

    # capteurs.csv (schéma imposé par l'énoncé)
    write_csv(
        os.path.join(DATA_DIR, "capteurs.csv"),
        ["capteur_id", "type_mesure", "plage_nominale_min", "plage_nominale_max",
         "fabricant", "date_installation", "precision_capteur"],
        [
            (c.capteur_id, c.type_mesure, c.plage_min, c.plage_max,
             c.fabricant, c.date_installation, c.precision)
            for c in CAPTEURS
        ],
    )

    # machines.csv
    write_csv(
        os.path.join(DATA_DIR, "machines.csv"),
        ["machine_id", "type_machine", "ligne_production", "criticite",
         "date_mise_service", "capacite_nominale", "responsable_technique", "site_id"],
        [m for m in MACHINES],
    )

    # sites.csv
    write_csv(
        os.path.join(DATA_DIR, "sites.csv"),
        ["site_id", "nom", "region", "capacite_site", "fuseau_horaire", "responsable_site"],
        [s for s in SITES],
    )

    # seuils_machine.csv (surcharge des seuils d'anomalie, par machine)
    write_csv(
        os.path.join(DATA_DIR, "seuils_machine.csv"),
        ["machine_id", "type_mesure", "seuil_min", "seuil_max"],
        [s for s in SEUILS_MACHINE],
    )

    print("Référentiel généré avec succès.")


if __name__ == "__main__":
    main()
