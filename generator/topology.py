"""
Topologie du site industriel simulé.

SOURCE UNIQUE DE VÉRITÉ pour la cohérence des identifiants.
- `build_referentiel.py` génère les CSV (data/capteurs.csv, machines.csv, sites.csv)
  à partir de cette topologie.
- `generator.py` émet des événements Kafka en n'utilisant QUE ces identifiants.

=> Tout capteur_id / machine_id / site_id présent dans le flux existe dans le
   référentiel, et chaque capteur reporte toujours la même machine et le même site.

Aucune dépendance externe : stdlib uniquement (importable partout).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

SEED = 42

# ---------------------------------------------------------------------------
# Sites
# ---------------------------------------------------------------------------
# site_id, nom, region, capacite_site, fuseau_horaire, responsable_site
SITES = [
    ("site-lyon", "Lyon Usine 1", "Auvergne-Rhône-Alpes", 3000, "Europe/Paris", "M. Bernard"),
    ("site-nantes", "Nantes Usine 2", "Pays de la Loire", 1800, "Europe/Paris", "L. Petit"),
    ("site-paris", "Paris Usine 3", "Île-de-France", 2500, "Europe/Paris", "C. Durand"),
]

# ---------------------------------------------------------------------------
# Machines
# ---------------------------------------------------------------------------
# machine_id, type_machine, ligne_production, criticite,
# date_mise_service, capacite_nominale, responsable_technique, site_id
MACHINES = [
    ("m-01", "presse hydraulique", "ligne-A", "haute", "2021-06-01", 500, "J. Dupont", "site-lyon"),
    ("m-07", "four industriel", "ligne-A", "haute", "2020-02-10", 800, "A. Moreau", "site-lyon"),
    ("m-03", "convoyeur", "ligne-B", "moyenne", "2022-09-15", 1200, "S. Martin", "site-lyon"),
    ("m-12", "convoyeur", "ligne-B", "moyenne", "2020-09-15", 1200, "S. Martin", "site-nantes"),
    ("m-05", "compresseur", "ligne-C", "haute", "2019-11-20", 600, "K. Nguyen", "site-nantes"),
    ("m-08", "pompe", "ligne-C", "basse", "2023-01-05", 300, "F. Leroy", "site-nantes"),
    ("m-09", "tour CNC", "ligne-D", "haute", "2021-03-30", 450, "P. Girard", "site-paris"),
    ("m-10", "robot soudure", "ligne-D", "moyenne", "2022-07-12", 700, "R. Simon", "site-paris"),
    ("m-11", "ventilateur", "ligne-E", "basse", "2020-05-18", 250, "N. Faure", "site-paris"),
]

# ---------------------------------------------------------------------------
# Types de mesure : plage nominale, unité, précision
# ---------------------------------------------------------------------------
TYPES_MESURE = {
    #                 min   max   unite       precision
    "temperature": (10.0, 85.0, "celsius", 0.5),
    "vibration":   (0.0, 12.0, "mm_s",    0.1),
    "pression":    (1.0, 8.0, "bar",     0.2),
}

FABRICANTS = ["Siemens", "Bosch", "ABB", "Schneider", "Honeywell"]

NB_CAPTEURS = 50  # "plusieurs dizaines de capteurs"


@dataclass
class Capteur:
    capteur_id: str
    machine_id: str
    site_id: str
    type_mesure: str
    plage_min: float
    plage_max: float
    unite: str
    precision: float
    fabricant: str
    date_installation: str
    # état runtime (utilisé uniquement par le générateur, pas dans le référentiel)
    batterie: float = 100.0
    next_emit: float = 0.0


# ---------------------------------------------------------------------------
# Seuils d'alerte PAR MACHINE (surcharge la plage nominale du capteur).
# ---------------------------------------------------------------------------
# L'énoncé impose un seuil de vibration "défini par machine". On ne peut donc
# pas se contenter de la plage nominale (par capteur) de capteurs.csv.
# => Ce référentiel de surcharge est consommé par le Silver avec la règle :
#      seuil_effectif = COALESCE(seuil_machine, plage_nominale_du_capteur)
# Valeur modulée par la criticité de la machine (plus critique => tolérance basse).
SEUIL_VIBRATION_PAR_CRITICITE = {"haute": 8.0, "moyenne": 10.0, "basse": 11.0}


def build_seuils_machine() -> list[tuple]:
    """Un seuil vibration par machine (min, max), dérivé de la criticité."""
    rows = []
    for machine_id, _type, _ligne, criticite, *_ in MACHINES:
        seuil_max = SEUIL_VIBRATION_PAR_CRITICITE[criticite]
        rows.append((machine_id, "vibration", 0.0, seuil_max))
    return rows


def build_capteurs() -> list[Capteur]:
    """Construit la liste déterministe des capteurs répartis sur les machines."""
    rng = random.Random(SEED)
    types = list(TYPES_MESURE.keys())
    machine_ids = [m[0] for m in MACHINES]
    machine_site = {m[0]: m[7] for m in MACHINES}

    capteurs: list[Capteur] = []
    for i in range(1, NB_CAPTEURS + 1):
        cid = f"cpt-{i:03d}"
        machine_id = machine_ids[(i - 1) % len(machine_ids)]
        type_mesure = types[(i - 1) % len(types)]
        pmin, pmax, unite, precision = TYPES_MESURE[type_mesure]
        fabricant = FABRICANTS[(i - 1) % len(FABRICANTS)]
        annee = rng.choice(["2020", "2021", "2022", "2023"])
        mois = rng.randint(1, 12)
        jour = rng.randint(1, 28)
        date_install = f"{annee}-{mois:02d}-{jour:02d}"
        capteurs.append(
            Capteur(
                capteur_id=cid,
                machine_id=machine_id,
                site_id=machine_site[machine_id],
                type_mesure=type_mesure,
                plage_min=pmin,
                plage_max=pmax,
                unite=unite,
                precision=precision,
                fabricant=fabricant,
                date_installation=date_install,
                batterie=rng.uniform(55.0, 100.0),
            )
        )
    return capteurs


# Instances partagées (déterministes grâce au SEED)
CAPTEURS = build_capteurs()
SEUILS_MACHINE = build_seuils_machine()
