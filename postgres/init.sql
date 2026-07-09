-- Postgres de SERVICE (serving layer) lu par Metabase.
-- Delta reste la source de vérité ; le job Gold recopie le star-schema ici via JDBC.
--
-- Le job Gold écrit dans le schéma `gold` :
--   jdbc:postgresql://postgres:5432/warehouse   user=warehouse  password=warehouse
--
-- Ce fichier est exécuté UNE fois, à la création du volume Postgres.

CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION warehouse;

-- Le job Gold crée/écrase ses tables (fait + dimensions + état courant) dans ce schéma.
-- On donne au user warehouse tous les droits par défaut sur les objets futurs.
ALTER DEFAULT PRIVILEGES IN SCHEMA gold GRANT ALL ON TABLES TO warehouse;
GRANT ALL ON SCHEMA gold TO warehouse;

-- Table témoin : prouve que la connexion Metabase -> Postgres fonctionne
-- même avant que le job Gold n'ait tourné. À supprimer si superflu.
CREATE TABLE IF NOT EXISTS gold._infra_healthcheck (
    check_id     serial PRIMARY KEY,
    message      text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);
INSERT INTO gold._infra_healthcheck (message)
VALUES ('serving layer OK — en attente des tables Gold du job Spark');
