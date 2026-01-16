#!/bin/bash
set -e

# 1. On se connecte avec l'utilisateur par défaut (airflow) pour créer "user" et "taxidb"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER "user" WITH PASSWORD 'password';
    CREATE DATABASE taxidb;
    GRANT ALL PRIVILEGES ON DATABASE taxidb TO "user";
EOSQL

# 2. On se connecte spécifiquement à la base "taxidb" pour donner les droits sur le schéma public
# C'est cette partie qui faisait planter le script SQL classique
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "taxidb" <<-EOSQL
    GRANT ALL ON SCHEMA public TO "user";
EOSQL