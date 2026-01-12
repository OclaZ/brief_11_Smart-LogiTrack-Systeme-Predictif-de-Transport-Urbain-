-- Crée l'utilisateur et la base de données pour votre projet
CREATE USER "user" WITH PASSWORD 'password';
CREATE DATABASE taxidb;
GRANT ALL PRIVILEGES ON DATABASE taxidb TO "user";

-- (Optionnel) Si vous utilisez un user différent dans votre init_db.py (postgres:112001), 
-- adaptez les lignes ci-dessus ou votre code Python.