import psycopg2
import psycopg2.extras
import sys


USERNAME="postgres"
PASS="1234"
try:
   conn = psycopg2.connect(host="localhost", dbname="db_projet", user=USERNAME, password=PASS)
except Exception as e :
   exit("Connexion impossible à la base de données: " + str(e))

print('Connecté à la base de données')
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


# Création des tables

# Table des regions:

cur.execute("CREATE TABLE IF NOT EXISTS regions (id_region int PRIMARY KEY, nom_region VARCHAR(26) NOT NULL);")