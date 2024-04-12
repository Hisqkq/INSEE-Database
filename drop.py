import psycopg2
import psycopg2.extras


USERNAME="postgres"
PASS="1234"
try:
   conn = psycopg2.connect(host="localhost", dbname="db_projet", user=USERNAME, password=PASS)
except Exception as e :
   exit("Connexion impossible à la base de données: " + str(e))

print('Connecté à la base de données')
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Vos requêtes SQL de suppression de table
drop_statistiques_population = """
DROP TABLE statistiques_population;
"""

drop_statistique_mariage = """
DROP TABLE statistique_mariage;
"""

drop_chef_lieu_region = """
DROP TABLE chef_lieu_region;
"""

drop_chef_lieu_departement = """
DROP TABLE chef_lieu_departement;
"""

drop_commune = """
DROP TABLE commune;
"""

drop_departement = """
DROP TABLE departement;
"""

drop_region = """
DROP TABLE region;
"""

# Exécution des requêtes SQL
cur.execute(drop_statistiques_population)
cur.execute(drop_statistique_mariage)
cur.execute(drop_chef_lieu_region)
cur.execute(drop_chef_lieu_departement)
cur.execute(drop_commune)
cur.execute(drop_departement)
cur.execute(drop_region)

# Validation des changements
conn.commit()

print("Tables supprimées avec succès")
