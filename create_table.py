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

create_region = """
CREATE TABLE region (
    id_region INT PRIMARY KEY,
    nom_region VARCHAR(26)
);
"""

create_departement = """
CREATE TABLE departement (
    id_departement VARCHAR(3) PRIMARY KEY,
    nom_departement VARCHAR(23),
    id_region INT REFERENCES region(id_region)
);
"""

create_commune = """
CREATE TABLE commune (
    id_commune CHAR(5) PRIMARY KEY,
    nom_commune VARCHAR(45),
    superf FLOAT CONSTRAINT sup_pos CHECK ( superf > 0 ),
    id_departement varchar(3) REFERENCES departement(id_departement)
);
"""

create_chef_lieu_departement = """
CREATE TABLE chef_lieu_departement (
    id_departement VARCHAR(3) PRIMARY KEY,
    id_chef_lieu CHAR(5) REFERENCES commune(id_commune),
    FOREIGN KEY (id_departement) REFERENCES departement(id_departement)
);
"""

create_chef_lieu_region = """
CREATE TABLE chef_lieu_region (
    id_region INT PRIMARY KEY,
    id_chef_lieu CHAR(5) REFERENCES commune(id_commune),
    FOREIGN KEY (id_region) REFERENCES region(id_region)
);
"""

create_statistique_mariage = """
CREATE TABLE statistique_mariage (
    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    typmar VARCHAR(3),
    regdep INT,
    sexe CHAR(1) CONSTRAINT check_sexe CHECK ( sexe IN ('H', 'F') ),
    etamat INT,
    grage VARCHAR(5),
    lnepoux VARCHAR(6),
    natepoux VARCHAR(6),
    mmar INT,
    nbmaries INT,
    id_departement VARCHAR(3),
    id_region INT,
    FOREIGN KEY (id_departement) REFERENCES departement(id_departement),
    FOREIGN KEY (id_region) REFERENCES region(id_region)
);
"""

create_statistiques_population = """
CREATE TABLE statistiques_population (
    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    codgeo VARCHAR(5),
    annee INT,
    type_statistique VARCHAR(20),
    valeur NUMERIC,
    FOREIGN KEY (codgeo) REFERENCES commune(id_commune)
);
"""

cur.execute(create_region)
cur.execute(create_departement)
cur.execute(create_commune)
cur.execute(create_chef_lieu_departement)
cur.execute(create_chef_lieu_region)
cur.execute(create_statistique_mariage)
cur.execute(create_statistiques_population)

conn.commit()

print("Tables créées avec succès")
