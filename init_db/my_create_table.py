import psycopg2
import psycopg2.extras
from db import connect

conn = connect()
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

create_table_queries = [
    """
    CREATE TABLE region (
        id_region INT PRIMARY KEY,
        nom_region VARCHAR(26)
    );
    """,
    """
    CREATE TABLE departement (
        id_departement VARCHAR(3) PRIMARY KEY,
        nom_departement VARCHAR(23),
        id_region INT REFERENCES region(id_region)
    );
    """,
    """
    CREATE TABLE commune (
        id_commune CHAR(5) PRIMARY KEY,
        nom_commune VARCHAR(45),
        superf FLOAT CONSTRAINT sup_pos CHECK ( superf > 0 ),
        id_departement varchar(3) REFERENCES departement(id_departement)
    );
    """,
    """
    CREATE TABLE chef_lieu_departement (
        id_departement VARCHAR(3) PRIMARY KEY,
        id_chef_lieu CHAR(5) REFERENCES commune(id_commune),
        FOREIGN KEY (id_departement) REFERENCES departement(id_departement)
    );
    """,
    """
    CREATE TABLE chef_lieu_region (
        id_region INT PRIMARY KEY,
        id_chef_lieu CHAR(5) REFERENCES commune(id_commune),
        FOREIGN KEY (id_region) REFERENCES region(id_region)
    );
    """,
    """
    CREATE TABLE statistiques_population (
        codgeo VARCHAR(5),
        annee INT,
        annee2 INT,
        type_statistique VARCHAR(50),
        valeur FLOAT CONSTRAINT val_pos CHECK ( valeur >= 0 ),
        FOREIGN KEY (codgeo) REFERENCES commune(id_commune),
        PRIMARY KEY (codgeo, annee, type_statistique)
    );
    """,
    """
    CREATE TABLE statistiques_mariages_age (
        annee INT NOT NULL DEFAULT 2021,
        typmar3 VARCHAR(5) CHECK (typmar3 IN ('FF', 'HH', 'HF-H', 'HF-F')),
        id_region INT NOT NULL REFERENCES region(id_region),
        id_departement VARCHAR(3) NULL REFERENCES departement(id_departement),
        grage VARCHAR(10),
        nb_mariages INT CHECK (nb_mariages >= 0),
        nb_mariages_premier INT CHECK (nb_mariages_premier >= 0),
        PRIMARY KEY (annee, typmar3, id_region, id_departement, grage)
    );
    """,
    """
    CREATE TABLE statistiques_mariages_etat_matrimonial (
        annee INT NOT NULL DEFAULT 2021,
        typmar VARCHAR(5) CHECK (typmar IN ('HF', 'HH-FF')),
        id_region INT NOT NULL REFERENCES region(id_region),
        id_departement VARCHAR(2) NULL REFERENCES departement(id_departement),
        sexe CHAR(1) CHECK (sexe IN ('H', 'F')),
        etamat CHAR(1) CHECK (etamat IN ('E', '1', '3', '4')),
        nbmaries INT CHECK (nbmaries >= 0),
        PRIMARY KEY (annee, typmar, id_region, id_departement, sexe, etamat)
    );
    """,
    """
    CREATE TABLE statistiques_mariages_mensuel (
        annee INT NOT NULL DEFAULT 2021,
        typmar2 VARCHAR(3) CHECK (typmar2 IN ('FF', 'HH', 'HF')),
        id_region INT NOT NULL REFERENCES region(id_region),
        id_departement VARCHAR(3) NULL REFERENCES departement(id_departement),
        mmar CHAR(2) CHECK (mmar IN ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', 'AN')),
        nbmar INT CHECK (nbmar >= 0),
        PRIMARY KEY (annee, typmar2, id_region, id_departement, mmar)
    );
    """,
    """
    CREATE TABLE statistiques_mariages_origine (
        annee INT NOT NULL DEFAULT 2021,
        typmar2 VARCHAR(3) CHECK (typmar2 IN ('FF', 'HH', 'HF')),
        id_region INT NOT NULL REFERENCES region(id_region),
        id_departement VARCHAR(3) NULL REFERENCES departement(id_departement),
        code VARCHAR(7) CHECK (code IN ('FR_FR', 'FR_ETR', 'ETR_ETR')),
        nb_mariages_nationalite INT CHECK (nb_mariages_nationalite >= 0),
        nb_mariages_pays_naissance INT CHECK (nb_mariages_pays_naissance >= 0),
        PRIMARY KEY (annee, typmar2, id_region, id_departement, code)
    );
    """
]

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
    conn.commit()
    print("Tables créées avec succès")
