import psycopg2
import pandas as pd
import io

USERNAME="postgres"
PASS="1234"

df_region = pd.read_csv('data/CDR/v_region_2023.csv', sep=',')

df_region_to_insert = df_region[['REG', 'LIBELLE']]

df_region_to_insert = df_region_to_insert.rename(columns={'REG': 'id_region', 'LIBELLE': 'nom_region'})
# ON ENLEVE LES REGIONS D'OUTRE MER
df_region_to_insert = df_region_to_insert[df_region_to_insert['id_region'] > 6]

try:
    conn = psycopg2.connect(host="localhost", dbname="db_projet", user=USERNAME, password=PASS)
except Exception as e :
    exit("Connexion impossible à la base de données: " + str(e))

print('Connecté à la base de données')
cur = conn.cursor()

buffer = io.StringIO()
df_region_to_insert.to_csv(buffer, index=False, header=False)
buffer.seek(0)

copy_query = """
COPY region (id_region, nom_region) FROM STDIN WITH (FORMAT CSV);
"""

cur.copy_expert(copy_query, buffer)

conn.commit()

print("Données insérées dans la table 'region' avec succès")

df_departement = pd.read_csv('data/CDR/v_departement_2023.csv', sep=',')

df_departement_to_insert = df_departement[['DEP', 'LIBELLE', 'REG']]

df_departement_to_insert = df_departement_to_insert.rename(columns={'DEP': 'id_departement', 'LIBELLE': 'nom_departement', 'REG': 'id_region'})
# On enleve les departements d'outre mer
dom_codes = ['971', '972', '973', '974', '976']

# Filtrer le DataFrame pour ne conserver que les lignes correspondant aux départements métropolitains
df_departement_to_insert = df_departement_to_insert[~df_departement_to_insert['id_departement'].isin(dom_codes)]

buffer = io.StringIO()
df_departement_to_insert.to_csv(buffer, index=False, header=False)
buffer.seek(0)

copy_query = """
COPY departement (id_departement, nom_departement, id_region) FROM STDIN WITH (FORMAT CSV);
"""

cur.copy_expert(copy_query, buffer)

conn.commit()

df_commune = pd.read_csv('data/CDR/v_commune_2023.csv', sep=',', dtype={'TYPECOM': str})
df_commune = df_commune[df_commune['TYPECOM'] == 'COM']
pop_data = pd.read_csv('data/statistiques/population/base-cc-serie-historique-2020.csv', sep=';', dtype={'CODGEO': str})
# On garde seuelement les lignes ou TYPECOM = 'COM'
superficie = pop_data[['CODGEO', 'SUPERF']]
df_commune = df_commune.merge(superficie, left_on='COM', right_on='CODGEO')
df_commune = df_commune[['COM', 'LIBELLE', 'SUPERF', 'DEP']]
df_commune = df_commune.rename(columns={'COM': 'id_commune', 'LIBELLE': 'nom_commune', 'SUPERF': 'superf', 'DEP': 'id_departement'})
# On enleve les communes d'outre mer
df_commune = df_commune[~df_commune['id_departement'].isin(dom_codes)]

buffer = io.StringIO()
df_commune.to_csv(buffer, index=False, header=False)
buffer.seek(0)

copy_query = """
COPY commune (id_commune, nom_commune, superf, id_departement) FROM STDIN WITH (FORMAT CSV);
"""

cur.copy_expert(copy_query, buffer)

conn.commit()


df_chef_lieu_departement = df_departement[['DEP', 'CHEFLIEU']]

df_chef_lieu_departement = df_chef_lieu_departement.rename(columns={'DEP': 'id_departement', 'CHEFLIEU': 'id_chef_lieu'})
# On enleve les departements d'outre mer
df_chef_lieu_departement = df_chef_lieu_departement[~df_chef_lieu_departement['id_departement'].isin(dom_codes)]

buffer = io.StringIO()
df_chef_lieu_departement.to_csv(buffer, index=False, header=False)
buffer.seek(0)

copy_query = """
COPY chef_lieu_departement (id_departement, id_chef_lieu) FROM STDIN WITH (FORMAT CSV);
"""

cur.copy_expert(copy_query, buffer)

conn.commit()

df_chef_lieu_region = df_region[['REG', 'CHEFLIEU']]
df_chef_lieu_region = df_chef_lieu_region.rename(columns={'REG': 'id_region', 'CHEFLIEU': 'id_chef_lieu'})
# On enleve les regions d'outre mer
df_chef_lieu_region = df_chef_lieu_region[df_chef_lieu_region['id_region'] > 6]

buffer = io.StringIO()
df_chef_lieu_region.to_csv(buffer, index=False, header=False)
buffer.seek(0)

copy_query = """
COPY chef_lieu_region (id_region, id_chef_lieu) FROM STDIN WITH (FORMAT CSV);
"""

cur.copy_expert(copy_query, buffer)

conn.commit()