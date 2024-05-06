import psycopg2
import psycopg2.extras
from db import connect

conn = connect()
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Nombre de population par region en 2020 

select_query = """
SELECT SUM(valeur) as population_region, region.nom_region 
FROM statistiques_population JOIN commune ON statistiques_population.codgeo = commune.id_commune
JOIN departement ON commune.id_departement = departement.id_departement
JOIN region ON departement.id_region = region.id_region
WHERE type_statistique = 'Population' AND annee = 2020
GROUP BY region.nom_region;
"""

select_query = """
SELECT * from departement;
"""

select_query = """
SELECT * FROM commune_population;
"""

cur.execute(select_query)

rows = cur.fetchall()

for row in rows:
    print(row)
    #print('Population de la région', row['nom_region'], 'en 2020:', row['population_region'])
    
conn.commit()

