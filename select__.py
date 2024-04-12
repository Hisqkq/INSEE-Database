import psycopg2
import psycopg2.extras
import pandas as pd

USERNAME="postgres"
PASS="1234"

try:
    conn = psycopg2.connect(host="localhost", dbname="db_projet", user=USERNAME, password=PASS)
except Exception as e :
    exit("Connexion impossible à la base de données: " + str(e))

print('Connecté à la base de données')
cur = conn.cursor()

slect_query = """
SELECT * FROM chef_lieu_region;
"""

cur.execute(slect_query)

rows = cur.fetchall()

for row in rows:
    print(row)
    
conn.commit()

