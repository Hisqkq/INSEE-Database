import psycopg2
import psycopg2.extras
from db_connection import connect

conn = connect()
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

select_query = """
SELECT * FROM chef_lieu_departement;
"""

cur.execute(select_query)

rows = cur.fetchall()

for row in rows:
    print(row['id_departement'], row['id_chef_lieu'])
    
conn.commit()

