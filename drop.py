import psycopg2
import psycopg2.extras
from db_connection import connect

conn = connect()
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

drop_tables_queries = [
    """DROP TABLE statistiques_population;""",
    """DROP TABLE statistique_mariage;""",
    """DROP TABLE chef_lieu_region;""",
    """DROP TABLE chef_lieu_departement;""",
    """DROP TABLE commune;""",
    """DROP TABLE departement;""",
    """DROP TABLE region;"""
]

for query in drop_tables_queries:
    cur.execute(query)

conn.commit()

print("Tables supprimées avec succès")