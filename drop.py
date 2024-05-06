import psycopg2
import psycopg2.extras
from db import connect

conn = connect()
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

drop_tables_queries = [
    """DROP TABLE IF EXISTS statistiques_population CASCADE;""",
    """DROP TABLE IF EXISTS statistiques_mariages_age CASCADE;""",
    """DROP TABLE IF EXISTS statistiques_mariages_etat_matrimonial CASCADE;""",
    """DROP TABLE IF EXISTS statistiques_mariages_mensuel CASCADE;""",
    """DROP TABLE IF EXISTS statistiques_mariages_origine CASCADE;""",
    """DROP TABLE IF EXISTS chef_lieu_region CASCADE;""",
    """DROP TABLE IF EXISTS chef_lieu_departement CASCADE;""",
    """DROP TABLE IF EXISTS commune CASCADE;""",
    """DROP TABLE IF EXISTS departement CASCADE;""",
    """DROP TABLE IF EXISTS region CASCADE;"""
]

for query in drop_tables_queries:
    cur.execute(query)

conn.commit()

print("Tables supprimées avec succès")