import psycopg2
import psycopg2.extras
from db import connect

conn = connect()
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Create a view for commune population
commune_view_query = """
CREATE OR REPLACE VIEW commune_population AS
SELECT codgeo, annee, valeur  
FROM statistiques_population 
WHERE type_statistique = 'Population'
ORDER BY annee ASC;
"""

# Execute the view creation queries
cur.execute(commune_view_query)

# Commit the changes and close the connection
conn.commit()
conn.close()