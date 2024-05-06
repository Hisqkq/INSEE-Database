import psycopg2
import psycopg2.extras
from db import connect

conn = connect()
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# Create triggers to block INSERT, UPDATE, and DELETE operations on the tables REGIONS and DEPARTEMENTS
trigger_queries = [
    """
    CREATE OR REPLACE FUNCTION block_changes()
    RETURNS TRIGGER AS $$
    BEGIN
        RAISE EXCEPTION 'Modifying this table is not allowed';
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """,
    """
    CREATE TRIGGER block_insert_region
    BEFORE INSERT ON region
    FOR EACH STATEMENT
    EXECUTE FUNCTION block_changes();
    """,
    """
    CREATE TRIGGER block_update_region
    BEFORE UPDATE ON region
    FOR EACH STATEMENT
    EXECUTE FUNCTION block_changes();
    """,
    """
    CREATE TRIGGER block_delete_region
    BEFORE DELETE ON region
    FOR EACH STATEMENT
    EXECUTE FUNCTION block_changes();
    """,
    """
    CREATE TRIGGER block_insert_departement
    BEFORE INSERT ON departement
    FOR EACH STATEMENT
    EXECUTE FUNCTION block_changes();
    """,
    """
    CREATE TRIGGER block_update_departement
    BEFORE UPDATE ON departement
    FOR EACH STATEMENT
    EXECUTE FUNCTION block_changes();
    """,
    """
    CREATE TRIGGER block_delete_departement
    BEFORE DELETE ON departement
    FOR EACH STATEMENT
    EXECUTE FUNCTION block_changes();
    """
]

for query in trigger_queries:
    cur.execute(query)

conn.commit()

print("Triggers créées avec succès")
