import psycopg2
import io

USERNAME = "postgres"
PASS = "1234"

def connect():
    try:
        conn = psycopg2.connect(host="localhost", dbname="db_projet", user=USERNAME, password=PASS)
        return conn
    except Exception as e:
        exit("Connexion impossible à la base de données: " + str(e))

def insert_dataframe_into_table(df, table_name, columns): 
    conn = connect()
    cur = conn.cursor()
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    copy_query = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV);"
    cur.copy_expert(copy_query, buffer)
    conn.commit()
    print(f"Données insérées dans la table '{table_name}' avec succès")
    cur.close()
    conn.close()