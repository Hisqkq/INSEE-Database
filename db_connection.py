import psycopg2

USERNAME = "postgres"
PASS = "1234"

def connect():
    try:
        conn = psycopg2.connect(host="localhost", dbname="db_projet", user=USERNAME, password=PASS)
        return conn
    except Exception as e:
        exit("Connexion impossible à la base de données: " + str(e))
