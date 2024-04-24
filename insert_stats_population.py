import pandas as pd
import io
from db_connection import connect

def convert_to_int(value):
    # Si la valeur est NaN, retourner None
    if value is None or pd.isna(value):
        return None
    else:
        return int(value)



def insert_dataframe_into_table(df, table_name, columns):
    buffer = io.StringIO()
    df['annee'] = df['annee'].astype(str).astype(int)
    df = df[columns]  # Ajoutez cette ligne pour réorganiser les colonnes
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    copy_query = f"""
    COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH (FORMAT CSV);
    """

    cur.copy_expert(copy_query, buffer)
    conn.commit()
    print(f"Données insérées dans la table '{table_name}' avec succès")

# Connexion à la base de données
conn = connect()
cur = conn.cursor()

# Mappage des colonnes du CSV aux types de statistiques
stat_mapping = {
    'P20_POP': ('Population', 2020),
    'P14_POP': ('Population', 2014),
    'P09_POP': ('Population', 2009),
    'D99_POP': ('Population', 1999),
    'D90_POP': ('Population', 1990),
    'D82_POP': ('Population', 1982),
    'D75_POP': ('Population', 1975),
    'NAIS1420': ('Naissances', 2014, 2020),
    'NAIS0914': ('Naissances', 2009, 2014),
    'NAIS9909': ('Naissances', 1999, 2009),
    'NAIS9099': ('Naissances', 1990, 1999),
    'NAIS8290': ('Naissances', 1982, 1990),
    'NAIS7582': ('Naissances', 1975, 1982),
    'NAIS6875': ('Naissances', 1968, 1975),
    'DECE1420': ('Décès', 2014, 2020),
    'DECE0914': ('Décès', 2009, 2014),
    'DECE9909': ('Décès', 1999, 2009),
    'DECE9099': ('Décès', 1990, 1999),
    'DECE8290': ('Décès', 1982, 1990),
    'DECE7582': ('Décès', 1975, 1982),
    'DECE6875': ('Décès', 1968, 1975),
    'P20_LOG': ('Nombre de Logements', 2020),
    'P14_LOG': ('Nombre de Logements', 2014),
    'P09_LOG': ('Nombre de Logements', 2009), 
    'D99_LOG': ('Nombre de Logements', 1999),
    'D90_LOG': ('Nombre de Logements', 1990),
    'D82_LOG': ('Nombre de Logements', 1982),
    'D75_LOG': ('Nombre de Logements', 1975),
    'D68_LOG': ('Nombre de Logements', 1968),
    'P20_RP': ('Nombre de résidences principales', 2020),
    'P14_RP': ('Nombre de résidences principales', 2014),
    'P09_RP': ('Nombre de résidences principales', 2009),
    'D99_RP': ('Nombre de résidences principales', 1999),
    'D90_RP': ('Nombre de résidences principales', 1990),
    'D82_RP': ('Nombre de résidences principales', 1982),
    'D75_RP': ('Nombre de résidences principales', 1975),
    'D68_RP': ('Nombre de résidences principales', 1968),
    'P20_RSECOCC': ('Nombre de Rés secondaires et logts occasionnels', 2020),
    'P14_RSECOCC': ('Nombre de Rés secondaires et logts occasionnels', 2014),
    'P09_RSECOCC': ('Nombre de Rés secondaires et logts occasionnels', 2009),
    'D99_RSECOCC': ('Nombre de Rés secondaires et logts occasionnels', 1999),
    'D90_RSECOCC': ('Nombre de Rés secondaires et logts occasionnels', 1990),
    'D82_RSECOCC': ('Nombre de Rés secondaires et logts occasionnels', 1982),
    'D75_RSECOCC': ('Nombre de Rés secondaires et logts occasionnels', 1975),
    'D68_RSECOCC': ('Nombre de Rés secondaires et logts occasionnels', 1968),
    'P20_LOGVAC': ('Nombre de logement vacants', 2020),
    'P14_LOGVAC': ('Nombre de logement vacants', 2014),
    'P09_LOGVAC': ('Nombre de logement vacants', 2009),
    'D99_LOGVAC': ('Nombre de logement vacants', 1999),
    'D90_LOGVAC': ('Nombre de logement vacants', 1990),
    'D82_LOGVAC': ('Nombre de logement vacants', 1982),
    'D75_LOGVAC': ('Nombre de logement vacants', 1975),
    'D68_LOGVAC': ('Nombre de logement vacants', 1968),
    'P20_PMEN': ('Nombre de personnes des ménages', 2020),
    'P14_PMEN': ('Nombre de personnes des ménages', 2014),
    'P09_PMEN': ('Nombre de personnes des ménages', 2009),
    'D99_PMEN': ('Nombre de personnes des ménages', 1999),
    'D90_NPER_RP': ('Nombre de personnes des ménages', 1990),
    'D82_NPER_RP': ('Nombre de personnes des ménages', 1982),
    'D75_NPER_RP': ('Nombre de personnes des ménages', 1975),
    'D68_NPER_RP': ('Nombre de personnes des ménages', 1968),
}

start_year = 2010
end_year = 2020

# Filtrer les entrées de stat_mapping pour ne garder que celles pertinentes pour l'intervalle d'années
filtered_stat_mapping = {
    key: value for key, value in stat_mapping.items()
    if (value[1] >= start_year and value[1] <= end_year) or (len(value) == 3 and value[2] >= start_year and value[2] <= end_year)
}

# Lecture du fichier CSV
df = pd.read_csv('data/statistiques/population/base-cc-serie-historique-2020.csv', sep=';', dtype={'CODGEO': str})
df = df[~df['CODGEO'].str.startswith(('970', '971', '972', '973', '974', '975', '976', '977', '978'))]

stat_dfs = []

# Générer les DataFrames pour chaque type de statistique pertinent
for col_name, stat_info in filtered_stat_mapping.items():
    stat_type, annee1, annee2 = stat_info if len(stat_info) == 3 else stat_info + (None,)

    stat_df = df[['CODGEO', col_name]].copy()
    stat_df.columns = ['codgeo', 'valeur']
    stat_df['annee'] = int(annee1)
    stat_df['type_statistique'] = stat_type
    stat_df['annee2'] = int(annee2) if annee2 is not None else None

    stat_dfs.append(stat_df)

# Concaténer tous les DataFrames de statistiques en un seul DataFrame
all_stat_df = pd.concat(stat_dfs, ignore_index=True)

# Convertir les colonnes en types appropriés
all_stat_df['codgeo'] = all_stat_df['codgeo'].astype(str)
all_stat_df['annee'] = all_stat_df['annee'].astype(int)
all_stat_df['type_statistique'] = all_stat_df['type_statistique'].astype(str)
all_stat_df['valeur'] = all_stat_df['valeur'].astype(float)
all_stat_df['annee2'] = all_stat_df['annee2'].fillna(0).astype(int)

print(all_stat_df.head())

# Insérer les données dans la table statistiques_population
insert_dataframe_into_table(all_stat_df, 'statistiques_population', ['codgeo', 'annee', 'type_statistique', 'valeur', 'annee2'])