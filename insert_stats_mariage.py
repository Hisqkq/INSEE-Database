import pandas as pd
from db import insert_dataframe_into_table
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)



# Fonction principale pour insérer les données dans la table des mariages
def insert_marriage_data_age(dep1_path, dep3_path, annee, table_name):
    # Lire les données des fichiers CSV
    dep1_data = pd.read_csv(dep1_path, sep=';')
    dep3_data = pd.read_csv(dep3_path, sep=';')
    
    # Préparer les données
    dep1_data = filter_and_prepare_data_age(dep1_data, annee)
    dep1_data.rename(columns={'NBMARIES': 'nb_mariages'}, inplace=True)
    dep1_data['nb_mariages_premier'] = 0  # Initialiser la colonne
    
    dep3_data = filter_and_prepare_data_age(dep3_data, annee)
    dep3_data.rename(columns={'NBMARIES': 'nb_mariages_premier'}, inplace=True)
    
    # Ajouter les valeurs de `dep3` à `dep1`
    for index, row in dep3_data.iterrows():
        condition = (dep1_data['annee'] == row['annee']) & (dep1_data['typmar3'] == row['typmar3']) & \
                    (dep1_data['id_region'] == row['id_region']) & (dep1_data['id_departement'] == row['id_departement']) & \
                    (dep1_data['grage'] == row['grage'])
        dep1_data.loc[condition, 'nb_mariages_premier'] = row['nb_mariages_premier']
    
    insert_dataframe_into_table(dep1_data, table_name, ['annee', 'typmar3', 'id_region', 'id_departement', 'grage', 'nb_mariages', 'nb_mariages_premier'])

def filter_and_prepare_data_age(df, annee):
    df = df[(df['REGDEP_MAR'].str.len() == 4) & (~df['REGDEP_MAR'].str.endswith("XX"))]
    
    df = df[df['GRAGE'] != 'TOTAL']
    df['id_region'] = df['REGDEP_MAR'].str[:2].astype(int)
    df['id_departement'] = df['REGDEP_MAR'].str[2:]

    df['annee'] = annee
    df.rename(columns={'TYPMAR3': 'typmar3', 'GRAGE': 'grage'}, inplace=True)
    
    return df[['annee', 'typmar3', 'id_region', 'id_departement', 'grage', 'NBMARIES']]

Dep1 = 'data/statistiques/mariage/Dep1.csv'
Dep3 = 'data/statistiques/mariage/Dep3.csv'
table_name = 'statistiques_mariages_age'
annee = 2021

# insert_marriage_data_age(Dep1, Dep3, annee, table_name)


# Fonction principale pour insérer les données dans la table matrimoniale
def insert_matrimonial_data(dep2_path, annee, table_name):
    # Lire les données du fichier Dep2
    dep2_data = pd.read_csv(dep2_path, sep=';')
    
    # Préparer les données
    dep2_data = filter_and_prepare_data_matrimonial(dep2_data, annee)
    dep2_data.rename(columns={'TYPMAR': 'typmar', 'SEXE': 'sexe', 'ETAMAT': 'etamat', 'NBMARIES': 'nbmaries'}, inplace=True)
    
    # Insérer les données dans la base de données
    insert_dataframe_into_table(dep2_data, table_name, ['annee', 'typmar', 'id_region', 'id_departement', 'sexe', 'etamat', 'nbmaries'])

# Fonction pour filtrer et préparer les fichiers CSV
def filter_and_prepare_data_matrimonial(df, annee):
    # Filtrer pour les régions continentales et exclure les départements manquants
    df = df[(df['REGDEP_MAR'].str.len() == 4) & (~df['REGDEP_MAR'].str.endswith("XX"))]
    
    # Extraire `id_region` et `id_departement`
    df['id_region'] = df['REGDEP_MAR'].str[:2].astype(int)
    df['id_departement'] = df['REGDEP_MAR'].str[2:]
    
    # Préparer les autres colonnes
    df['annee'] = annee
    
    return df[['annee', 'TYPMAR', 'id_region', 'id_departement', 'SEXE', 'ETAMAT', 'NBMARIES']]

Dep2 = 'data/statistiques/mariage/Dep2.csv'
table_name = 'statistiques_mariages_etat_matrimonial'
annee = 2021

# insert_matrimonial_data(Dep2, annee, table_name)

def insert_monthly_marriage_data(dep6_path, annee, table_name):
    # Lire les données du fichier Dep6
    dep6_data = pd.read_csv(dep6_path, sep=';')
    
    # Préparer les données
    dep6_data = filter_and_prepare_monthly_data(dep6_data, annee)
    dep6_data.rename(columns={'TYPMAR2': 'typmar2', 'MMAR': 'mmar', 'NBMAR': 'nbmar'}, inplace=True)
    
    # Insérer les données dans la base de données
    insert_dataframe_into_table(dep6_data, table_name, ['annee', 'typmar2', 'id_region', 'id_departement', 'mmar', 'nbmar'])

# Fonction pour filtrer et préparer les fichiers CSV
def filter_and_prepare_monthly_data(df, annee):
    # Filtrer pour les régions continentales et exclure les départements manquants
    df = df[(df['REGDEP_MAR'].str.len() == 4) & (~df['REGDEP_MAR'].str.endswith("XX"))]
    
    # Extraire `id_region` et `id_departement`
    df['id_region'] = df['REGDEP_MAR'].str[:2].astype(int)
    df['id_departement'] = df['REGDEP_MAR'].str[2:]
    
    # Préparer les autres colonnes
    df['annee'] = annee
    
    return df[['annee', 'TYPMAR2', 'id_region', 'id_departement', 'MMAR', 'NBMAR']]

# Exemple d'utilisation
Dep6 = 'data/statistiques/mariage/Dep6.csv'
table_name = 'statistiques_mariages_mensuel'
annee = 2021

# insert_monthly_marriage_data(Dep6, annee, table_name)


# Fonction principale pour insérer les données dans la table des mariages d'origine
def insert_marriage_data_origine(dep4_path, dep5_path, annee, table_name):
    # Lire les données des fichiers CSV
    dep4_data = pd.read_csv(dep4_path, sep=';')
    dep5_data = pd.read_csv(dep5_path, sep=';')
    
    # Préparer les données
    dep4_data = filter_and_prepare_data_origine(dep4_data, annee, 'NATEPOUX')
    dep4_data.rename(columns={'NBMAR': 'nb_mariages_nationalite'}, inplace=True)
    dep4_data['nb_mariages_pays_naissance'] = 0  # Initialiser la colonne

    dep5_data = filter_and_prepare_data_origine(dep5_data, annee, 'LNEPOUX')
    dep5_data.rename(columns={'NBMAR': 'nb_mariages_pays_naissance'}, inplace=True)
    
    # Ajouter les valeurs de `dep5` à `dep4`
    for index, row in dep5_data.iterrows():
        condition = (dep4_data['annee'] == row['annee']) & (dep4_data['typmar2'] == row['typmar2']) & \
                    (dep4_data['id_region'] == row['id_region']) & (dep4_data['id_departement'] == row['id_departement']) & \
                    (dep4_data['code'] == row['code'])
        dep4_data.loc[condition, 'nb_mariages_pays_naissance'] = row['nb_mariages_pays_naissance']
    
    # Insérer les données dans la base de données
    insert_dataframe_into_table(dep4_data, table_name, ['annee', 'typmar2', 'id_region', 'id_departement', 'code', 'nb_mariages_nationalite', 'nb_mariages_pays_naissance'])

# Fonction pour filtrer et préparer les fichiers CSV
def filter_and_prepare_data_origine(df, annee, code_column):
    # Filtrer pour les régions continentales et exclure les départements manquants
    df = df[(df['REGDEP_DOMI'].str.len() == 4) & (~df['REGDEP_DOMI'].str.endswith("XX"))]
    
    # Filtrer les valeurs "TOTAL" dans la colonne `code_column`
    df = df[df[code_column] != 'TOTAL']
    
    # Extraire `id_region` et `id_departement`
    df['id_region'] = df['REGDEP_DOMI'].str[:2].astype(int)
    df['id_departement'] = df['REGDEP_DOMI'].str[2:]
    
    # Préparer les autres colonnes
    df['annee'] = annee
    df.rename(columns={code_column: 'code', 'TYPMAR2': 'typmar2'}, inplace=True)
    
    return df[['annee', 'typmar2', 'id_region', 'id_departement', 'code', 'NBMAR']]

# Exemple d'utilisation
Dep4 = 'data/statistiques/mariage/Dep4.csv'
Dep5 = 'data/statistiques/mariage/Dep5.csv'
table_name = 'statistiques_mariages_origine'
annee = 2021

#insert_marriage_data_origine(Dep4, Dep5, annee, table_name)

def fill_tables_mariage(annee = 2021):
    # Insertion des données pour les mariages par âge
    insert_marriage_data_age(Dep1, Dep3, annee, 'statistiques_mariages_age')
    
    # Insertion des données pour les mariages par état matrimonial
    insert_matrimonial_data(Dep2, annee, 'statistiques_mariages_etat_matrimonial')
    
    # Insertion des données pour les mariages mensuels
    insert_monthly_marriage_data(Dep6, annee, 'statistiques_mariages_mensuel')
    
    # Insertion des données pour les mariages par origine
    insert_marriage_data_origine(Dep4, Dep5, annee, 'statistiques_mariages_origine')
