import pandas as pd
from db import insert_dataframe_into_table
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.SettingWithCopyWarning)

Dep1 = 'data/statistiques/mariage/Dep1.csv'
Dep2 = 'data/statistiques/mariage/Dep2.csv'
Dep3 = 'data/statistiques/mariage/Dep3.csv'
Dep4 = 'data/statistiques/mariage/Dep4.csv'
Dep5 = 'data/statistiques/mariage/Dep5.csv'
Dep6 = 'data/statistiques/mariage/Dep6.csv'

def insert_marriage_data_age(dep1_path, dep3_path, annee, table_name):
    dep1_data = pd.read_csv(dep1_path, sep=';')
    dep3_data = pd.read_csv(dep3_path, sep=';')
    dep1_data = filter_and_prepare_data_age(dep1_data, annee)
    dep1_data.rename(columns={'NBMARIES': 'nb_mariages'}, inplace=True)
    dep1_data['nb_mariages_premier'] = 0  
    dep3_data = filter_and_prepare_data_age(dep3_data, annee)
    dep3_data.rename(columns={'NBMARIES': 'nb_mariages_premier'}, inplace=True)
    
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


def insert_matrimonial_data(dep2_path, annee, table_name):
    dep2_data = pd.read_csv(dep2_path, sep=';') 
    dep2_data = filter_and_prepare_data_matrimonial(dep2_data, annee)
    dep2_data.rename(columns={'TYPMAR': 'typmar', 'SEXE': 'sexe', 'ETAMAT': 'etamat', 'NBMARIES': 'nbmaries'}, inplace=True)
    
    insert_dataframe_into_table(dep2_data, table_name, ['annee', 'typmar', 'id_region', 'id_departement', 'sexe', 'etamat', 'nbmaries'])

def filter_and_prepare_data_matrimonial(df, annee):
    df = df[(df['REGDEP_MAR'].str.len() == 4) & (~df['REGDEP_MAR'].str.endswith("XX"))]    
    df['id_region'] = df['REGDEP_MAR'].str[:2].astype(int)
    df['id_departement'] = df['REGDEP_MAR'].str[2:]
    df['annee'] = annee
    return df[['annee', 'TYPMAR', 'id_region', 'id_departement', 'SEXE', 'ETAMAT', 'NBMARIES']]



def insert_monthly_marriage_data(dep6_path, annee, table_name):
    dep6_data = pd.read_csv(dep6_path, sep=';')
    
    dep6_data = filter_and_prepare_monthly_data(dep6_data, annee)
    dep6_data.rename(columns={'TYPMAR2': 'typmar2', 'MMAR': 'mmar', 'NBMAR': 'nbmar'}, inplace=True)
    
    insert_dataframe_into_table(dep6_data, table_name, ['annee', 'typmar2', 'id_region', 'id_departement', 'mmar', 'nbmar'])

def filter_and_prepare_monthly_data(df, annee):
    df = df[(df['REGDEP_MAR'].str.len() == 4) & (~df['REGDEP_MAR'].str.endswith("XX"))]
    
    df['id_region'] = df['REGDEP_MAR'].str[:2].astype(int)
    df['id_departement'] = df['REGDEP_MAR'].str[2:]
    
    df['annee'] = annee
    
    return df[['annee', 'TYPMAR2', 'id_region', 'id_departement', 'MMAR', 'NBMAR']]


def insert_marriage_data_origine(dep4_path, dep5_path, annee, table_name):
    dep4_data = pd.read_csv(dep4_path, sep=';')
    dep5_data = pd.read_csv(dep5_path, sep=';')

    dep4_data = filter_and_prepare_data_origine(dep4_data, annee, 'NATEPOUX')
    dep4_data.rename(columns={'NBMAR': 'nb_mariages_nationalite'}, inplace=True)
    dep4_data['nb_mariages_pays_naissance'] = 0  # Initialiser la colonne

    dep5_data = filter_and_prepare_data_origine(dep5_data, annee, 'LNEPOUX')
    dep5_data.rename(columns={'NBMAR': 'nb_mariages_pays_naissance'}, inplace=True)
    
    for index, row in dep5_data.iterrows():
        condition = (dep4_data['annee'] == row['annee']) & (dep4_data['typmar2'] == row['typmar2']) & \
                    (dep4_data['id_region'] == row['id_region']) & (dep4_data['id_departement'] == row['id_departement']) & \
                    (dep4_data['code'] == row['code'])
        dep4_data.loc[condition, 'nb_mariages_pays_naissance'] = row['nb_mariages_pays_naissance']
    
    insert_dataframe_into_table(dep4_data, table_name, ['annee', 'typmar2', 'id_region', 'id_departement', 'code', 'nb_mariages_nationalite', 'nb_mariages_pays_naissance'])

def filter_and_prepare_data_origine(df, annee, code_column):
    df = df[(df['REGDEP_DOMI'].str.len() == 4) & (~df['REGDEP_DOMI'].str.endswith("XX"))]
    df = df[df[code_column] != 'TOTAL']
    df['id_region'] = df['REGDEP_DOMI'].str[:2].astype(int)
    df['id_departement'] = df['REGDEP_DOMI'].str[2:]
    df['annee'] = annee
    df.rename(columns={code_column: 'code', 'TYPMAR2': 'typmar2'}, inplace=True)
    
    return df[['annee', 'typmar2', 'id_region', 'id_departement', 'code', 'NBMAR']]



def fill_tables_mariage(annee = 2021):
    insert_marriage_data_age(Dep1, Dep3, annee, 'statistiques_mariages_age')
    insert_matrimonial_data(Dep2, annee, 'statistiques_mariages_etat_matrimonial')
    insert_monthly_marriage_data(Dep6, annee, 'statistiques_mariages_mensuel')
    insert_marriage_data_origine(Dep4, Dep5, annee, 'statistiques_mariages_origine')
