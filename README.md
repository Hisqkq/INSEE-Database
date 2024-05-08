# Projet de base de données relationnelles

Ce projet consiste à construire et manipuler une base de données relationnelles avec un grand volume de données réelles provenant du site de l'INSEE. Nous avons modélisé et construit une base de données en 3FN pour gérer les régions, les départements et les villes françaises. Nous avons également importé la population de chaque commune depuis les séries historiques 2020 et les statistiques régionales et départementales sur les mariages en 2021.

## Prérequis

* PostgreSQL
* Python 3
* Jupyter Notebook
* Bibliothèque Python psycopg2

## Installation

1. Installer PostgreSQL et créer une base de données vide.
2. Ouvrir le fichier rapport.ipynb dans Jupyter Notebook (et mettez à jour le fichier `config.ini` si nécessaire).
3. Utiliser le programme fourni pour importer les données dans la base de données PostgreSQL.

## Utilisation

Le fichier rapport.ipynb contient toutes les commandes principales pour interroger la base de données et afficher les résultats de façon lisible et compréhensible. Les requêtes incluent :

* Liste des départements d'une région donnée.
* Liste des communes de plus de X habitants d'un département donné.
* La région la plus/la moins peuplée.

Le fichier rapport.ipynb contient également des instructions pour créer des vues, des procédures stockées et des triggers pour automatiser les calculs de population et bloquer les modifications sur certaines tables. Il contient également des exemples de plans d'exécution et d'utilisation d'index pour optimiser les requêtes.

## Contribution

Les contributions sont les bienvenues. Si vous souhaitez contribuer à ce projet, veuillez ouvrir une pull request.

## Auteurs

* [Théo Lavandier]
* [Hamad Tria]