# Documentation Technique
Architecture & Modélisation de l'Application

## 1. Modèle de Données
Le modèle est structuré selon un Schéma en Étoile (Star Schema) pour optimiser les performances de calcul DAX et la lisibilité :
* Table de Faits (Fact_DPE) : Contient toutes les mesures quantitatives (consommations, surfaces, scores numériques).
* Tables de Dimensions (Dim_Geographie, Dim_Temps, Dim_Batiment) : Contient les attributs descriptifs utilisés pour les filtres et les axes des graphiques.
**Note** : Les relations sont de type 1:N (une-à-plusieurs) avec une direction de filtrage unique pour garantir l'intégrité du modèle.

## 2. Sécurité des Données (RLS)
La sécurité au niveau des lignes (Row-Level Security) est implémentée pour garantir la confidentialité entre les départements :
* Logique de filtrage : La table Dim_Geographie est filtrée dynamiquement via la fonction USERPRINCIPALNAME().
* Règles appliquées :
  * `Role_Maire_75` : `[Code_Departement]` = "75"
  * `Role_Maire_77` : `[Code_Departement]` = "77"
  * `Role_Admin` : Pas de filtre (vue globale).

## 3. Diagnostic de Performance
L'analyseur de performance de Power BI a été utilisé pour valider la fluidité du rapport :
| Élément | Temps de chargement moyen | Statut |
| --- | --- | --- |
| Requêtes DAX | < 150 ms | Optimisé |
| Affichage Visuel | < 300 ms | Fluide |
| Autres (moteur) | < 50 ms | Excellent |
