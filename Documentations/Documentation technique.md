# Documentation Technique
Architecture & Modélisation de l'Application

## 1. Modèle de Données
Le modèle est structuré selon un Schéma en Étoile (Star Schema) pour optimiser les performances de calcul DAX et la lisibilité :
* Table de Faits (Fact_DPE) : Contient toutes les mesures quantitatives (consommations, surfaces, scores numériques).
* Tables de Dimensions (Dim_Geographie, Dim_Temps, Dim_Batiment) : Contient les attributs descriptifs utilisés pour les filtres et les axes des graphiques.
#### Note : Les relations sont de type 1:N (une-à-plusieurs) avec une direction de filtrage unique pour garantir l'intégrité du modèle.

## 2. Sécurité des Données (RLS)
La sécurité au niveau des lignes (Row-Level Security) est implémentée pour garantir la confidentialité entre les départements :
* Logique de filtrage : La table Dim_Geographie est filtrée dynamiquement via la fonction USERPRINCIPALNAME().
* Règles souhaitées :
  * `Role_Maire_01` : `[Code_Departement]` = "01"
  * `Role_Maire_30` : `[Code_Departement]` = "30"
  * `Role_Admin` : Pas de filtre (vue globale).

**Règle réelle :** tous le monde est lecteur et à accès à toutes les pages

## 3. Diagnostic de Performance
L'analyseur de performance de Power BI a été utilisé pour valider la fluidité du rapport :

| Indicateur | Valeur |
| --- | --- |
| Nombre total de visuels | 47 |
| Temps moyen de requête (DAX) | 1 303 ms |
| Temps moyen de rendu visuel | 565 ms |
| Élément le plus lourd | "Forme" (7 346 ms) |
| Performance globale | <span style="color:orange">Optimisation suggérée</span> |

| Visuel | Requête DAX (ms) | Rendu (ms) | Total (ms) |
| --- | --- | --- | --- |
| **Forme (Arrière-plan/Déco)** | 911 | 6 435 | 7 346 |
| **Besoin de chauffage par m²** | 4 034 | 554 | 4 588 |
| **Image (Logos/Icones)** | 2 866 | 1 708 | 4 574 |
| **Distribution des DPE** | 3 524 | 671 | 4 195 |
| **Répartition périodes construction** | 3 331 | 504 | 3 835 |

**Optimisations effectuées :**
* Récupération des lignes jugées utilent seulement avec R
* Suppression des colonnes inutiles dans Power Query (Réduction de la taille du fichier).
* Utilisation de mesures explicites au lieu de mesures implicites.
* Désactivation de l'option "Date/Heure automatique" pour alléger le modèle.
