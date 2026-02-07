# Documentation Technique
Architecture & Modélisation de l'Application

## 1. Architecture du Modèle de Données
Le modèle est conçu selon une architecture en schéma en étoile (Star Schema). Cette structure garantit une simplicité de maintenance et des performances de calcul optimales en minimisant les jointures complexes.

**Schéma Conceptuel**
Le cœur du modèle est la table de faits, entourée de dimensions normalisées :
* **Table de Faits (`FaitsDPE`) :** Centralise les données brutes des diagnostics (consommation énergétique, émissions de CO2, surfaces).
* **Dimensions (Tables de référence) :**
  * `DimCommune`, `DimDepartement` & `DimRegion`: Hiérarchie géographique permettant l'analyse par département (01 - Ain et 30 - Gard).
  * `DimBatiment` : Caractéristiques physiques des biens.
  * `DimPeriodeConstruction` : Segmentation temporelle des constructions.
  * `DimChauffage` : Typologie des sources d'énergie.
  * `DimDPE` & `DimGES` : Référentiels des classes énergétiques (A à G).
  * `Mesures` : Table technique regroupant l'ensemble des indicateurs DAX (Moyennes, % de passoires thermiques, etc.).
  * `Calendrier` : Dates
 
**Relations**
* **Cardinalité :** Toutes les relations sont de type 1:N (Une-à-plusieurs) depuis les dimensions vers la table de faits.
* **Sens du filtrage :** Unique (des dimensions vers les faits) pour éviter les ambiguïtés de calcul et les boucles de filtrage.


## 2. Sécurité des Données (RLS)
La sécurité au niveau des lignes (Row-Level Security) est implémentée pour garantir la confidentialité entre les départements :
* Logique de filtrage : La table Dim_Geographie est filtrée dynamiquement via la fonction USERPRINCIPALNAME().
* Règles souhaitées :
  * `Role_Maire_01` : `[Code_Departement]` = "01"
  * `Role_Maire_30` : `[Code_Departement]` = "30"
  * `Role_Admin` : Pas de filtre (vue globale).

**Règle réelle :** tous le monde est lecteur et à accès à toutes les pages

## 3. Diagnostic de Performance
Voici un résumé des performances globales du tableau de bord, extrait de l'analyseur de performance :

**Résumé de la Performance Globale**
| Indicateur | Valeur |
| --- | --- |
| Nombre total de visuels | 47 |
| Temps moyen de requête (DAX) | 1 303 ms |
| Temps moyen de rendu visuel | 565 ms |
| Élément le plus lourd | "Forme" (7 346 ms) |
| Performance globale | <span style="color:orange">Optimisation suggérée</span> |

**Top 5 des visuels les plus gourmands**
| Visuel | Requête DAX (ms) | Rendu (ms) | Total (ms) |
| --- | --- | --- | --- |
| **Forme (Arrière-plan/Déco)** | 911 | 6 435 | 7 346 |
| **Besoin de chauffage par m²** | 4 034 | 554 | 4 588 |
| **Image (Logos/Icones)** | 2 866 | 1 708 | 4 574 |
| **Distribution des DPE** | 3 524 | 671 | 4 195 |
| **Répartition périodes construction** | 3 331 | 504 | 3 835 |

** 4. Maintenance et Évolutivité
* **Source de données :** Les données sont issues des extractions ADEME pour les départements 01 et 30.
* **Ajout de données :** Pour intégrer un nouveau département (ex: Haute-Savoie - 74), il suffit d'ajouter les lignes correspondantes dans les tables FaitsDPE et DimCommune et de créer le rôle RLS associé.
* **Normalisation :** Les libellés des classes DPE et GES sont harmonisés dans les tables Dim pour éviter les erreurs de saisie provenant des données sources.

