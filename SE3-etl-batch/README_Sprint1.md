# SE3 — Sprint 1 : Nettoyage des Données (MOCK) 🧹

> Architecture du Pipeline de Traitement et Stratégie de Nettoyage des Données  
> Université Mohammed V Rabat — Master SNIA — Big Data Project

---

## Équipe

| Rôle | Nom |
|---|---|
| Membre | Abbar Amina |
| Membre | Nader Chaimaa |
| Encadrant | Pr. EL ASRI Mohamed Youssef |

---

##  Objectif du Sprint 1

Valider la logique de traitement **avant le passage au cluster Big Data** en appliquant un pipeline de nettoyage local sur le fichier `mock_yelp_data.csv` avec **Python / Pandas**.

##  Structure du dossier

```
SE3-etl-batch/
├── README_sprint1.md                        ← Ce fichier
├── data/
│   ├── mock_yelp_data.csv           ← Dataset MOCK brut 
│   └── mock_yelp_data_cleaned.csv   ← Dataset nettoyé 
└──sprint1
    └── s1_cleaning_pandas.py        ← Script de nettoyage Pandas
```

##  Stack Technique — Sprint 1

| Technologie | Version recommandée | Rôle |
|---|---|---|
| Python | 3.8+ | Langage standard |
| Pandas | 1.5+ | Nettoyage et transformation local |

> Les sprints suivants (S2→S4) utiliseront PySpark + Spark SQL + HDFS.

---

##  Prérequis

### 1. Installer Python et les dépendances
```bash
pip install pandas
```

### 2. Vérifier l'installation
```bash
python --version
python -c "import pandas; print(pandas.__version__)"
```

---

##  Exécution

```bash
cd sprint1
python s1_cleaning_pandas.py
```

Le script lit `../data/mock_yelp_data.csv` et produit `../data/mock_yelp_data_cleaned.csv`.

---

##  Dataset MOCK — `mock_yelp_data.csv`
- **Colonnes** : `business_id`, `name`, `city`, `state`, `stars`, `review_count`, `categories`, `is_open`, `latitude`, `longitude`

### Anomalies présentes (volontaires)

| Code | Type d'anomalie | Description |
|---|---|---|
| ERR_* | IDs corrompus | business_id commençant par ERR_ |
| — | Doublons | Doublons sur business_id (4 cas) |
| ERR_STAR | Notes invalides | Valeurs hors échelle [0-5] : ex. -1, 7 |
| ERR_GEO | GPS manquants | latitude et/ou longitude absentes |
| ERR_CITY | Ville manquante | Champ city vide |
| ERR_LOWER | Casse incohérente | Ville en minuscules (ex. apollo beach) |
| ERR_CAT | Catégories nulles | Champ categories null |

---

##  Plan de Nettoyage — 8 Opérations Séquentielles



| # | Opération | Colonne(s) | Résultat | Justification Technique |
|---|---|---|---|---|
| 1 | Suppression des doublons | business_id | −4 lignes | Un commerce dupliqué fausse les moyennes, comptages et toutes les statistiques agrégées |
| 2 | Suppression notes invalides | stars | 0 (déjà supprimées via ERR_) | L'échelle Yelp est strictement [0-5]. Toute valeur hors plage (-1, 7...) est physiquement impossible et corrompt les calculs statistiques |
| 3 | Suppression GPS manquants | latitude, longitude | 0 (déjà supprimées via ERR_) | Sans coordonnées valides, aucune analyse géographique (cartographie, clustering spatial) n'est réalisable |
| 4 | Suppression ville manquante | city | 0 (déjà supprimées via ERR_) | La ville est la clé de regroupement principale pour toutes les agrégations géographiques. Sa présence est obligatoire |
| 5 | Normalisation de casse ville | city | apollo beach → Apollo Beach | 'apollo beach' != 'Apollo Beach' lors d'un groupBy → création de faux doublons qui fragmentent les agrégats |
| 6 | Imputation catégories vides | categories | null → "Unknown" | Remplacement null par 'Unknown' : conserve la ligne et évite la perte d'informations valides dans les autres colonnes |
| 7 | Conversion str → types numériques | stars, latitude, longitude, review_count | Types corrects | Les colonnes importées comme chaînes doivent être converties avant tout filtrage numérique pour éviter la perte de données valides (ex: "2.5") |
| 8 | Nettoyage espaces superflus | name, city, state | Espaces supprimés | Des espaces invisibles causent des inégalités incorrectes lors des jointures |

** Résultat final : 36 lignes propres → `mock_yelp_data_cleaned.csv`**

---

##  Fichier de Sortie

| Propriété | Valeur |
|---|---|
| Fichier | `mock_yelp_data_cleaned.csv` |
| Colonnes | 10 (inchangées) |
| Format | CSV |

Ce fichier servira d'**entrée au Sprint 2** pour les transformations PySpark enrichies.

---

##  Liens utiles

- [Documentation Pandas](https://pandas.pydata.org/docs/)
- [Dépôt GitHub du projet](https://github.com/TheHunter365/BigDataGroupe1)
