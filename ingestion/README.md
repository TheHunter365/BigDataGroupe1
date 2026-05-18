# 🍽️ Yelp Data Pipeline — Simulation d'Ingestion

Pipeline de données simulant l'ingestion en temps réel du dataset académique Yelp.
Le script `producer_mock.py` joue le rôle d'un **producteur Kafka offline** : il lit les données business en batch, puis streame les avis (reviews) avec un délai artificiel pour imiter un flux live.

---

## 📁 Structure du projet

```
.
├── ingestion/
│   ├── producer_mock.py                  # Script principal d'ingestion
│   └── README.md
└── data/sample/
    ├── mock_review.json                  # Échantillon de 100 avis (JSONL)
    └── yelp_academic_dataset_business.json # Dataset complet des entreprises (~118 MB, JSONL)
```

---

## 🗂️ Description des fichiers de données

### `yelp_academic_dataset_business.json`
Fichier JSONL (une entrée JSON par ligne) contenant les entreprises référencées sur Yelp.

| Champ        | Type    | Description                        |
|--------------|---------|------------------------------------|
| `business_id`| string  | Identifiant unique de l'entreprise |
| `name`       | string  | Nom de l'établissement             |
| `city`       | string  | Ville                              |
| `stars`      | float   | Note moyenne (1.0 à 5.0)           |
| `review_count`| int    | Nombre total d'avis                |
| `categories` | string  | Catégories séparées par des virgules |

### `mock_review.json`
Échantillon de **100 avis** au format JSONL, extrait du dataset Yelp Reviews pour les tests.

| Champ       | Type   | Description                        |
|-------------|--------|------------------------------------|
| `review_id` | string | Identifiant unique de l'avis       |
| `user_id`   | string | Identifiant de l'auteur            |
| `business_id`| string| Entreprise concernée               |
| `stars`     | float  | Note donnée (1.0 à 5.0)            |
| `text`      | string | Contenu textuel de l'avis          |
| `date`      | string | Horodatage (`YYYY-MM-DD HH:MM:SS`) |
| `useful`    | int    | Votes "utile"                      |
| `funny`     | int    | Votes "drôle"                      |
| `cool`      | int    | Votes "cool"                       |

---

## ⚙️ Fonctionnement du script

Le script `producer_mock.py` s'exécute en deux phases séquentielles :

```
PHASE 1 — Batch Business
  └── Lecture ligne par ligne de yelp_academic_dataset_business.json
  └── Affichage des 5 premières entrées + comptage total

PHASE 2 — Streaming Reviews (Simulation)
  └── Lecture ligne par ligne de mock_review.json
  └── Pause de 0.5s entre chaque avis pour simuler un flux temps réel
```

> 💡 **Pourquoi ligne par ligne ?**
> Les fichiers JSONL sont volumineux. Charger tout le fichier en RAM d'un coup (`json.load()`) provoquerait un crash sur les petites machines. La lecture ligne par ligne (`for line in f`) maintient une empreinte mémoire constante, quel que soit la taille du fichier.

---

## 🚀 Utilisation

### Prérequis

- Python 3.7+
- Aucune dépendance externe (uniquement la bibliothèque standard)

### Installation

```bash
git clone <url-du-repo>
cd <nom-du-repo>
```

### Lancement

Les fichiers de données sont attendus dans `data/sample/`. Depuis la racine du dépôt :

```bash
python ingestion/producer_mock.py
```

### Exemple de sortie

```
🚀 DÉMARRAGE DE L'INGESTION DES DONNÉES (OFFLINE MOCK)
==================================================

🏢 PHASE 1 : Lecture du fichier Business...
   ✅ [Business 1] Rancho Caliente | Ville: Las Vegas | ⭐ 4.0
   ✅ [Business 2] Body Cycle Studio | Ville: Pittsburgh | ⭐ 4.5
   ...
✔️ SUCCÈS : 150346 entreprises lues avec succès !

💬 PHASE 2 : Streaming des Reviews (Simulation Live)...
   🔄 [NOUVEL AVIS] 📅 2018-07-07 22:09:11 | ⭐ 3.0 étoiles | 🆔 KU_O5udG6zpxOg-VcAEodg
   🔄 [NOUVEL AVIS] 📅 2012-01-03 15:28:18 | ⭐ 5.0 étoiles | 🆔 BiTunyQ73aT9WBnpR9DZGw
   ...
✔️ SUCCÈS : Fin du flux de données MOCK.

==================================================
🏁 PIPELINE D'INGESTION (TEST LOCAL) TERMINÉ.
```

---

## 🔭 Évolutions prévues

Ce script est une **simulation locale** destinée à valider la logique d'ingestion avant intégration avec Apache Kafka.

| Étape | Description |
|-------|-------------|
| ✅ Mock local | Lecture fichiers + simulation délai |
| 🔲 Producteur Kafka | Envoi des messages dans des topics Kafka |
| 🔲 Consommateur | Lecture et traitement des messages depuis Kafka |
| 🔲 Stockage | Persistance dans une base de données (ex. PostgreSQL, MongoDB) |
| 🔲 Dashboard | Visualisation temps réel des avis |

---

## 📄 Source des données

Dataset issu du **Yelp Open Dataset** — [https://www.yelp.com/dataset](https://www.yelp.com/dataset)

> Utilisation réservée à des fins académiques et de recherche, conformément aux conditions d'utilisation de Yelp.
