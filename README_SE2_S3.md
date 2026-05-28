# 🚀 Pipeline d'Ingestion Kafka — Équipe SE2

> **Projet Big Data** | Apache Kafka Producer | Dataset Yelp Academic  
> *Meryem & Ibtisam*

---

## 📋 Présentation

Ce dépôt contient le **Producer Kafka** développé par l'équipe SE2 dans le cadre du projet Big Data. L'objectif est d'ingérer les 5 fichiers JSON du dataset Yelp Academic vers un cluster Apache Kafka local, en garantissant vitesse, fiabilité et intégrité des données pour les équipes consommatrices en aval (SE3 & SE4).

---

## 🏗️ Architecture

```
Fichiers JSON (5x)
      │
      ▼
 Producer Python
 (kafka-producer.py)
      │
      ▼
 Broker Kafka (kafka:29092)
      │
      ├──▶ topic_business
      ├──▶ topic_reviews
      ├──▶ topic_users
      ├──▶ topic_tips
      └──▶ topic_checkins
```

---

## 📁 Fichiers Sources & Topics Kafka

| Fichier JSON | Topic Kafka | Messages ingérés | Taille brute (JSON) | Taille Kafka (LZ4) |
|---|---|---|---|---|
| `yelp_academic_dataset_user.json` | `topic_users` | 1 626 396 | ~3.3 GB | 2 GB |
| `yelp_academic_dataset_review.json` | `topic_reviews` | 258 329 | ~4.5 GB | 123 MB |
| `yelp_academic_dataset_tip.json` | `topic_tips` | 908 915 | ~490 MB | 110 MB |
| `yelp_academic_dataset_checkin.json` | `topic_checkins` | 131 929 | ~270 MB | 145 MB |
| `yelp_academic_dataset_business.json` | `topic_business` | 150 346 | ~115 MB | 43 MB |
| **Total** | **5 topics** | **3 075 915** | **~8.65 GB** | **~2.4 GB** |

> 💡 **Note** : La taille brute (~8.65 GB) correspond au dataset JSON officiel Yelp non compressé ([source officielle](https://business.yelp.com/data/resources/open-dataset/)). La taille Kafka (~2.4 GB) reflète le stockage après compression **LZ4** appliquée par le Producer.

---

## ⚙️ Prérequis

- Python 3.8+
- Un cluster Apache Kafka accessible sur `kafka:29092`
- Les 5 fichiers JSON du dataset Yelp placés dans le même répertoire que le script

**Installation des dépendances :**

```bash
pip install kafka-python
```

---

## ▶️ Utilisation

1. Clonez ce dépôt et placez les fichiers JSON dans le dossier racine.
2. Assurez-vous que votre broker Kafka est lancé et accessible.
3. Exécutez le script :

```bash
python kafka-producer.py
```

Le script traitera les fichiers **séquentiellement** et affichera la progression tous les 100 000 messages envoyés.

---

## 🔧 Choix Techniques

### Traitement séquentiel
Les 5 fichiers sont traités **l'un après l'autre** (et non en parallèle) afin d'éviter toute saturation de la RAM et tout goulot d'étranglement I/O disque. Cette approche garantit la stabilité du système.

### Micro-Batching & Compression LZ4
Le Producer est configuré en mode **Haute Performance** :

```python
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'],
    batch_size=65536,       # Paquets de 64 KB
    linger_ms=10,           # Attente max avant envoi
    compression_type='lz4'  # Compression réseau
)
```

Ces paramètres regroupent les messages en lots compressés, réduisant la bande passante et maximisant le débit.

---

## 🛡️ Tolérance aux Pannes

Des `JSONDecodeError` ont été détectées lors des premiers tests, causées par des caractères de contrôle invisibles dans `review.json`. La solution adoptée :

```python
data = json.loads(line, strict=False)  # Tolérance aux caractères de contrôle
```

Les lignes non parsables sont **ignorées et comptabilisées** sans interrompre le pipeline. Au total, **2 lignes corrompues** ont été détectées (1 dans `review.json`, 1 dans `user.json`), pour un taux d'intégrité des données de **99.9999 %**.

---

## ✅ Résultats

L'ingestion s'est terminée avec un **succès total** : les 5 topics Kafka ont été alimentés avec intégrité, confirmé via l'interface *UI for Apache Kafka*.

---

## 👥 Équipe

| Membre | Rôle |
|---|---|
| Meryem | Développement Producer & Tests · Architecture & Documentation |
| Ibtisam | Développement Producer & Tests · Architecture & Documentation |

*Équipe SE2 — Projet Big Data*
