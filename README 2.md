# Pipeline d'Ingestion Kafka — Équipe SE2
**Projet Big Data — Yelp Open Dataset**  
Meryem Koulam & Ibtisam Wardi

---

## Vue d'ensemble

Ce dépôt contient l'infrastructure de messagerie distribuée développée par l'équipe SE2 dans le cadre du projet Big Data sur le Yelp Open Dataset. L'objectif est de déployer un broker Apache Kafka (mode KRaft) via Docker et d'y ingérer les données Yelp à travers un Producer Python, pour permettre aux équipes SE3 et SE4 de consommer les flux en temps réel.

---

## Architecture

```
[Script Python Producer]          [Kafka UI - Navigateur]
        |                                   |
  localhost:9092                    localhost:8080
  (Listener EXTERNAL)                       |
        |                         kafka:29092 (Listener INTERNAL)
        └──────────────┬──────────────────┘
                       ▼
             [Broker Kafka KRaft]
             ┌─────────────────┐
             │  topic_business  │  ← Données Batch (141 035 entités)
             │  topic_reviews   │  ← Données Streaming (temps réel)
             └─────────────────┘
```

---

## Prérequis

- [Docker](https://www.docker.com/) et Docker Compose
- Python 3.8+
- Bibliothèque `kafka-python`

```bash
pip install kafka-python
```

- Fichiers de données Yelp (à placer dans le même répertoire que `producer_final.py`) :
  - `yelp_academic_dataset_business.json`
  - `mock_review.json`

---

## Démarrage rapide

### 1. Lancer le cluster Kafka

```bash
docker-compose up -d
```

> Le broker Kafka prend **30 à 45 secondes** pour s'initialiser complètement. Ne pas lancer le Producer avant que le cluster soit prêt.

### 2. Vérifier l'état du cluster

Ouvrir l'interface Kafka UI dans un navigateur :

```
http://localhost:8080
```

Attendre que le cluster affiche le statut **Online** avant de continuer.

### 3. Lancer le Producer Python

```bash
python producer_final.py
```

---

## Fonctionnement du Producer

Le script `producer_final.py` effectue deux phases d'ingestion :

**Phase 1 — Batch (données Business)**
Lecture ligne par ligne du fichier `yelp_academic_dataset_business.json` et envoi vers `topic_business`. Le traitement ligne par ligne évite de saturer la RAM (bonne pratique Big Data).

**Phase 2 — Streaming (avis clients)**
Envoi des reviews depuis `mock_review.json` vers `topic_reviews` avec une pause de 0,5 seconde entre chaque message, simulant un flux temps réel. Cela permet aux équipes consommatrices de tester leurs fenêtres temporelles (windowing).

---

## Configuration réseau (Listeners)

La configuration multi-listeners dans `docker-compose.yml` résout le problème classique de Docker networking :

| Listener | Port | Utilisé par |
|---|---|---|
| `INTERNAL` | `kafka:29092` | Kafka UI (conteneur Docker) |
| `EXTERNAL` | `localhost:9092` | Script Python (hôte) |
| `CONTROLLER` | `kafka:9093` | Gestion interne KRaft |

> Un conteneur Docker ne peut pas utiliser `localhost` pour joindre un autre conteneur — `localhost` y désigne le conteneur lui-même. D'où la nécessité d'un listener dédié via le DNS interne Docker (`kafka:29092`).

---

## Informations pour les équipes consommatrices (SE3 / SE4)

| Paramètre | Valeur |
|---|---|
| Bootstrap Server | `localhost:9092` |
| Topic Métier (Batch) | `topic_business` |
| Topic Avis (Streaming) | `topic_reviews` |
| Format des messages | JSON (sérialisé en UTF-8) |
| Nombre d'entités Business | 141 035 |

---

## Choix d'architecture justifiés

**Pourquoi KRaft plutôt que Zookeeper ?**
Le mode KRaft internalise la gestion des métadonnées dans le broker lui-même. Résultat : un seul conteneur au lieu de deux, un démarrage en ~3 secondes, et une consommation RAM réduite de moitié — idéal pour des machines personnelles.

**Pourquoi Docker ?**
La conteneurisation garantit que Kafka s'exécute de manière identique sur toutes les machines des sous-équipes, indépendamment de la version Java ou de l'OS.

**Pourquoi Kafka ?**
Kafka joue le rôle de tampon (buffer) entre les équipes. Si SE2 ingère à 1 000 msg/sec et que SE4 traite à 100 msg/sec, Kafka absorbe la différence sur disque, évitant tout crash par surcharge.

---

## Incidents résolus (Sprint 2)

| Incident | Problème | Solution |
|---|---|---|
| Permissions Docker | Boucle de redémarrage (`Permission Denied` sur volumes) | Migration vers KRaft + volumes natifs Docker |
| Kafka UI hors ligne | `localhost` dans Docker = le conteneur lui-même | Listener `INTERNAL` sur `kafka:29092` |
| `NoBrokersAvailable` | Résolution DNS : `localhost` → IPv6 (`::1`) au lieu d'IPv4 | Utiliser `127.0.0.1` explicitement dans `bootstrap_servers` |

---

## Structure du dépôt

```
.
├── docker-compose.yml          # Infrastructure Kafka (KRaft + Kafka UI)
├── producer_final.py           # Script d'ingestion (Batch + Streaming)
├── yelp_academic_dataset_business.json   # Données Business Yelp (à fournir)
└── mock_review.json            # Données Reviews (échantillon, à fournir)
```

---

## Protocole de lancement recommandé

1. `docker-compose up -d`
2. Attendre 45 secondes
3. Vérifier **http://localhost:8080** → statut Online
4. `python producer_final.py`

> Ne jamais lancer le Producer immédiatement après `docker-compose up`. Le port 9092 est exposé avant que Kafka soit réellement prêt à accepter des connexions.
