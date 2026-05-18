

# SE4 — Flink & Kafka (mode Docker)

Ce dossier contient des exemples pour intégrer un producteur Kafka et un consommateur Flink. Les conteneurs sont construits à partir du `Dockerfile` et orchestrés par `docker-compose.yml` — il n'est pas nécessaire d'installer Python ou des paquets Python localement.

Contenu principal
- `docker-compose.yml` : compose définissant les services `kafka`, `jobmanager`, `taskmanager`, `python-producer`, `python-flink`.
- `Dockerfile` : image basée sur Python + Java, installe `apache-flink` et `kafka-python`, et ajoute le connecteur Kafka pour Flink.
- `flink.py` et `kafka-producer.py` : scripts exécutés à l'intérieur des conteneurs.

Prérequis
- Docker et docker-compose installés sur la machine hôte.

Lancer l'environnement (Docker)

Depuis le dossier `SE4` :

```bash
cd SE4
docker-compose up --build -d
```

Vérifier les services et logs

- Voir l'état des containers :

```bash
docker-compose ps
```

- Suivre les logs du producteur Kafka :

```bash
docker-compose logs -f python-producer
```

- Suivre les logs du job Flink (consommateur) :

```bash
docker-compose logs -f python-flink
```

Accès à l'interface Flink

- L'interface web de Flink est exposée sur le port `8081` : http://localhost:8081

Configuration et variables d'environnement

- Les services `python-producer` et `python-flink` utilisent les variables d'environnement suivantes (définies dans `docker-compose.yml`) :
	- `KAFKA_BOOTSTRAP_SERVERS` (ex. `kafka:9092`)
	- `KAFKA_TOPIC` (par défaut `yelp-reviews-mock`)
	- `FLINK_KAFKA_CONNECTOR_JAR` (chemin vers le connecteur Kafka dans l'image)

Arrêter et nettoyer

```bash
docker-compose down
```
