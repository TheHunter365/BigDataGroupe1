# Utilisation d'une image Python légère basée sur Debian Bookworm
FROM python:3.10-slim-bookworm

# Définition du répertoire de travail à l'intérieur du conteneur
WORKDIR /app

# 1. Mise à jour du système et installation de Java 17 (nécessaire pour Flink)
# On installe les dépendances système nécessaires pour le bon fonctionnement de Java
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        wget \
        ca-certificates \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Configuration des variables d'environnement pour Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# 2. Installation des bibliothèques Python nécessaires pour le Producer Kafka
# kafka-python pour communiquer avec le broker et lz4 pour la compression
RUN pip install --no-cache-dir --default-timeout=1000\
    "apache-flink==1.20.0" \
    kafka-python-ng \
    lz4

# 3. Préparation de l'environnement Flink et téléchargement du connecteur Kafka
# On crée le dossier lib pour Flink et on télécharge le connecteur compatible
RUN mkdir -p /opt/flink/lib \
    && curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.1-2.0/flink-sql-connector-kafka-4.0.1-2.0.jar

# 4. Copie des scripts Python (Producer et Flink) depuis le dossier 'streaming' vers le conteneur
# Note : Ces fichiers doivent être copiés dans le répertoire de travail /app
COPY flink.py ./

# 5. Définition de l'entrée par défaut (Python)
# Le conteneur exécutera la commande par défaut avec le script spécifié dans le compose
ENTRYPOINT ["python"]
CMD ["flink.py"]