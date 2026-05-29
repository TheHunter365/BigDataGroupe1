FROM python:3.10-slim-bookworm

WORKDIR /app

# Installation de curl au cas où pour les healthchecks ou scripts d'attente
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# On installe uniquement le connecteur Kafka moderne
RUN pip install --no-cache-dir kafka-python-ng lz4

# On copie uniquement le script du producer
COPY kafka-producer.py ./

ENTRYPOINT ["python"]
CMD ["kafka-producer.py"]