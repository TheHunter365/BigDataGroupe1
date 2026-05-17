# Cluster Big Data — Hadoop + YARN

Déployé via **Docker Compose** — Hadoop 3.2.1 avec HDFS et YARN.

---

## Services

| Conteneur         | Rôle                             | Port   |
|-------------------|----------------------------------|--------|
| `namenode`        | Gestionnaire HDFS                | `9870` |
| `datanode1/2`     | Stockage distribué (x2)          | —      |
| `resourcemanager` | Gestionnaire des ressources YARN | `8088` |
| `nodemanager1/2`    | Exécuteur de tâches YARN  (x2)        | —      |
| `historyserver`   | Historique des jobs              | —      |

---

## Prérequis
- Docker Desktop lancé et en cours d'exécution

## Lancement

```bash
docker compose up -d
```

Vérifier que tous les conteneurs sont actifs :

```bash
docker compose ps
```

---

## Vérification du cluster

### HDFS

```
http://localhost:9870
```
→ `Live Nodes: 2` dans l'onglet **Datanodes**

```bash
docker exec namenode hdfs dfsadmin -report
```

### YARN

```
http://localhost:8088
```
→ `Active Nodes: 1` dans l'onglet **Nodes**

```bash
docker exec resourcemanager yarn node -list
```

### Test écriture / lecture HDFS

```bash
docker exec namenode hdfs dfs -mkdir -p /test
docker exec namenode bash -c "echo 'Hello HDFS' > /tmp/test.txt && hdfs dfs -put /tmp/test.txt /test/"
docker exec namenode hdfs dfs -cat /test/test.txt
```

Résultat attendu : `Hello HDFS`

---

## 🛑 Arrêt

```bash
docker compose down
```