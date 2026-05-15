# SE5 — ClickHouse & Grafana (mode Docker)

Ce dossier contient une configuration complète pour un pipeline de visualisation de données avec ClickHouse et Grafana. Les conteneurs sont orchestrés par `docker-compose.yml` — il n'est pas nécessaire d'installer les services localement.

## Contenu principal

- `docker-compose.yml` : compose définissant les services `clickhouse` et `grafana`.
- `init-db.sql` : script d'initialisation de la base de données ClickHouse.
- `provisioning/` : configuration Grafana (datasources et dashboards).
- `dashboards/` : dashboards Grafana (ex. `se5-dashboard.json`).
- `rapport/` : documentation et rapports du projet.

## Prérequis

- Docker et docker-compose installés sur la machine hôte.

## Lancer l'environnement (Docker)

Depuis le dossier `se5` :

### Bash / Linux / macOS

```bash
cd se5
docker-compose up --build -d
```

### PowerShell (Windows)

```powershell
cd se5
docker-compose up --build -d
```

## Vérifier les services et logs

### Bash / Linux / macOS

- Voir l'état des conteneurs :

```bash
docker-compose ps
```

- Suivre les logs de ClickHouse :

```bash
docker-compose logs -f clickhouse
```

- Suivre les logs de Grafana :

```bash
docker-compose logs -f grafana
```

### PowerShell (Windows)

- Voir l'état des conteneurs :

```powershell
docker-compose ps
```

- Suivre les logs de ClickHouse :

```powershell
docker-compose logs -f clickhouse
```

- Suivre les logs de Grafana :

```powershell
docker-compose logs -f grafana
```

## Accès aux interfaces

- **Grafana** : http://localhost:3001
  - Identifiants par défaut : `admin` / `admin123`
  - Dashboard par défaut : `se5-dashboard.json`

- **ClickHouse HTTP** : http://localhost:8123
  - Interface native TCP sur le port `9000`

## Configuration et variables d'environnement

Les services utilisent les variables d'environnement suivantes (définies dans `docker-compose.yml`) :

**Grafana :**
- `GF_SECURITY_ADMIN_USER` : utilisateur administrateur (par défaut `admin`)
- `GF_SECURITY_ADMIN_PASSWORD` : mot de passe administrateur (par défaut `admin123`)
- `GF_INSTALL_PLUGINS` : plugins Grafana (ex. `grafana-clickhouse-datasource`)
- `GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH` : dashboard par défaut

**ClickHouse :**
- `CLICKHOUSE_DB` : base de données par défaut (par défaut `bigdata_db`)
- `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT` : gestion d'accès activée

## Architecture de stockage

- `clickhouse-data` : données persistantes de ClickHouse
- `clickhouse-logs` : logs de ClickHouse
- `grafana-data` : données persistantes de Grafana
- `bigdata-net` : réseau Docker partagé

## Arrêter et nettoyer

### Bash / Linux / macOS

```bash
docker-compose down
```

Pour supprimer également les volumes (réinitialise les données) :

```bash
docker-compose down -v
```

### PowerShell (Windows)

```powershell
docker-compose down
```

Pour supprimer également les volumes (réinitialise les données) :

```powershell
docker-compose down -v
```
