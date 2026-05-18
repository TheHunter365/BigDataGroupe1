# SE5 S2 — Livrable M2 (Data Analyst)

**Membre 2 — Data Analyst / BI** · Groupe 1 · SNIA S2 · FSR Rabat
**Sprint :** Semaine 2 — *Pipeline MOCK & Dashboard Fonctionnel*

---

## 🎯 Objectif du sprint (rappel du plan SE5)

> Créer les premiers panneaux dans Grafana. Écrire les requêtes SQL pour
> interroger les tables créées par M1. Afficher les Time Series du flux MOCK.
>
> **Livrable S2 :** Dashboard MOCK terminé — les graphiques s'animent tout
> seuls avec les fausses données.

---

## 📦 Livrable

| Fichier | Rôle |
|---|---|
| `../S2_M1/dashboards/se5-yelp-dashboard.json` | **Le dashboard** (chargé automatiquement par Grafana) |
| `queries.sql` | Les 15 requêtes analytiques, documentées |
| `README.md` | Ce document |

> Le fichier `.json` du dashboard est placé dans `S2_M1/dashboards/` car
> c'est ce dossier qui est monté dans le conteneur Grafana (voir
> `S2_M1/docker-compose.yml`). M1 et M2 partagent la même instance Grafana.

---

## 🚀 Lancer le dashboard (3 étapes)

```bash
# Depuis le dossier S2_M1/
cd ../S2_M1

# 1. Démarrer Kafka + ClickHouse + Grafana et appliquer le pipeline
bash setup-s2.sh

# 2. Installer les dépendances Python puis injecter les données mock.
#    >>> Conseil : générer un VOLUME conséquent pour un dashboard riche <<<
pip install -r requirements.txt
python mock-producer.py --businesses 500 --reviews 1000

# 3. Ouvrir Grafana
#    URL      : http://localhost:3001
#    Login    : admin  /  admin123
#    Dashboard: « SE5 — Dashboard Yelp · Flux MOCK (S2) »
```

> **Sous Windows :** `setup-s2.sh` est un script bash → l'exécuter via
> *Git Bash* ou *WSL*. Sinon, lancer manuellement `docker compose up -d`,
> créer les topics, puis appliquer `kafka-pipeline.sql`.

---

## 📊 Contenu du dashboard (15 panels, 5 sections)

### 📌 Vue d'ensemble — KPIs Clés
| Panel | Type | Mesure |
|---|---|---|
| 🏢 Total Businesses | stat | `count()` sur `businesses_live` |
| 📝 Total Avis | stat | `count()` sur `reviews_live` |
| ⭐ Note Moyenne Businesses | stat | `avg(stars)`, coloré par seuil |
| 💬 Note Moyenne Avis | stat | `avg(stars)`, coloré par seuil |
| 🟢 Taux d'Ouverture | stat | % de businesses `is_open = 1` |
| 🚨 Alertes Négatives | stat | `count()` sur `negative_alerts_live` |

### 📈 Tendances Temporelles
- **Volume d'Avis par Mois** — time series (barres)
- **Note Moyenne par Mois** — time series (ligne, échelle 0–5)

### ⭐ Analyse des Notes
- **Répartition des Notes** — donut (1 à 5 étoiles)
- **Note Moyenne par Ville** — bar chart top 15, colorée par seuil
- **Businesses Ouverts vs Fermés** — pie

### 🏆 Classements & Géographie
- **Top 10 Businesses par Nombre d'Avis** — bar chart
- **Top 10 Villes par Nombre de Businesses** — bar chart

### 📋 Détails & Alertes Temps Réel
- **Derniers Avis Reçus** — table (50 lignes, note colorée)
- **Alertes Négatives Temps Réel (SE4)** — table

---

## 🎨 Choix de design (à présenter en soutenance)

- **Codage couleur cohérent** : la note suit toujours la même échelle
  rouge → orange → jaune → vert (< 2.5 / < 3.5 / < 4 / ≥ 4), sur les KPIs,
  la barre par ville et la table des avis.
- **Lecture en 5 secondes** : les 6 KPIs en haut donnent l'état global ;
  les sections descendent du synthétique vers le détail.
- **Rafraîchissement auto 10 s** → les graphiques s'animent seuls pendant
  l'injection mock (exigence du livrable S2).
- **Infobulles** : chaque panel a une description (icône ⓘ en haut à gauche).

---

## ⚠️ Points d'attention

1. **Plage de temps** — les avis mock sont datés entre **2015 et 2024**.
   Le dashboard est donc pré-réglé sur `2015 → 2025`. Si les time series
   sont vides, vérifier le sélecteur de temps en haut à droite.
2. **Alertes négatives** — la table `negative_alerts_live` reste **vide
   en S2** : ces alertes proviennent du job Flink de SE4, branché en S3.
   Le panel est créé dès maintenant et se remplira tout seul.
3. **Volume de données** — `mock-producer.py` génère 200 businesses /
   100 avis par défaut. Utiliser `--businesses 500 --reviews 1000` pour
   un rendu visuel convaincant.

---

## 📝 À faire en S3 / S4 (suite de la feuille de route M2)

- **S3** : variables interactives (filtre par ville / catégorie),
  alertes Grafana, connexion aux flux réels SE3 / SE4.
- **S4** : peaufinage UX, rédaction de 2–3 *insights* métier,
  vidéo de démo, résumé technique individuel.

### Brouillon d'insights (à compléter après injection des données)

> _Exemples à confirmer une fois le dashboard alimenté :_
> 1. La ville **___** affiche la meilleure note moyenne (___ / 5).
> 2. **___ %** des avis sont des notes ≥ 4 étoiles → satisfaction globale.
> 3. **___ %** des businesses sont fermés (`is_open = 0`).
