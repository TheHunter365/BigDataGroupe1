# ===========================================================================
# PIPELINE D'INGESTION KAFKA (PRODUCER) - ÉQUIPE SE2
# ===========================================================================
# Description : Ce script lit les données réelles du dataset Yelp localement 
# et les envoie vers le cluster Kafka. Il gère deux types d'ingestion :
# 1. Traitement Batch (Envoi rapide des données d'entreprises)
# 2. Traitement Streaming (Simulation temps réel des avis clients)

import json
import time
from pathlib import Path
from kafka import KafkaProducer  # Bibliothèque pour interagir avec Apache Kafka

# Chemins vers les fichiers de données extraits de Kaggle
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data" / "sample"
business_file = DATA_DIR / "yelp_academic_dataset_business.json"
review_mock_file = DATA_DIR / "mock_review.json"

print("🚀 DÉMARRAGE DE L'INGESTION VERS KAFKA...\n" + "="*50)

# ===========================================================================
# INITIALISATION DU PRODUCER KAFKA
# ===========================================================================
try:
    # Création de l'instance Producer
    producer = KafkaProducer(
        # Adresse du Broker Kafka (Configuré dans notre docker-compose)
        bootstrap_servers=['localhost:9092'],
        
        # Sérialisation : Kafka n'accepte que des bytes. Cette fonction lambda 
        # convertit automatiquement nos dictionnaires Python en JSON puis en bytes (utf-8).
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Connecté avec succès au Broker Kafka Local !")
except Exception as e:
    print(f"❌ Erreur critique : Impossible de se connecter à Kafka.\n Détails : {e}")
    exit()  # Arrête le script si Kafka n'est pas accessible


# ===========================================================================
# PHASE 1 : INGESTION DU FICHIER BUSINESS (MODE BATCH)
# ===========================================================================
# Objectif : Envoyer rapidement les données de référence (Les restaurants).
print("\n🏢 PHASE 1 : Envoi des données Business...")

business_count = 0
try:
    with open(business_file, 'r', encoding='utf-8') as f_biz:
        # Lecture ligne par ligne pour éviter de surcharger la mémoire RAM (Optimisation Big Data)
        for line in f_biz:
            business = json.loads(line)  # Conversion du texte en objet JSON
            
            # Envoi du message au topic 'topic_business'. 
            # (Si le topic n'existe pas, Kafka le créera automatiquement)
            producer.send('topic_business', value=business)
            
            business_count += 1
            # Affichage limité aux 5 premiers éléments pour ne pas spammer la console
            if business_count <= 5:
                print(f"   📤 [Envoyé] {business.get('name')} (Ville: {business.get('city')})")

    # producer.flush() force l'envoi immédiat de tous les messages en attente dans le buffer
    producer.flush() 
    print(f"✔️ SUCCÈS : {business_count} entreprises ingérées dans Kafka !")

except FileNotFoundError:
    print(f"❌ Erreur : Fichier '{business_file}' introuvable.")


# ===========================================================================
# PHASE 2 : INGESTION DES REVIEWS (MODE STREAMING TEMPS RÉEL)
# ===========================================================================
# Objectif : Simuler un flux continu d'avis clients pour que l'équipe SE4 
# puisse effectuer des analyses en temps réel (ex: Analyse de sentiment).
print("\n💬 PHASE 2 : Streaming des Reviews...")

try:
    with open(review_mock_file, 'r', encoding='utf-8') as f_rev:
        for line in f_rev:
            review = json.loads(line)
            
            # Envoi du message au topic 'topic_reviews'
            producer.send('topic_reviews', value=review)
            
            # Affichage en direct dans la console pour le suivi
            print(f"   🔄 [Live Review] ⭐ {review.get('stars')} étoiles | ID: {review.get('review_id')}")
            
            # 🎭 DÉCOUPLAGE ET SIMULATION :
            # Pause de 0.5 seconde. Cela imite le comportement d'un site web réel
            # où les avis arrivent au compte-gouttes. Cela permet de tester les 
            # capacités de fenêtrage (windowing) des consommateurs (SE4).
            time.sleep(0.5)

    producer.flush()
    print("\n✔️ SUCCÈS : Fin du streaming de l'échantillon des avis.")

except FileNotFoundError:
    print(f"❌ Erreur : Fichier '{review_mock_file}' introuvable.")

# Fin d'exécution
print("\n" + "="*50 + "\n🏁 PIPELINE D'INGESTION (SE2) TERMINÉ AVEC SUCCÈS !")
