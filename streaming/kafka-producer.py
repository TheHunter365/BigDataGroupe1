# ===========================================================================
# PIPELINE D'INGESTION KAFKA (PRODUCER) - ÉQUIPE SE2 - FULL DATASET
# ===========================================================================

import json
import os
from kafka import KafkaProducer
# 1. Mapping des 5 fichiers Yelp vers leurs Topics Kafka respectifs
FILES_TOPICS = {
    '/app/dataset/yelp_academic_dataset_business.json': 'topic_business',
    '/app/dataset/yelp_academic_dataset_review.json':   'topic_reviews',
    '/app/dataset/yelp_academic_dataset_user.json':     'topic_users',
    '/app/dataset/yelp_academic_dataset_tip.json':      'topic_tips',
    '/app/dataset/yelp_academic_dataset_checkin.json':  'topic_checkins'
}

print("🚀 DÉMARRAGE DE L'INGESTION MASSIVE (FULL DATASET)...\n" + "="*50)

# ===========================================================================
# INITIALISATION DU PRODUCER (MODE HAUTE PERFORMANCE)
# ===========================================================================
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'], 
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        
        # --- CONFIGURATION BIG DATA (BATCHING) ---
        batch_size=131072,   # 128 KB pour de meilleurs débits
        linger_ms=20,        # Optimisation du débit
        compression_type='lz4' 
    )
    print("✅ Connecté au Broker Kafka avec succès !")
except Exception as e:
    print(f"❌ Erreur critique : Impossible de se connecter à Kafka.\n Détails : {e}")
    exit()

# ===========================================================================
# LECTURE ET INGESTION DES 5 FICHIERS
# ===========================================================================

for file_path, topic_name in FILES_TOPICS.items():
    print(f"\n📂 Traitement en cours : {file_path}")
    
    # Vérification robuste de l'existence du fichier
    if not os.path.exists(file_path):
        print(f"⚠️ AVERTISSEMENT : Le fichier '{file_path}' est introuvable. On passe au suivant.")
        continue

    try:
        count = 0
        error_count = 0
        
        # Lecture ligne par ligne pour économiser la mémoire (Streaming)
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                if not line.strip(): continue # Sauter les lignes vides
                try:
                    data = json.loads(line, strict=False)
                    producer.send(topic_name, value=data)
                    count += 1
                    
                    if count % 100000 == 0:
                        print(f"   ⏳ Progression : {count:,} messages envoyés pour '{topic_name}'...")
                        
                except json.JSONDecodeError:
                    error_count += 1
                    continue

        producer.flush()
        print(f"✔️ SUCCÈS : {count:,} messages ingérés dans '{topic_name}'.")
        
        if error_count > 0:
            print(f"   ⚠️ Lignes corrompues ignorées : {error_count:,}")

    except Exception as e:
        print(f"❌ ERREUR lors de la lecture de '{file_path}' : {e}")

print("\n INGESTION TERMINÉE AVEC SUCCÈS !")
producer.close()