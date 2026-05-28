# ===========================================================================
# PIPELINE D'INGESTION KAFKA (PRODUCER) - ÉQUIPE SE2 - FULL DATASET
# ===========================================================================

import json
from kafka import KafkaProducer

# 1. Mapping des 5 fichiers Yelp vers leurs Topics Kafka respectifs
# Assurez-vous que les 5 fichiers JSON sont dans le même dossier que ce script.
FILES_TOPICS = {
    'yelp_academic_dataset_business.json': 'topic_business',
    'yelp_academic_dataset_review.json':   'topic_reviews',
    'yelp_academic_dataset_user.json':     'topic_users',
    'yelp_academic_dataset_tip.json':      'topic_tips',
    'yelp_academic_dataset_checkin.json':  'topic_checkins'
}

print("🚀 DÉMARRAGE DE L'INGESTION MASSIVE (FULL DATASET)...\n" + "="*50)

# ===========================================================================
# INITIALISATION DU PRODUCER (MODE HAUTE PERFORMANCE)
# ===========================================================================
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'], # Utilisation de l'IP stricte pour Ubuntu
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        
        # --- CONFIGURATION BIG DATA (BATCHING) ---
        batch_size=65536,    # Envoi par paquets de 64 KB pour maximiser le débit
        linger_ms=10,        # Attente maximale de 10ms avant l'envoi d'un paquet
        compression_type='lz4' # Compression des messages pour soulager le réseau
    )
    print("✅ Connecté au Broker Kafka (Mode Haute Performance) !")
except Exception as e:
    print(f"❌ Erreur critique : Impossible de se connecter à Kafka.\n Détails : {e}")
    exit()

# ===========================================================================
# LECTURE ET INGESTION DES 5 FICHIERS
# ===========================================================================

for file_path, topic_name in FILES_TOPICS.items():
    print(f"\n📂 Traitement en cours : {file_path}")
    print(f"   ↳ Destination      : {topic_name}")
    
    try:
        count = 0
        error_count = 0 # Nouveau compteur pour les lignes ignorées
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    # 1. Ajout de strict=False pour tolérer les caractères de contrôle
                    data = json.loads(line, strict=False)
                    
                    producer.send(topic_name, value=data)
                    count += 1
                    
                    if count % 100000 == 0:
                        print(f"   ⏳ Progression : {count:,} messages envoyés...")
                        
                except json.JSONDecodeError:
                    # 2. En cas d'erreur de parsing, on incrémente l'erreur et on passe à la ligne suivante
                    error_count += 1
                    continue

        producer.flush()
        print(f"✔️ SUCCÈS : {count:,} messages ingérés dans '{topic_name}'.")
        
        # Affichage du nombre de lignes corrompues ignorées à la fin
        if error_count > 0:
            print(f"   ⚠️ Lignes corrompues ignorées : {error_count:,}")

    except FileNotFoundError:
        print(f"⚠️ AVERTISSEMENT : Le fichier '{file_path}' est introuvable. On passe au suivant.")
    except Exception as e:
        print(f"❌ ERREUR lors de la lecture de '{file_path}' : {e}")