import json  # Bibliothèque pour manipuler les données au format JSON
import time  # Bibliothèque pour gérer le temps (utilisée ici pour la simulation)

# ==============================================================================
# CONFIGURATION DES FICHIERS SOURCES
# ==============================================================================
# Les chemins vers nos fichiers de données. 
# Ces fichiers doivent être dans le même dossier que ce script Python.
business_file = 'yelp_academic_dataset_business.json'
review_mock_file = 'mock_review.json'

print("🚀 DÉMARRAGE DE L'INGESTION DES DONNÉES (OFFLINE MOCK)\n" + "="*50)

# ==============================================================================
# PHASE 1 : INGESTION DU FICHIER BUSINESS (Mode Batch)
# ==============================================================================
# Objectif : Lire la liste des entreprises (businesses).
# C'est un fichier de 118 MB, donc la lecture sera très rapide.
print("\n🏢 PHASE 1 : Lecture du fichier Business...")

try:
    business_count = 0  # Compteur pour savoir combien d'entreprises on a lu
    
    # On ouvre le fichier en mode lecture ('r').
    # L'utilisation de "with open" est cruciale : elle garantit que le fichier 
    # sera fermé automatiquement même s'il y a un plantage.
    with open(business_file, 'r', encoding='utf-8') as f_biz:
        
        # Astuce Big Data : On lit le fichier LIGNE PAR LIGNE avec une boucle "for".
        # On ne fait SURTOUT PAS f_biz.read() car cela chargerait tout dans la RAM.
        for line in f_biz:
            # On convertit la ligne textuelle en objet JSON (Dictionnaire Python)
            business = json.loads(line)
            business_count += 1
            
            # Pour ne pas polluer l'écran du terminal, on n'affiche que les 5 premiers.
            # (Dans la vraie version avec Kafka, on enverra tout sans utiliser "print")
            if business_count <= 5:
                nom = business.get('name')
                ville = business.get('city')
                etoiles = business.get('stars')
                print(f"   ✅ [Business {business_count}] {nom} | Ville: {ville} | ⭐ {etoiles}")
                
    # Une fois la boucle terminée, on affiche le nombre total lu.
    print(f"✔️ SUCCÈS : {business_count} entreprises lues avec succès !")

except FileNotFoundError:
    # Si le fichier n'est pas dans le dossier, on attrape l'erreur pour ne pas faire crasher le programme brutalement.
    print(f"❌ Erreur : Le fichier '{business_file}' est introuvable.")
except Exception as e:
    print(f"❌ Erreur inattendue (Business) : {e}")


# ==============================================================================
# PHASE 2 : INGESTION DES REVIEWS (Mode Streaming / Simulation Temps Réel)
# ==============================================================================
# Objectif : Lire notre échantillon (MOCK) de 100 lignes et simuler un flux continu.
print("\n💬 PHASE 2 : Streaming des Reviews (Simulation Live)...")

try:
    # Même principe : ouverture sécurisée et lecture ligne par ligne
    with open(review_mock_file, 'r', encoding='utf-8') as f_rev:
        
        for line in f_rev:
            # Transformation en JSON
            review = json.loads(line)
            
            # Extraction de quelques champs intéressants pour l'affichage
            review_id = review.get('review_id')
            stars = review.get('stars')
            date = review.get('date')
            
            # On affiche la donnée comme si elle venait d'être générée en direct
            print(f"   🔄 [NOUVEL AVIS] 📅 {date} | ⭐ {stars} étoiles | 🆔 {review_id}")
            
            # SIMULATION DE STREAMING : 
            # On demande au programme de faire une pause de 0.5 seconde.
            # Cela imite le comportement des données qui arrivent au compte-gouttes.
            time.sleep(0.5)

    print("\n✔️ SUCCÈS : Fin du flux de données MOCK.")

except FileNotFoundError:
    print(f"❌ Erreur : Le fichier '{review_mock_file}' est introuvable.")
except Exception as e:
    print(f"❌ Erreur inattendue (Reviews) : {e}")

# Fin du programme
print("\n" + "="*50 + "\n🏁 PIPELINE D'INGESTION (TEST LOCAL) TERMINÉ.")