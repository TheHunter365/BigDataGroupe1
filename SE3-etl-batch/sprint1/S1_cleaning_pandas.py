import pandas as pd

# --- 1. Chargement ---
df = pd.read_csv("mock_yelp_data.csv")
print(f"[INFO] Lignes avant nettoyage : {len(df)}")


# --- 1. Suppression des doublons ---
# Basé sur business_id
avant = len(df)
df = df.drop_duplicates(subset=["business_id"])


# --- 2. Suppression notes invalides ---
# L'échelle Yelp est strictement [0-5]
avant = len(df)
df = df[df["stars"].between(0, 5)]


# --- 3. Suppression GPS manquants ---
# Supprime les lignes où latitude ou longitude sont NaN
avant = len(df)
df = df.dropna(subset=["latitude", "longitude"])


# --- 4. Suppression ville manquante ---
avant = len(df)
df = df.dropna(subset=["city"])
# On s'assure aussi que ce n'est pas une chaîne vide après strip
df = df[df["city"].str.strip() != ""]


# --- 5. Normalisation de casse ville ---
# 'apollo beach' -> 'Apollo Beach' (Title Case)
df["city"] = df["city"].str.title()

# --- 6. Imputation catégories vides ---
# Remplacement des null par 'Unknown'
df["categories"] = df["categories"].fillna("Unknown")

# --- 7. . Conversions de types
df["stars"]        = pd.to_numeric(df["stars"],        errors="coerce")
df["latitude"]     = pd.to_numeric(df["latitude"],     errors="coerce")
df["longitude"]    = pd.to_numeric(df["longitude"],    errors="coerce")
df["review_count"] = pd.to_numeric(df["review_count"], errors="coerce")

# --- 8. Nettoyage espaces superflus ---
# Supprime les espaces au début et à la fin pour les colonnes textuelles clés
for col in ["name", "city", "state"]:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()

# --- Résultat final ---
print(f"\n[INFO] Lignes après nettoyage complet : {len(df)}")

# --- Export ---
df.to_csv("mock_yelp_clean_S1.csv", index=False)
print("[OK] Fichier 'mock_yelp_clean_S1.csv' sauvegardé.")
