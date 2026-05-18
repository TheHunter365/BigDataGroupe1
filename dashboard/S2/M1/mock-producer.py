#!/usr/bin/env python3
"""
SE5 — Mock Producer (Simulateur SE2 / Yelp Dataset)
Génère des données Yelp fictives (businesses + reviews) et les publie
sur les topics Kafka 'topic_business' et 'topic_reviews'.

Usage:
    pip install -r requirements.txt
    python3 mock-producer.py                        # mode complet
    python3 mock-producer.py --only reviews         # reviews seulement
    python3 mock-producer.py --broker localhost:9092 --rate 5
"""

import argparse
import json
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Windows : force la sortie console en UTF-8 (sinon crash sur les caracteres
# speciaux comme la fleche avec l'encodage cp1252 par defaut).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ── Configuration ─────────────────────────────────────────────
DEFAULT_BROKER   = "localhost:9092"
TOPIC_BUSINESS   = "topic_business"
TOPIC_REVIEWS    = "topic_reviews"

# Données de référence (simulation réaliste du dataset Yelp)
CITIES = [
    ("Philadelphia", "PA"), ("Tucson", "AZ"), ("Tampa", "FL"),
    ("Nashville", "TN"),    ("Charlotte", "NC"), ("Pittsburgh", "PA"),
    ("Montreal", "QC"),     ("Calgary", "AB"),   ("Toronto", "ON"),
]

CATEGORY_POOL = [
    "Restaurants", "Food", "Bars", "Nightlife", "Fast Food",
    "Pizza", "Italian", "Mexican", "Chinese", "Coffee & Tea",
    "Hotels & Travel", "Shopping", "Beauty & Spas", "Health & Medical",
    "Automotive", "Home Services", "Education", "Arts & Entertainment",
]

BUSINESS_NAMES = [
    "The Golden Fork", "Bella Vista", "Pizza Palace", "Sunrise Café",
    "The Daily Grind", "Burger Barn", "Sushi World", "Casa Bonita",
    "The Rusty Nail", "Green Garden", "Blue Moon Bar", "Metro Diner",
    "Lakeview Lounge", "The Noodle House", "Corner Bakery",
]

# ── Générateurs ────────────────────────────────────────────────
def generate_business() -> dict:
    city, state = random.choice(CITIES)
    num_cats = random.randint(1, 4)
    categories = ", ".join(random.sample(CATEGORY_POOL, num_cats))
    stars = round(random.choice([1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]), 1)
    return {
        "business_id":  str(uuid.uuid4()),
        "name":         random.choice(BUSINESS_NAMES) + f" #{random.randint(1, 99)}",
        "address":      f"{random.randint(100, 9999)} Main St",
        "city":         city,
        "state":        state,
        "postal_code":  f"{random.randint(10000, 99999)}",
        "latitude":     round(random.uniform(25.0, 50.0), 6),
        "longitude":    round(random.uniform(-125.0, -65.0), 6),
        "stars":        stars,
        "review_count": random.randint(1, 2000),
        "is_open":      random.choices([1, 0], weights=[0.85, 0.15])[0],
        "categories":   categories,
    }


def generate_review(business_ids: list) -> dict:
    # Date aléatoire entre 2015 et 2024
    base = datetime(2015, 1, 1)
    rand_days = random.randint(0, 365 * 9)
    date = (base + timedelta(days=rand_days)).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "review_id":    str(uuid.uuid4()),
        "user_id":      str(uuid.uuid4()),
        "business_id":  random.choice(business_ids) if business_ids else str(uuid.uuid4()),
        "stars":        float(random.randint(1, 5)),
        "useful":       random.randint(0, 50),
        "funny":        random.randint(0, 20),
        "cool":         random.randint(0, 30),
        "text":         f"Mock review text #{random.randint(1000, 9999)}. "
                        f"Rating reflects overall experience.",
        "date":         date,
    }


# ── Main ───────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="SE2 Mock Producer → Kafka (Yelp format)")
    parser.add_argument("--broker", default=DEFAULT_BROKER)
    parser.add_argument("--businesses", type=int, default=200,
                        help="Nombre de businesses à produire (défaut: 200)")
    parser.add_argument("--reviews", type=int, default=100,
                        help="Nombre de reviews à produire (défaut: 100)")
    parser.add_argument("--rate", type=float, default=2.0,
                        help="Délai entre reviews en secondes (défaut: 0.5s comme SE2)")
    parser.add_argument("--only", choices=["business", "reviews", "both"], default="both")
    args = parser.parse_args()

    try:
        producer = KafkaProducer(
            bootstrap_servers=args.broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
    except NoBrokersAvailable:
        print(f"[ERREUR] Impossible de joindre le broker Kafka sur {args.broker}")
        print("         Lancez d'abord : docker compose up -d kafka")
        raise SystemExit(1)

    business_ids = []

    # ── Phase 1 : Businesses (batch) ──────────────────────────
    if args.only in ("business", "both"):
        print(f"[Phase 1] Envoi de {args.businesses} businesses → '{TOPIC_BUSINESS}' (batch)...")
        for i in range(args.businesses):
            biz = generate_business()
            business_ids.append(biz["business_id"])
            producer.send(TOPIC_BUSINESS, value=biz)
            if (i + 1) % 50 == 0:
                producer.flush()
                print(f"  {i + 1}/{args.businesses} businesses envoyés")
        producer.flush()
        print(f"  Terminé — {args.businesses} businesses dans '{TOPIC_BUSINESS}'.\n")

    # ── Phase 2 : Reviews (streaming simulé, 0.5s entre chaque) ──
    if args.only in ("reviews", "both"):
        delay = 1.0 / args.rate
        print(f"[Phase 2] Envoi de {args.reviews} reviews → '{TOPIC_REVIEWS}' "
              f"(streaming, délai={delay:.2f}s)...")
        for i in range(args.reviews):
            rev = generate_review(business_ids)
            producer.send(TOPIC_REVIEWS, value=rev)
            print(f"  [{i + 1:>4}/{args.reviews}] stars={rev['stars']} "
                  f"business={rev['business_id'][:8]}...")
            time.sleep(delay)
        producer.flush()
        print(f"  Terminé — {args.reviews} reviews dans '{TOPIC_REVIEWS}'.")

    print("\n[Done] Mock producer terminé.")

if __name__ == "__main__":
    main()
