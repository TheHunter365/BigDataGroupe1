import json
import os
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Configuration du Producer Kafka
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
topic_name = os.getenv("KAFKA_TOPIC", "yelp-reviews-mock")

producer = KafkaProducer(
    bootstrap_servers=[bootstrap_servers],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"🚀 Début de l'envoi des avis Yelp simulés vers le topic '{topic_name}'...")

# List of sample review texts
review_texts = [
    "Amazing food and great service! Highly recommend.",
    "The place was okay, but the service was slow.",
    "Terrible experience. Will not visit again.",
    "Loved the ambiance and the food was delicious.",
    "Mediocre at best. Not worth the price.",
    "Fantastic! Best restaurant in town.",
    "The coffee was great, but the pastries were stale.",
    "A hidden gem! Will definitely come back.",
    "Overrated and overpriced.",
    "Good food, but the portions were small."
]

while True:
    # Create a diverse mock review
    mock_review = {
        "review_id": f"rev_{random.randint(1000, 9999)}",
        "user_id": f"usr_{random.randint(100, 999)}",
        "business_id": random.choice(["restau_A", "restau_B", "cafe_C", "bar_D", "diner_E", "bistro_F"]),
        "stars": random.choice([1.0, 2.0, 3.0, 4.0, 5.0]),
        "useful": random.randint(0, 20),
        "funny": random.randint(0, 10),
        "cool": random.randint(0, 10),
        "text": random.choice(review_texts),
        "review_length": random.randint(50, 300),
        "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Send to Kafka
    producer.send(topic_name, mock_review)
    print(f"Envoyé : {mock_review['business_id']} - {mock_review['stars']} étoiles")

    # Pause of 2 seconds between each review
    time.sleep(2)