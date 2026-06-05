import os
import json
import re
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, 
    KafkaOffsetsInitializer, 
    KafkaSink, 
    KafkaRecordSerializationSchema, 
    DeliveryGuarantee
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time

def analyze_sentiment(text, stars=None):
    """
    Perform computationally cheap but highly accurate rule-based sentiment analysis.
    Handles negation, common strong/moderate positive/negative keywords, and
    correlates with Yelp star ratings if available for maximum accuracy.
    """
    # Hybrid heuristic: Star ratings are explicit signals of sentiment
    if stars is not None:
        if stars <= 2.0:
            return "negative"
        elif stars >= 4.0:
            return "positive"
        # 3.0 stars is neutral, let's analyze the text to refine it

    if not text or not isinstance(text, str):
        return "neutral"

    # Extract words and clause-level punctuation as separate tokens
    tokens = re.findall(r"[a-z']+|[.,!;?]", text.lower())
    if not tokens:
        return "neutral"

    # Lexicon Definition
    negations = {
        "not", "no", "never", "neither", "nor", "none", "nothing",
        "didnt", "wasnt", "isnt", "werent", "havent", "hadnt", "dont",
        "cant", "couldnt", "shouldnt", "wouldnt", "wont", "without",
        "lack", "lacked", "hardly", "barely", "rarely",
        # Support versions with apostrophes
        "didn't", "wasn't", "isn't", "weren't", "haven't", "hadn't", "don't",
        "can't", "couldn't", "shouldn't", "wouldn't", "won't"
    }

    # If any of these tokens are encountered during negation lookahead, stop the lookahead
    clause_boundaries = {
        ".", ",", "!", "?", ";", "and", "but", "yet", "or", "so", 
        "though", "although", "however", "then", "because"
    }
    
    strong_neg = {
        "terrible", "horrible", "awful", "worst", "disgusting", "dirty",
        "rude", "overpriced", "disappointing", "avoid", "unacceptable",
        "useless", "waste", "poisoning", "roach", "roaches", "disaster", "vomit",
        "nasty", "gross", "garbage", "trash", "badly", "fail", "failed"
    }
    
    mod_neg = {
        "bad", "poor", "slow", "cold", "stale", "tasteless", "burnt",
        "raw", "mean", "dislike", "unfortunately", "sadly", "hate",
        "hated", "expensive", "dry", "bland", "limit", "limited", "wait",
        "waited", "waiting", "boring", "mediocre", "rude", "poorly", "bug", "bugs"
    }
    
    strong_pos = {
        "excellent", "amazing", "great", "fantastic", "wonderful", "love",
        "loved", "perfect", "delicious", "awesome", "outstanding", "favorite",
        "best", "superb", "highly", "incredible", "incredibly", "phenomenal"
    }
    
    mod_pos = {
        "good", "nice", "fresh", "clean", "tasty", "friendly", "happy",
        "enjoyed", "like", "liked", "pleasant", "recommend", "recommended",
        "polite", "attentive", "warm", "worth", "glad"
    }

    score = 0.0
    n = len(tokens)
    i = 0
    while i < n:
        token = tokens[i]
        
        # Skip pure punctuation tokens for direct scoring, they only serve as boundaries
        if token in {".", ",", "!", "?", ";"}:
            i += 1
            continue

        # Check for negation
        if token in negations:
            negated = False
            # Check up to 3 tokens ahead (e.g. "not very clean", "not good")
            lookahead = min(4, n - i)
            for j in range(1, lookahead):
                next_token = tokens[i + j]
                # If we cross a clause boundary, terminate the negation lookup immediately
                if next_token in clause_boundaries:
                    break
                
                if next_token in strong_pos:
                    score -= 2.0  # e.g., "not amazing" -> strong negative
                    i += j
                    negated = True
                    break
                elif next_token in mod_pos:
                    score -= 1.0  # e.g., "not good" -> negative
                    i += j
                    negated = True
                    break
                elif next_token in strong_neg or next_token in mod_neg:
                    score += 1.0  # e.g., "not bad" -> positive
                    i += j
                    negated = True
                    break
            if not negated:
                score -= 0.5  # standalone negation (e.g. "no service")
        elif token in strong_neg:
            score -= 2.0
        elif token in mod_neg:
            score -= 1.0
        elif token in strong_pos:
            score += 2.0
        elif token in mod_pos:
            score += 1.0
        
        i += 1

    # Classify sentiment based on score threshold
    if score < -0.2:
        return "negative"
    elif score > 0.2:
        return "positive"
    else:
        return "neutral"

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # --- CORRECTION 1 : Activer le Checkpointing ---
    # Flink va faire une sauvegarde de son état toutes les 5 secondes (5000 ms)
    # C'est ce qui permet à Kafka UI de voir le "Consumer Group" avancer !
    env.enable_checkpointing(5000)

    connector_jar = os.getenv(
        "FLINK_KAFKA_CONNECTOR_JAR",
        "/opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar",
    )
    env.add_jars(f"file://{connector_jar}")

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    input_topic = os.getenv("KAFKA_INPUT_TOPIC", os.getenv("KAFKA_TOPIC", "topic_reviews"))
    output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "yelp-negative-reviews")

    # 1. SOURCE
    source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics(input_topic) \
        .set_group_id("flink-group-1") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # 2. TRANSFORMATIONS
    def process_review(review_json):
        try:
            review = json.loads(review_json)
            business_id = str(review.get("business_id", "unknown"))
            stars = float(review.get("stars", 0.0))
            text = review.get("text", "")
            date = str(review.get("date", ""))

            sentiment = analyze_sentiment(text, stars)

            return business_id, stars, sentiment, date
        except json.JSONDecodeError:
            return "unknown", 0.0, "error", ""

    processed_stream = stream.map(
        process_review, 
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING()])
    )

    # Flux 1 : Les moyennes (On garde le print pour voir dans la console)
    mapped_for_avg = processed_stream.map(
        lambda x: (x[0], x[1], 1),
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.INT()])
    )
    
    (mapped_for_avg
        .key_by(lambda x: x[0], key_type=Types.STRING())
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .reduce(lambda a, b: (a[0], a[1] + b[1], a[2] + b[2]))
        .map(lambda x: (x[0], x[1] / x[2]), output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()]))
        .print("Moyenne (10s)")
    )

    # Flux 2 : Filtrer les avis négatifs
    negative_reviews = processed_stream.filter(lambda x: x[2] == "negative")

    # --- CORRECTION 2 : Préparer et envoyer vers le Kafka Sink ---
    # Kafka s'attend à recevoir des chaînes de caractères (Strings), pas des Tuples Python.
    # On transforme donc notre Tuple en un joli JSON textuel.
    negative_json_stream = negative_reviews.map(
        lambda x: json.dumps({"business_id": x[0], "stars": x[1], "date": x[3], "alert": "Avis Négatif Détecté"}),
        output_type=Types.STRING()
    )

    # Configuration du Sink Kafka
    serialization_schema = KafkaRecordSerializationSchema.builder() \
        .set_topic(output_topic) \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()

    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_record_serializer(serialization_schema) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    # On branche l'évier (Sink) au bout du tuyau
    negative_json_stream.sink_to(kafka_sink)
    
    # On garde aussi un print pour s'assurer que ça marche côté Docker
    negative_json_stream.print("Vers Kafka SINK >")

    # 3. EXECUTION
    env.execute("Yelp Review Processing - Streaming to Kafka")

if __name__ == '__main__':
    main()