import os
import json
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
    input_topic = os.getenv("KAFKA_TOPIC", "yelp-reviews-mock")
    output_topic = "yelp-negative-reviews" # Le nouveau topic pour les alertes

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

            text_lower = text.lower()
            if any(word in text_lower for word in ["amazing", "great", "fantastic"]):
                sentiment = "positive"
            elif any(word in text_lower for word in ["terrible", "not", "stale", "overpriced"]):
                sentiment = "negative"
            else:
                sentiment = "neutral"

            return business_id, stars, sentiment
        except json.JSONDecodeError:
            return "unknown", 0.0, "error"

    processed_stream = stream.map(
        process_review, 
        output_type=Types.TUPLE([Types.STRING(), Types.FLOAT(), Types.STRING()])
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
        lambda x: json.dumps({"business_id": x[0], "stars": x[1], "alert": "Avis Négatif Détecté"}),
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