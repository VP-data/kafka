from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'mensagens',
    api_version=(3, 8, 0),
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earlist",
    enable_auto_commit=True,
    group_id="mensagens_consumer_group",
    value_deserializer=lambda x: json.loads(x.encode("uft-8"))
)

if __name__ == "__main__":
    for messagem in consumer:
        print(f"Received: {messagem.value}")
