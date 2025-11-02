import json
import io
import uuid
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime, timezone

consumer = KafkaConsumer(
    "mensagens",
    api_version=(3, 8, 0),
    bootstrap_servers="kafka:9093",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="mensagens_consumer_group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
)

if __name__ == "__main__":

    bucket = "mensagens"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    for record in consumer:
        payload = record.value
        line = json.dumps(payload, ensure_ascii=False) + "\n"  # NDJSON
        data_bytes = line.encode("utf-8")
        bio = io.BytesIO(data_bytes)
        length = len(data_bytes)

        now = datetime.now(timezone.utc) # Timestamp UTC seguro para filename e prefixo particionado
        
        ts = now.strftime("%Y%m%dT%H%M%S")
        ms = f"{int(now.microsecond/1000):03d}"
        unique = uuid.uuid4().hex[:8]
        object_name = f"users_{ts}{ms}_{unique}.ndjson"

        client.put_object(
            bucket,
            object_name,
            data=bio,
            length=length,
            content_type="application/x-ndjson",
        )