from kafka import KafkaProducer
from faker import Faker
import time
import json
import random

fake = Faker('pt_BR')
producer = KafkaProducer(
    bootstrap_servers="kafka:9093",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

def generate_user_data():
    return {
        "user_id": str(random.randint(1, 1_000_000)),
        "nome": fake.name(),
        "cpf": fake.cpf().replace('.', '').replace('-', ''),
        "data_nascimento": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
        "pais": "Brasil",
        "estado": fake.estado_sigla(),
        "bairro": fake.bairro(),
        "rua": fake.street_name(),
        "telefone": fake.phone_number(),
        "email": fake.free_email(),
        "created": fake.date_time().isoformat()
    }

if __name__ == "__main__":
    topic = "mensagens"
    while True:
        data = generate_user_data()
        key = data["user_id"]
        producer.send(topic, key=key, value=data)
        producer.flush()
        time.sleep(1)
