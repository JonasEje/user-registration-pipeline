
import json
import os

from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9094"
KAFKA_TOPIC = "user-registrations"

MONGO_URI = (
    "mongodb://projectadmin:LocalDev"
    "@localhost:27017/?authSource=admin"
)

mongo_client = MongoClient(MONGO_URI)
database = mongo_client["UserRegistrationDB"]
users = database["User"]

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="user-registration-consumer",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print("Consumer gestartet. Warte auf Registrierungen...")

try:
    for message in consumer:
        event = message.value
        user_id = event.get("_id")

        if not user_id:
            print("Nachricht ohne User-ID übersprungen:", event)
            continue

        users.update_one(
            {"_id": user_id},
            {"$setOnInsert": event},
            upsert=True,
        )

        print(f"User verarbeitet: {user_id}")

except KeyboardInterrupt:
    print("Consumer wird beendet.")

finally:
    consumer.close()
    mongo_client.close()
