import json
import logging
import os
import signal
from typing import Any
from urllib.parse import quote

from kafka import KafkaConsumer
from pymongo import MongoClient

LOGGER = logging.getLogger("consumer")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def optional_env(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def build_mongo_uri() -> str:
    explicit_uri = optional_env("MONGO_URI", "")
    if explicit_uri:
        return explicit_uri

    username = quote(require_env("MONGO_ROOT_USERNAME"), safe="")
    password = quote(require_env("MONGO_ROOT_PASSWORD"), safe="")
    host = optional_env("MONGO_HOST", "localhost")
    port = optional_env("MONGO_PORT", "27017")
    return f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"


def decode_registration(raw_value: bytes) -> dict[str, str] | None:
    try:
        event = json.loads(raw_value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None

    if not isinstance(event, dict):
        return None

    user_id = event.get("_id")
    registered_at = event.get("registered_at")
    if not isinstance(user_id, str) or not user_id:
        return None
    if not isinstance(registered_at, str) or not registered_at:
        return None

    return {"_id": user_id, "registered_at": registered_at}


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        optional_env("KAFKA_TOPIC", "user-registrations"),
        bootstrap_servers=optional_env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "127.0.0.1:9094",
        ),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=optional_env(
            "KAFKA_GROUP_ID",
            "user-registration-consumer",
        ),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    topic = optional_env("KAFKA_TOPIC", "user-registrations")
    mongo_client = MongoClient(
        build_mongo_uri(),
        serverSelectionTimeoutMS=5000,
    )
    collection = mongo_client[
        optional_env("MONGO_DATABASE", "UserRegistrationDB")
    ][optional_env("MONGO_COLLECTION", "User")]
    consumer = build_consumer()
    running = True

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    LOGGER.info("Consumer started for topic %s", topic)
    try:
        while running:
            batches = consumer.poll(timeout_ms=1000)
            for messages in batches.values():
                for message in messages:
                    registration = decode_registration(message.value)
                    if registration is None:
                        LOGGER.warning("Skipped malformed registration")
                        continue

                    collection.update_one(
                        {"_id": registration["_id"]},
                        {"$setOnInsert": registration},
                        upsert=True,
                    )
                    LOGGER.info(
                        "Stored registration %s",
                        registration["_id"],
                    )
    finally:
        consumer.close()
        mongo_client.close()
        LOGGER.info("Consumer stopped")


if __name__ == "__main__":
    main()
