import json
import logging
import os
import signal
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaProducer

LOGGER = logging.getLogger("producer")


def optional_env(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def build_interval_seconds() -> float:
    raw_value = optional_env("PRODUCER_INTERVAL_SECONDS", "5000")
    try:
        interval = float(raw_value)
    except ValueError as error:
        raise RuntimeError(
            "PRODUCER_INTERVAL_SECONDS must be a number"
        ) from error

    if interval <= 0:
        raise RuntimeError(
            "PRODUCER_INTERVAL_SECONDS must be greater than zero"
        )

    return interval


def build_registration(now: datetime | None = None) -> dict[str, str]:
    timestamp = (now or datetime.now(timezone.utc)).isoformat()
    return {"_id": str(uuid.uuid4()), "registered_at": timestamp}


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=optional_env(
            "KAFKA_BOOTSTRAP_SERVERS",
            "127.0.0.1:9094",
        ),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    producer = build_producer()
    topic = optional_env("KAFKA_TOPIC", "user-registrations")
    interval = build_interval_seconds()
    stop_event = threading.Event()

    def stop(_signum: int, _frame: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    LOGGER.info("Producer started for topic %s", topic)
    try:
        while not stop_event.is_set():
            registration = build_registration()
            producer.send(topic, registration)
            producer.flush()
            LOGGER.info("Published registration %s", registration["_id"])
            stop_event.wait(interval)
    finally:
        producer.close()
        LOGGER.info("Producer stopped")


if __name__ == "__main__":
    main()
