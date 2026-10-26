
import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9094",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

try:
    while True:
        event = {
            "_id": str(uuid.uuid4()),
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }

        producer.send("user-registrations", event)
        producer.flush()

        print(f"Registration sent: {event}")
        time.sleep(5000)

except KeyboardInterrupt:
    print("Producer stopped.")

finally:
    producer.close()
