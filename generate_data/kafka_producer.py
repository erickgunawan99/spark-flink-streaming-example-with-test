import random
import time
from datetime import datetime, timedelta
from uuid import uuid4

from confluent_kafka import Producer
from faker import Faker
import orjson


# ---------- Config ----------
KAFKA_BROKER = "localhost:9092"   # adjust to your Kafka broker
TOPIC = "user_activity"

EVENT_TYPES = ["liked", "viewed", "bookmarked", "commented"]

fake = Faker()


# ---------- Kafka Setup ----------
conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "user-activity-producer",
}
producer = Producer(conf)


# ---------- Serializer ----------
def to_json_bytes(obj) -> bytes:
    return orjson.dumps(obj)


# ---------- Data Generator ----------
def generate_event():
    # unique but not too long ID (short UUID hex)
    user_id = random.randint(1, 10)

    # random datetime up to "now"
    # random_days = random.randint(0, 30)
    # random_seconds = random.randint(0, 86400)
    date = datetime.now()

    event = {
        "event_type": random.choice(EVENT_TYPES),
        "url": fake.url(),
    }

    return {
        "id": str(user_id),
        "date": int(date.timestamp() * 1000),  # ISO-8601 timestamp
        "event": event,
    }


# ---------- Delivery Callback ----------
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


# ---------- Main Producer Loop ----------
if __name__ == "__main__":
    try:
        while True:
            record = generate_event()
            payload = to_json_bytes(record)

            producer.produce(
                TOPIC,
                value=payload,
                callback=delivery_report
            )

            # Trigger delivery callbacks
            producer.poll(0)
            # Control event rate
            time.sleep(0.8)

    except KeyboardInterrupt:
        print("Stopping producer...")

    finally:
        producer.flush()
