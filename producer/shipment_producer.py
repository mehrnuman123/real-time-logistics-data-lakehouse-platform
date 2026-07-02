import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC_NAME = "shipment_events"

WAREHOUSES = ["Helsinki", "Berlin", "Warsaw", "Amsterdam", "Stockholm"]
DESTINATIONS = ["Paris", "Munich", "Prague", "Copenhagen", "Oslo", "Tallinn"]

STATUSES = [
    "CREATED",
    "PICKED_UP",
    "IN_TRANSIT",
    "ARRIVED_AT_HUB",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
    "FAILED_DELIVERY"
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5
)


def generate_event():
    shipment_id = f"SHP-{random.randint(100000, 999999)}"

    event = {
        "event_id": str(uuid.uuid4()),
        "shipment_id": shipment_id,
        "driver_id": f"DRV-{random.randint(1000, 9999)}",
        "vehicle_id": f"VEH-{random.randint(100, 999)}",
        "warehouse": random.choice(WAREHOUSES),
        "destination": random.choice(DESTINATIONS),
        "status": random.choice(STATUSES),
        "temperature_c": round(random.uniform(-10, 25), 2),
        "vehicle_speed_kmh": random.randint(0, 110),
        "fuel_level_pct": random.randint(5, 100),
        "is_fragile": random.choice([True, False]),
        "estimated_delivery_minutes": random.randint(20, 1440),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    return event


def main():
    print("Starting shipment event producer...")

    while True:
        event = generate_event()

        producer.send(
            TOPIC_NAME,
            key=event["shipment_id"],
            value=event
        )

        print("Produced:", event)

        time.sleep(1)


if __name__ == "__main__":
    main()