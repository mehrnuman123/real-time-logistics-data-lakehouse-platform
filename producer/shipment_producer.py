import json
import random
import time
import uuid
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
TOPIC_NAME = "shipment_events"

WAREHOUSES = ["Helsinki", "Berlin", "Warsaw", "Amsterdam", "Stockholm"]
DESTINATIONS = ["Paris", "Munich", "Prague", "Copenhagen", "Oslo", "Tallinn"]

STATUS_FLOW = [
    "CREATED",
    "PICKED_UP",
    "IN_TRANSIT",
    "ARRIVED_AT_HUB",
    "OUT_FOR_DELIVERY",
    "DELIVERED"
]

active_shipments = {}


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5
)


def create_new_shipment():
    shipment_id = f"SHP-{random.randint(100000, 999999)}"

    active_shipments[shipment_id] = {
        "shipment_id": shipment_id,
        "driver_id": f"DRV-{random.randint(1000, 9999)}",
        "vehicle_id": f"VEH-{random.randint(100, 999)}",
        "warehouse": random.choice(WAREHOUSES),
        "destination": random.choice(DESTINATIONS),
        "status_index": 0,
        "created_at": datetime.now(timezone.utc),
        "estimated_delivery_minutes": random.randint(120, 1440),
        "is_fragile": random.choice([True, False])
    }

    return active_shipments[shipment_id]


def advance_existing_shipment():
    if not active_shipments or random.random() < 0.35:
        return create_new_shipment()

    shipment_id = random.choice(list(active_shipments.keys()))
    shipment = active_shipments[shipment_id]

    if shipment["status_index"] < len(STATUS_FLOW) - 1:
        shipment["status_index"] += 1
    else:
        del active_shipments[shipment_id]
        return create_new_shipment()

    return shipment


def generate_event():
    shipment = advance_existing_shipment()
    status = STATUS_FLOW[shipment["status_index"]]

    now = datetime.now(timezone.utc)
    elapsed_minutes = int((now - shipment["created_at"]).total_seconds() / 60)

    event = {
        "event_id": str(uuid.uuid4()),
        "shipment_id": shipment["shipment_id"],
        "driver_id": shipment["driver_id"],
        "vehicle_id": shipment["vehicle_id"],
        "warehouse": shipment["warehouse"],
        "destination": shipment["destination"],
        "status": status,
        "temperature_c": round(random.uniform(-10, 25), 2),
        "vehicle_speed_kmh": random.randint(0, 110) if status in ["IN_TRANSIT", "OUT_FOR_DELIVERY"] else 0,
        "fuel_level_pct": random.randint(5, 100),
        "is_fragile": shipment["is_fragile"],
        "estimated_delivery_minutes": shipment["estimated_delivery_minutes"],
        "elapsed_minutes": elapsed_minutes,
        "event_time": now.isoformat()
    }

    return event


def main():
    print("Starting stateful shipment producer...")

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