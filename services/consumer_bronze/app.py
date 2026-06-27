from kafka import KafkaConsumer
import json
import os
import time

print("🚀 Starting Bronze Consumer...")

# ---------------------------
# Kafka connection
# ---------------------------
consumer = KafkaConsumer(
    "sales-events",
    bootstrap_servers="127.0.0.1:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="bronze-consumer-group"
)

print("✅ Connected to Kafka topic: sales-events")

# ---------------------------
# Bronze storage setup
# ---------------------------
BRONZE_PATH = "data/bronze"
os.makedirs(BRONZE_PATH, exist_ok=True)

print(f"📁 Writing Bronze files to: {BRONZE_PATH}")

# ---------------------------
# Consumption loop
# ---------------------------
for message in consumer:
    try:
        event = message.value

        # timestamp for unique file naming
        timestamp = int(time.time() * 1000)
        file_path = f"{BRONZE_PATH}/event_{timestamp}.json"

        # write raw event
        with open(file_path, "w") as f:
            json.dump(event, f)

        print("💾 Saved Bronze event:", event)

    except Exception as e:
        print("❌ Error processing message:", e)