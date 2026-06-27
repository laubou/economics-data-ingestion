from kafka import KafkaProducer
import json
import csv
import time

print("🚀 Starting Batch → Kafka Producer")

FILE_PATH = "data/landing/2m Sales Records.csv"

producer = KafkaProducer(
    bootstrap_servers="127.0.0.1:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("📂 Reading file:", FILE_PATH)

count = 0
MAX_ROWS = 10

with open(FILE_PATH, "r", encoding="utf-8") as file:
    reader = csv.DictReader(file)

    for i, row in enumerate(reader):

        event = {
            "order_id": int(row["Order ID"]),
            "region": row["Region"],
            "country": row["Country"],
            "item_type": row["Item Type"],
            "sales_channel": row["Sales Channel"],
            "priority": row["Order Priority"],
            "order_date": row["Order Date"],
            "ship_date": row["Ship Date"],

            "units_sold": int(float(row["Units Sold"])),
            "unit_price": float(row["Unit Price"]),
            "unit_cost": float(row["Unit Cost"]),

            "total_revenue": float(row["Total Revenue"]),
            "total_cost": float(row["Total Cost"]),
            "total_profit": float(row["Total Profit"])
        }

        producer.send("sales-events", value=event)

        if i % 2 == 0:
            print(f"📤 Sent {i} events...")

        count += 1
        if count >= MAX_ROWS:
            print("🛑 DEV LIMIT reached (10 rows)")
            break

        time.sleep(0.01)  # simulate streaming

producer.flush()

print("✅ Batch ingestion completed")