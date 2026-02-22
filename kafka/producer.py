# Write your kafka producer code here
import json
import time
import os
from confluent_kafka import Producer

conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)
    else:
        print(f"✅ Delivered to {msg.topic()} partition {msg.partition()}")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_PATH = os.path.join(BASE_DIR, "..", "data", "events_dirty.json")

print(f"Producer started, reading from: {os.path.abspath(FILE_PATH)}")

with open(FILE_PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        print("--> Producing:", line)
        producer.produce(
            "events_raw",
            value=line.encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(0.5)

producer.flush()
print("✅ All messages sent.")