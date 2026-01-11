import csv
import json
from kafka import KafkaProducer

CSV_PATH = "Electric_Vehicle_Population_Data.csv"
KAFKA_TOPIC = "dev-rt_electric_cars"
KAFKA_BROKER_URL = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

with open(CSV_PATH, encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Convert the single row (dict) to a JSON string
        json_payload = json.dumps(row)
        producer.send(KAFKA_TOPIC, value=json_payload.encode('utf-8'))

print("All rows sent to buffer. Waiting for Kafka to acknowledge...")
producer.flush() # This ensures all rows actually reach the broker
print("Flush complete. Producer exiting.")