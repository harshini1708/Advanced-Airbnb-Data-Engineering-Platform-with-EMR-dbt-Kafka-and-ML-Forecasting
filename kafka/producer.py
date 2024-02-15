import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

neighborhoods = ['Mission', 'SoMa', 'Tenderloin', 'Castro', 'Sunset']

while True:
    message = {
        "listing_id": random.randint(1000, 9999),
        "neighborhood": random.choice(neighborhoods),
        "price": round(random.uniform(80, 350), 2),
        "timestamp": datetime.now().isoformat()
    }

    producer.send("airbnb_bookings", message)
    print(f"Sent: {message}")
    time.sleep(5)