import os
import time
import random
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
KAFKA_TOPIC_NAME = os.environ.get("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

def generate_vitals():
    heart_rate = random.randint(60, 100)
    temperature = round(random.uniform(36.5, 37.5), 1)
    spo2 = random.randint(95, 100)

    # Introduce anomalies (10% probability)
    if random.random() < 0.1:
        anomaly_type = random.choice(["heart_rate", "temperature", "spo2"])
        if anomaly_type == "heart_rate":
            heart_rate = random.randint(40, 150)  # Simulate very low or high heart rate
        elif anomaly_type == "temperature":
            temperature = round(random.uniform(35, 40), 1)  # Simulate low or high temperature
        else:
            spo2 = random.randint(80, 94)  # Simulate low SpO2

    return {
        "heart_rate": heart_rate,
        "temperature": temperature,
        "spo2": spo2
    }

if __name__ == "__main__":
    while True:
        vitals = generate_vitals()
        print(f"Sending: {vitals}")
        producer.send(KAFKA_TOPIC_NAME, value=str(vitals).encode('utf-8'))
        time.sleep(1)
