import json
from aiokafka import AIOKafkaProducer
import os
import asyncio
from aiokafka.errors import KafkaConnectionError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

_producer: AIOKafkaProducer | None = None

async def get_producer() -> AIOKafkaProducer:
    global _producer
    if _producer is None:
        _producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        for attempt in range(10):
            try:
                await _producer.start()
                print("Producer started successfully and warmed up!")
                break
            except KafkaConnectionError:
                print(f"Kafka not ready for producer, retrying in 5s... ({attempt+1}/10)", flush=True)
                await asyncio.sleep(5)
        else:
            print("Could not connect producer to Kafka after 10 attempts, exiting.", flush=True)
            raise RuntimeError("Kafka initialization failed")
    return _producer

async def stop_producer():
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None