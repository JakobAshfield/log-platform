import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
async def alert(data: dict):
#change to webhook
    print(f"ALERT: error log received — {data['message']}")

async def consume_alerts():
    consumer = AIOKafkaConsumer(
        "log-events",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="alert-system",  
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            data = json.loads(msg.value)
            if data.get("level") == "error":
                await alert(data)
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_alerts())