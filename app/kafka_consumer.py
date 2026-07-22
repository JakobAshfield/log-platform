import asyncio
import json
import datetime
import os
from aiokafka import AIOKafkaConsumer
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.orm_models import LogEntryORM
from app.database import Base

DATABASE_URL = os.getenv("DATABASE_URL")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

engine = create_async_engine(DATABASE_URL)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

async def consume():
    consumer = AIOKafkaConsumer(
        "log-events",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="log-writer",
        auto_offset_reset="earliest",
    )
    for attempt in range(10):
        try:
            await consumer.start()
            break
        except Exception as e:
            print(f"Kafka not ready, retrying in 5s... ({attempt+1}/10)")
            await asyncio.sleep(5)
    else:
        print("Could not connect to Kafka after 10 attempts, exiting.")
        return
    print("Consumer started, waiting for messages...")
    try:
        async for msg in consumer:
            data = json.loads(msg.value)
            async with SessionLocal() as db:
                entry = LogEntryORM(
                    level=data["level"],
                    message=data["message"],
                    timestamp=datetime.datetime.fromisoformat(data["timestamp"]),
                )
                db.add(entry)
                await db.commit()
            print(f"Wrote log: {data['level']} — {data['message']}")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())