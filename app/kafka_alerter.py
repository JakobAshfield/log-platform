import asyncio
import json
import os
import logging
from aiokafka import AIOKafkaConsumer

from app.logging_config import setup_logging
setup_logging()

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


async def alert(data: dict):
    logger.warning("Error log alert triggered", extra={"log_message": data["message"]})

async def consume_alerts():
    consumer = AIOKafkaConsumer(
        "log-events",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="alert-system",  
        auto_offset_reset="earliest",
    )
    
    for attempt in range(10):
        try:
            await consumer.start()
            break
        except Exception as e:
            logger.warning(
                "Kafka broker initialization pending. Alerter retrying connection...",
                extra={"attempt": attempt + 1, "max_attempts": 10, "error": str(e)}
            )
            await asyncio.sleep(5)
    else:
        logger.critical("Aborting alerter monitor instantiation. Max connection limits exhausted.")
        return

    logger.info("Real-time system monitoring alerter worker connected and listening.")
    
    try:
        async for msg in consumer:
            data = json.loads(msg.value)
            if data.get("level") == "error":
                await alert(data)
    except Exception as e:
        logger.critical("alerter failed",  extra={"error": str(e), "type": type(e).__name__})
        raise
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume_alerts())
