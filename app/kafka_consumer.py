import asyncio
import json
import datetime
import os
import logging
import uuid
import boto3
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from app.orm_models import LogEntryORM
from app.logging_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
BUCKET = os.getenv("S3_BUCKET", "")

engine = create_async_engine(DATABASE_URL)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)
s3 = boto3.client("s3")

async def archive_to_dlq_s3(offset: int, payload: str, error_message: str) -> None:
    if not BUCKET:
        logger.error("S3 bucket not configured for DLQ archiving", extra={"offset": offset})
        return

    key = f"dlq/{datetime.datetime.utcnow().strftime('%Y-%m-%d')}/{uuid.uuid4()}.json"
    dlq_payload = {
        "offset": offset,
        "error": error_message,
        "payload": payload,
    }

    def _put_object(bucket: str, key: str, body: bytes, content_type: str):
        return s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            _put_object,
            BUCKET,
            key,
            json.dumps(dlq_payload).encode("utf-8"),
            "application/json",
        )
        logger.info("Archived message to DLQ S3 bucket", extra={"offset": offset, "s3_key": key})
    except Exception as err:
        logger.error("Failed to archive message to DLQ S3", extra={"offset": offset, "error": str(err)})

retry_counts: dict[int, int] = {}


async def process_db_batch(messages: list) -> list:
    valid_db_records = []
    failed_messages = []

    for msg in messages:
        try:
            data = json.loads(msg.value.decode("utf-8"))
            valid_db_records.append({
                "level": data["level"].lower(),
                "message": data["message"],
                "timestamp": datetime.datetime.fromisoformat(data["timestamp"]),
                "msg_object": msg  # Keep reference in case database commit fails below
            })
        except Exception as parse_err:
            # Immediate isolation of malformed JSON text arrays to avoid blocking the whole batch
            logger.error("JSON payload parsing failed, immediately routing to DLQ", extra={"offset": msg.offset})
            await archive_to_dlq_s3(msg.offset, msg.value.decode("utf-8", errors="replace"), str(parse_err))

    if not valid_db_records:
        return []

    # Second Pass: Perform high-throughput bulk micro-batch insertion
    try:
        async with SessionLocal() as db:
            await db.execute(
                insert(LogEntryORM),
                [
                    {
                        "level": r["level"], 
                        "message": r["message"], 
                        "timestamp": r["timestamp"]
                    } 
                    for r in valid_db_records
                ]
            )
            await db.commit()
            
        logger.info("Successfully committed micro-batch to database", extra={"batch_size": len(valid_db_records)})
        for r in valid_db_records:
            retry_counts.pop(r["msg_object"].offset, None)
            
    except Exception as db_err:
        logger.error("Database bulk insertion failed. Splitting batch down to isolate tracking dependencies.", extra={"error": str(db_err)})
        for r in valid_db_records:
            failed_messages.append(r["msg_object"])

    return failed_messages

async def consume():
    consumer = AIOKafkaConsumer(
        "log-events",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="log-writer",
        auto_offset_reset="earliest",
        enable_auto_commit=True  # Automatically registers completed batch offsets back to broker
    )
    
    while True:
        try:
            await consumer.start()
            logger.info("Durable micro-batching database consumer successfully started.")
            break
        except KafkaConnectionError:
            logger.warning("Waiting for Kafka broker initialization... Retrying in 3 seconds.")
            await asyncio.sleep(3)
            
    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=100)
            
            active_batch = []
            for topic_partition, messages in result.items():
                for msg in messages:
                    active_batch.append(msg)
                    
            if not active_batch:
                continue

            failed_elements = await process_db_batch(active_batch)
            
            for msg in failed_elements:
                offset = msg.offset
                retry_counts[offset] = retry_counts.get(offset, 0) + 1
                
                if retry_counts[offset] >= 3:
                    logger.critical("Message exhausted max processing retry allowances. Evicting to DLQ.", extra={"offset": offset})
                    await archive_to_dlq_s3(offset, msg.value.decode("utf-8", errors="replace"), "Max database retry limit exceeded")
                    retry_counts.pop(offset, None)
                else:
                    logger.warning(
                        "Message write failed. Re-evaluating on next polling frame loop context.", 
                        extra={"offset": offset, "attempt": retry_counts[offset]}
                    )
                    await asyncio.sleep(0.5)
    except Exception as e:
        logger.critical("Consumer crashed", extra={"error": str(e), "type": type(e).__name__})
        raise
                    
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(consume())