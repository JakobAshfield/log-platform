import asyncio
import datetime
from fastapi import APIRouter, HTTPException, status
from app.models import LogEntry, LogEntryCreate

router = APIRouter()

log_entries: dict[int, LogEntry] = {}
next_id = 1


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=LogEntry)
async def create_log_entry(log_entry: LogEntryCreate): 
    global next_id                                  
    entry = LogEntry(
        id=next_id,
        level=log_entry.level,
        message=log_entry.message,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
    )
    log_entries[next_id] = entry
    next_id += 1
    return entry


@router.get("/", response_model=list[LogEntry])
async def get_logs(level: str | None = None):       
    logs = list(log_entries.values())
    if level:
        logs = [l for l in logs if l.level == level]
    return logs


@router.get("/bulk-fetch", response_model=list[LogEntry])
async def bulk_fetch(ids: str):
    id_list = [int(i) for i in ids.split(",")]

    async def fetch_log(log_id: int) -> LogEntry | None:
        await asyncio.sleep(0.1)
        return log_entries.get(log_id)

    results = await asyncio.gather(*[fetch_log(i) for i in id_list])
    return [r for r in results if r is not None]


@router.get("/slow-fetch", response_model=list[LogEntry])
async def slow_fetch(ids: str):
    id_list = [int(i) for i in ids.split(",")]
    results = []
    for i in id_list:
        await asyncio.sleep(0.1)
        entry = log_entries.get(i)
        if entry:
            results.append(entry)
    return results


@router.get("/{log_id}", response_model=LogEntry)
async def get_log_entry(log_id: int):
    if log_id in log_entries:
        return log_entries[log_id]
    raise HTTPException(status_code=404, detail="Log entry not found")


@router.delete("/{log_id}")
async def delete_log_entry(log_id: int):
    if log_id in log_entries:
        del log_entries[log_id]
        return {"message": "Log entry deleted"}
    raise HTTPException(status_code=404, detail="Log entry not found")