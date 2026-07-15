import asyncio
import datetime
from fastapi import APIRouter, HTTPException, status, Depends
from app.models import LogEntry, LogEntryCreate
from sqlalchemy.ext.asyncio import AsyncSession 
from sqlalchemy import func, select
from app.orm_models import LogEntryORM
from app.dependencies import get_db

router = APIRouter()

#log_entries: dict[int, LogEntry] = {}
#next_id = 1


@router.post("/", status_code=status.HTTP_201_CREATED, response_model=LogEntry)
async def create_log_entry(log_entry: LogEntryCreate, db: AsyncSession = Depends(get_db)):                                 
    entry = LogEntryORM(
        level=log_entry.level,
        message=log_entry.message,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
    )
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    return LogEntry.model_validate(entry)


@router.get("/", response_model=list[LogEntry])
async def get_logs(level: str | None = None, db: AsyncSession = Depends(get_db)):       
    query = select(LogEntryORM)
    if level:
        query = query.where(LogEntryORM.level == level)
    result = await db.execute(query)
    entries = result.scalars().all()
    return [LogEntry.model_validate(entry) for entry in entries]


@router.get("/bulk-fetch", response_model=list[LogEntry])
async def bulk_fetch(ids: str, db: AsyncSession = Depends(get_db)):
    id_list = [int(i) for i in ids.split(",")]

    async def fetch_log(log_id: int) -> LogEntry | None:
        await asyncio.sleep(0.1)  # Simulate a delay for each fetch
        result = await db.execute(select(LogEntryORM).where(LogEntryORM.id == log_id))
        entry = result.scalar_one_or_none()
        return LogEntry.model_validate(entry) if entry else None

    results = await asyncio.gather(*[fetch_log(i) for i in id_list])
    return [r for r in results if r is not None]


@router.get("/slow-fetch", response_model=list[LogEntry])
async def slow_fetch(ids: str, db: AsyncSession = Depends(get_db)):
    id_list = [int(i) for i in ids.split(",")]
    results = []
    for i in id_list:
        await asyncio.sleep(0.1)
        result = await db.execute(select(LogEntryORM).where(LogEntryORM.id == i))
        entry = result.scalar_one_or_none()
        if entry:
            results.append(LogEntry.model_validate(entry))
    return results


@router.get("/{log_id}", response_model=LogEntry)
async def get_log_entry(log_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(LogEntryORM).where(LogEntryORM.id == log_id))
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Log entry not found")
    return LogEntry.model_validate(entry)


@router.delete("/{log_id}")
async def delete_log_entry(log_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(LogEntryORM).where(LogEntryORM.id == log_id))
    entry = result.scalar_one_or_none()
    if not entry:
        raise HTTPException(status_code=404, detail="Log entry not found")
    await db.delete(entry)
    await db.commit()
    return {"message": "Log entry deleted"}

@router.get("/stats")
async def get_log_stats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(LogEntryORM.level, func.count(LogEntryORM.id)).group_by(LogEntryORM.level))
    stats = {level: count for level, count in result.all()}
    return stats 