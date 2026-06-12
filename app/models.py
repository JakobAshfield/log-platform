from pydantic import BaseModel
from typing import Literal
import datetime

class LogEntryCreate(BaseModel):
    level: Literal["info", "warn", "error"]
    message: str


class LogEntry(LogEntryCreate):
    id: int
    timestamp: datetime.datetime