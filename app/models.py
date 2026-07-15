from pydantic import BaseModel, ConfigDict
from typing import Literal
import datetime


class LogEntryCreate(BaseModel):
    level: Literal["info", "warn", "error"]
    message: str


class LogEntry(LogEntryCreate):
    model_config = ConfigDict(from_attributes=True)
    id: int
    timestamp: datetime.datetime