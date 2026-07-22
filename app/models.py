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

class UserCreate(BaseModel):
    username: str
    password: str

class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    username: str
    role: str

class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"

class TokenRefreshRequest(BaseModel):
    refresh_token: str

class NewAccessTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class LogBatch(BaseModel):
    entries: list[LogEntryCreate]