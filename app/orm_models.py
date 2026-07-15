import datetime
from sqlalchemy import String, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from app.database import Base

class LogEntryORM(Base):
    __tablename__ = "logs"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    level: Mapped[str] = mapped_column(String(10), index=True)
    message: Mapped[str] = mapped_column(String)
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), index=True)