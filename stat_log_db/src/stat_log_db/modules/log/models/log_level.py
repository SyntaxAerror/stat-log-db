from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from stat_log_db.modules.base import BaseModel

class LogLevel(BaseModel):
    __tablename__ = "log_level"

    name: Mapped[str] = mapped_column(String, nullable=False)
