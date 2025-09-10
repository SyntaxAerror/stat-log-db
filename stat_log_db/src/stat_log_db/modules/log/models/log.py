from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from stat_log_db.modules.base import BaseModel
from stat_log_db.modules.log import LogType, LogLevel


class Log(BaseModel):
    __tablename__ = "log"

    type_id: Mapped[LogType] = mapped_column(ForeignKey("log_type.id"), nullable=False)

    level_id: Mapped[LogLevel] = mapped_column(ForeignKey("log_level.id"), nullable=False)

    message: Mapped[str] = mapped_column(String, nullable=False)
