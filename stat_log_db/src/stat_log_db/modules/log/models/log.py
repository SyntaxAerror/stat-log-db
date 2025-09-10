from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from stat_log_db.modules.base import BaseModel
from stat_log_db.modules.log import LogType, LogLevel


class Log(BaseModel):
    __tablename__ = "log"

    type_id: Mapped[int] = mapped_column(ForeignKey("log_type.id"), nullable=False)
    type: Mapped[LogType] = relationship()

    level_id: Mapped[int] = mapped_column(ForeignKey("log_level.id"), nullable=False)
    level: Mapped[LogLevel] = relationship()

    message: Mapped[str] = mapped_column(String, nullable=False)
