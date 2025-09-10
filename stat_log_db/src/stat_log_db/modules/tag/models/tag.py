from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from stat_log_db.modules.base import BaseModel


class Tag(BaseModel):
    __tablename__ = "tag"

    name: Mapped[str] = mapped_column(String, nullable=False)
