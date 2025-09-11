# from typing import Any, List, Optional
import uuid
from datetime import datetime

from sqlalchemy import String, TIMESTAMP, func # , ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column # , relationship

# from .exceptions import raise_auto_arg_type_error


class BaseModel(DeclarativeBase):
    __tablename__: str

    id: Mapped[int] = mapped_column(primary_key=True)

    external_id: Mapped[str] = mapped_column(String, unique=True, nullable=False, server_default=str(uuid.uuid4())) # TODO: Look into maintaining uniqueness more safely

    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    def __repr__(self):
        return f"<{self.__class__.__name__}(id={self.id}, external_id={self.external_id})>"
