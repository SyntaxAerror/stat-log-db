# from typing import Any, List, Optional
from datetime import datetime

from sqlalchemy import TIMESTAMP, func # , ForeignKey, String,
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column # , relationship

# from .exceptions import raise_auto_arg_type_error


class BaseModel(DeclarativeBase):
    __tablename__: str

    id: Mapped[int] = mapped_column(primary_key=True)

    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
