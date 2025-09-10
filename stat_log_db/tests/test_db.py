"""
Test SQL injection protection in create_table and drop_table methods.
"""

import pytest
import sys
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from stat_log_db.db import Database
from stat_log_db.modules.tag import Tag


# Add the src directory to the path to import the module
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "stat_log_db" / "src"))


@pytest.fixture
def db():
    """Create a test in-memory database and close it after tests."""
    sl_db = Database({
        "is_mem": True
    })
    sl_db.init_db()
    yield sl_db
    sl_db.close_db()


def test_db_engine_registered(db):
    """ Test database operations """
    assert isinstance(db, Database)
    assert db.engine is not None, "Database engine is not initialized"
    assert isinstance(db.engine, Engine), "Database engine is not of type Engine (sqlalchemy.engine.Engine)"


def test_db_session(db):
    """ Test database session creation """
    with Session(db.engine) as session:
        tag_name = "Test"
        test_tag = Tag(
            name=tag_name
        )
        session.add(test_tag)
        session.commit()
        tags = select(Tag).where(Tag.id == 1)
        results = session.scalars(tags)
        assert (num_res := len(results.all())) == 1, f"Expected 1 result, got {num_res}"


def test_db_basic_class(db):
    """ Test basic class (Tag) """
    with Session(db.engine) as session:
        tag_name = "Test"
        test_tag = Tag(
            name=tag_name
        )
        session.add(test_tag)
        session.commit()
        tags_stmt = select(Tag).where(Tag.id == 1)
        tags = session.scalars(tags_stmt).all()
        assert (num_res := len(tags)) == 1, f"Expected 1 result, got {num_res}"
        assert tags[0].name == tag_name
        assert hasattr(tags[0], "id"), "Tag object does not have an 'id' attribute"
        assert tags[0].id == 1, f"Expected tag id to be 1, got {tags[0].id}"
