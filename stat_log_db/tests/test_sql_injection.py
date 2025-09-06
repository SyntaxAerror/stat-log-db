"""
Test SQL injection protection in create_table and drop_table methods.
"""

import pytest
import sys
from pathlib import Path

# Add the src directory to the path to import the module
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "stat_log_db" / "src"))

from stat_log_db.db import MemDB


@pytest.fixture
def mem_db():
    """Create a test in-memory database and clean up after tests."""
    sl_db = MemDB({
        "is_mem": True,
        "fkey_constraint": True
    })
    con = sl_db.init_db(True)
    yield con
    # Cleanup
    sl_db.close_db()


class TestSQLInjectionProtection:
    """Test class for SQL injection protection in database operations."""

    def test_malicious_table_name_create(self, mem_db):
        """Test that malicious SQL injection in table names is rejected."""
        with pytest.raises(ValueError, match="Invalid SQL table name"):
            mem_db.create_table("test'; DROP TABLE users; --", [('notes', 'TEXT')], False, True)

    def test_reserved_word_table_name(self, mem_db):
        """Test that SQL reserved words are rejected as table names."""
        with pytest.raises(ValueError, match="is a reserved word"):
            mem_db.create_table("select", [('notes', 'TEXT')], False, True)

    def test_invalid_characters_table_name(self, mem_db):
        """Test that invalid characters in table names are rejected."""
        with pytest.raises(ValueError, match="Invalid SQL table name"):
            mem_db.create_table("test-table", [('notes', 'TEXT')], False, True)

    def test_malicious_column_name(self, mem_db):
        """Test that malicious SQL injection in column names is rejected."""
        with pytest.raises(ValueError, match="Invalid SQL column name"):
            mem_db.create_table("test_table", [('notes\'; DROP TABLE users; --', 'TEXT')], False, True)

    def test_invalid_column_type(self, mem_db):
        """Test that invalid/malicious column types are rejected."""
        with pytest.raises(ValueError, match="Unsupported column type"):
            mem_db.create_table("test_table", [('notes', 'MALICIOUS_TYPE; DROP TABLE users; --')], False, True)

    def test_valid_table_creation(self, mem_db):
        """Test that valid table creation works correctly."""
        # This should not raise any exception
        mem_db.create_table("test_table", [('notes', 'TEXT'), ('count', 'INTEGER')], False, True)
        
        # Verify table was created by attempting to insert data
        mem_db.execute("INSERT INTO test_table (notes, count) VALUES (?, ?);", ("test note", 42))
        mem_db.commit()
        
        # Verify data was inserted
        mem_db.execute("SELECT * FROM test_table;")
        result = mem_db.fetchall()
        assert len(result) == 1
        assert result[0][1] == "test note"  # Column 0 is auto-increment id
        assert result[0][2] == 42

    def test_malicious_drop_table_name(self, mem_db):
        """Test that malicious SQL injection in drop table is rejected."""
        # First create a valid table
        mem_db.create_table("test_table", [('notes', 'TEXT')], False, True)
        
        # Then try to drop with malicious name
        with pytest.raises(ValueError, match="Invalid SQL table name"):
            mem_db.drop_table("test_table'; DROP TABLE sqlite_master; --", False)

    def test_valid_drop_table(self, mem_db):
        """Test that valid table dropping works correctly."""
        # Create a table first
        mem_db.create_table("test_table", [('notes', 'TEXT')], False, True)
        
        # Verify it exists by checking sqlite_master
        mem_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", ("test_table",))
        assert mem_db.fetchone() is not None
        
        # Drop the table
        mem_db.drop_table("test_table", False)
        
        # Verify it's gone
        mem_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", ("test_table",))
        assert mem_db.fetchone() is None

    def test_empty_table_name_create(self, mem_db):
        """Test that empty table names are rejected."""
        with pytest.raises(ValueError, match="cannot be an empty string"):
            mem_db.create_table("", [('notes', 'TEXT')], False, True)

    def test_empty_table_name_drop(self, mem_db):
        """Test that empty table names are rejected in drop operations."""
        with pytest.raises(ValueError, match="cannot be an empty string"):
            mem_db.drop_table("", False)

    def test_empty_column_name(self, mem_db):
        """Test that empty column names are rejected."""
        with pytest.raises(ValueError, match="cannot be empty"):
            mem_db.create_table("test_table", [('', 'TEXT')], False, True)

    def test_column_name_with_numbers(self, mem_db):
        """Test that column names with numbers are allowed."""
        mem_db.create_table("test_table", [('column1', 'TEXT'), ('column_2', 'INTEGER')], False, True)

    def test_table_name_with_underscore(self, mem_db):
        """Test that table names starting with underscore are allowed."""
        mem_db.create_table("_test_table", [('notes', 'TEXT')], False, True)

    def test_valid_column_types(self, mem_db):
        """Test that all supported column types work correctly."""
        valid_types = [
            ('text_col', 'TEXT'),
            ('int_col', 'INTEGER'),
            ('real_col', 'REAL'),
            ('blob_col', 'BLOB'),
            ('numeric_col', 'NUMERIC'),
            ('varchar_col', 'VARCHAR(255)'),
            ('decimal_col', 'DECIMAL(10,2)')
        ]
        
        mem_db.create_table("type_test_table", valid_types, False, True)

    def test_case_insensitive_reserved_words(self, mem_db):
        """Test that reserved words are caught regardless of case."""
        with pytest.raises(ValueError, match="is a reserved word"):
            mem_db.create_table("SELECT", [('notes', 'TEXT')], False, True)
        
        with pytest.raises(ValueError, match="is a reserved word"):
            mem_db.create_table("Select", [('notes', 'TEXT')], False, True)

    def test_raise_if_exists_functionality(self, mem_db):
        """Test the raise_if_exists parameter works correctly."""
        # Create a table
        mem_db.create_table("test_table", [('notes', 'TEXT')], False, True)
        
        # Try to create the same table with raise_if_exists=True (should fail)
        with pytest.raises(ValueError, match="already exists"):
            mem_db.create_table("test_table", [('notes', 'TEXT')], False, True)
        
        # Try to create the same table with raise_if_exists=False (should succeed)
        mem_db.create_table("test_table", [('notes', 'TEXT')], False, False)

    def test_raise_if_not_exists_functionality(self, mem_db):
        """Test the raise_if_not_exists parameter works correctly."""
        # Try to drop non-existent table with raise_if_not_exists=True (should fail)
        with pytest.raises(ValueError, match="does not exist"):
            mem_db.drop_table("nonexistent_table", True)
        
        # Try to drop non-existent table with raise_if_not_exists=False (should succeed)
        mem_db.drop_table("nonexistent_table", False)

    def test_special_characters_rejection(self, mem_db):
        """Test that various special characters are properly rejected."""
        special_chars = [
            "table;name",
            "table'name",
            'table"name',
            "table name",   # space
            "table-name",   # hyphen
            "table.name",   # dot
            "table(name)",  # parentheses
            "table[name]",  # brackets
            "table{name}",  # braces
            "table@name",   # at symbol
            "table#name",   # hash
            "table$name",   # dollar (should be rejected by our implementation)
        ]
        
        for table_name in special_chars:
            with pytest.raises(ValueError, match="Invalid SQL"):
                mem_db.create_table(table_name, [('notes', 'TEXT')], False, True)
