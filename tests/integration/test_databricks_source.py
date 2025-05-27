"""Integration tests for the Databricks source implementation."""

from datetime import datetime
from unittest.mock import patch, call, MagicMock
import pytest
import pandas as pd

from feature_platform.sources.databricks import DatabricksSource, DeltaSourceConfig


def test_databricks_source_initialization(sample_source_config):
    """Test initializing a Databricks source with configuration."""
    source = DatabricksSource(sample_source_config)
    
    assert source.config.name == "test_source"
    assert source.config.entity == "transaction"
    assert source.config.timestamp_column == "event_time"
    assert source.config.fields == ["transaction_id", "amount", "customer_id"]


def test_get_full_table_name(sample_source_config):
    """Test generating a fully qualified table name."""
    source = DatabricksSource(sample_source_config)
    assert source.config.get_full_table_name() == "test_catalog.test_schema.test_table"
    
    # Test with different location formats
    config = DeltaSourceConfig(
        name="test",
        entity="test",
        type="delta",
        location="schema.table"
    )
    assert config.get_full_table_name() == "schema.table"
    
    config.location = "table"
    assert config.get_full_table_name() == "table"


def test_get_sql_where_clause(sample_source_config):
    """Test generating SQL WHERE clauses with time filters."""
    source = DatabricksSource(sample_source_config)
    start_time = datetime(2023, 1, 1)
    end_time = datetime(2023, 1, 2)
    
    # Test with both start and end time
    where_clause, params = source.config.get_sql_where_clause(start_time, end_time)
    assert "event_time >= %(start_time)s" in where_clause
    assert "event_time < %(end_time)s" in where_clause
    assert params["start_time"] == start_time
    assert params["end_time"] == end_time
    
    # Test with only start time
    where_clause, params = source.config.get_sql_where_clause(start_time=start_time)
    assert "event_time >= %(start_time)s" in where_clause
    assert "event_time < %(end_time)s" not in where_clause
    
    # Test with only end time
    where_clause, params = source.config.get_sql_where_clause(end_time=end_time)
    assert "event_time >= %(start_time)s" not in where_clause
    assert "event_time < %(end_time)s" in where_clause


def test_validate_source(sample_source_config, mock_databricks_connection):
    """Test validating a source configuration."""
    mock_conn, mock_cursor = mock_databricks_connection
    
    # Configure the mock cursor to return a result for DESCRIBE TABLE
    mock_cursor.description = [("col_name",), ("data_type",)]
    mock_cursor.fetchall.return_value = [
        ("transaction_id", "bigint"),
        ("amount", "double"),
        ("customer_id", "bigint")
    ]
    
    source = DatabricksSource(sample_source_config)
    assert source.validate() is True
    
    # Verify the connection was established
    mock_conn.cursor.assert_called_once()
    mock_cursor.execute.assert_called_once()


def test_read_data(sample_source_config, mock_databricks_connection):
    """Test reading data from the source."""
    mock_conn, mock_cursor = mock_databricks_connection
    
    # Mock the query results
    mock_cursor.description = [("transaction_id",), ("amount",), ("customer_id",)]
    mock_cursor.fetchall.return_value = [
        (1, 100.0, 101),
        (2, 200.0, 102)
    ]
    
    source = DatabricksSource(sample_source_config)
    df = source.read()
    
    # Verify the results
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["transaction_id", "amount", "customer_id"]
    
    # Verify the query was executed with the correct SQL
    # Should be called twice: once for validation, once for the actual query
    assert mock_cursor.execute.call_count == 2
    # Verify the actual query was called with the right SQL
    query_calls = [
        call('DESCRIBE TABLE test_catalog.test_schema.test_table LIMIT 1', parameters={}),
        call('\n            SELECT transaction_id, amount, customer_id\n            FROM test_catalog.test_schema.test_table\n            WHERE date = %(date)s\n            \n        ', parameters={'date': '2023-01-01'})
    ]
    mock_cursor.execute.assert_has_calls(query_calls, any_order=False)
    sql = mock_cursor.execute.call_args[0][0]
    assert "SELECT transaction_id, amount, customer_id" in sql
    assert "FROM test_catalog.test_schema.test_table" in sql


def test_get_schema(sample_source_config, mock_databricks_connection):
    """Test retrieving the schema of a table."""
    mock_conn, mock_cursor = mock_databricks_connection
    
    # Mock the DESCRIBE TABLE results
    mock_cursor.description = [("col_name",), ("data_type",)]
    mock_cursor.fetchall.return_value = [
        ("transaction_id", "bigint"),
        ("amount", "double"),
        ("customer_id", "bigint")
    ]
    
    source = DatabricksSource(sample_source_config)
    schema = source.get_schema()
    
    # Verify the schema
    assert schema == {
        "transaction_id": "bigint",
        "amount": "double",
        "customer_id": "bigint"
    }
    
    # Verify the query
    # Should be called twice: once for validation, once for the schema query
    assert mock_cursor.execute.call_count == 2
    # Verify the schema query was called
    schema_calls = [
        call('DESCRIBE TABLE test_catalog.test_schema.test_table LIMIT 1', parameters={}),
        call('DESCRIBE TABLE test_catalog.test_schema.test_table', parameters={})
    ]
    mock_cursor.execute.assert_has_calls(schema_calls, any_order=False)
    sql = mock_cursor.execute.call_args[0][0]
    assert "DESCRIBE TABLE test_catalog.test_schema.test_table" in sql


def test_close_connection(sample_source_config, mock_databricks_connection):
    """Test closing the connection to Databricks."""
    mock_conn, _ = mock_databricks_connection
    
    source = DatabricksSource(sample_source_config)
    
    # Trigger connection creation
    source._get_connection()
    
    # Close the connection
    source.close()
    
    # Verify the connection was closed
    mock_conn.close.assert_called_once()
    assert source._connection is None
