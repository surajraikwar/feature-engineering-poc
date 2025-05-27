"""Integration tests for the Databricks source implementation."""

from datetime import datetime
from unittest.mock import patch, call, MagicMock
import pytest
import pandas as pd

from feature_platform.sources.databricks_sql import DatabricksSQLSource, DeltaSQLSourceConfig


def test_databricks_sql_source_initialization(sample_source_config):
    """Test initializing a Databricks SQL source with configuration."""
    source = DatabricksSQLSource(sample_source_config)
    
    assert source.config.name == "test_source"
    assert source.config.entity == "transaction"
    assert source.config.timestamp_column == "event_time"
    assert source.config.fields == ["transaction_id", "amount", "customer_id"]


def test_get_full_table_name(sample_source_config):
    """Test generating a fully qualified table name."""
    source = DatabricksSQLSource(sample_source_config)
    assert source.config.get_full_table_name() == "test_catalog.test_schema.test_table"
    
    # Test with different location formats
    config = DeltaSQLSourceConfig(
        name="test",
        entity="test",
        type="delta", # Assuming 'type' is a valid field in your SourceConfig base
        location="schema.table"
    )
    assert config.get_full_table_name() == "schema.table"
    
    config.location = "table"
    assert config.get_full_table_name() == "table"


def test_get_sql_where_clause(sample_source_config):
    """Test generating SQL WHERE clauses with time filters."""
    source = DatabricksSQLSource(sample_source_config) # Changed to DatabricksSQLSource
    start_time = datetime(2023, 1, 1)
    end_time = datetime(2023, 1, 2)
    
    # Test with both start and end time
    # Accessing get_sql_where_clause from config object as per definition
    where_clause, params = source.config.get_sql_where_clause(start_time, end_time) 
    assert "event_time >= %(start_time)s" in where_clause
    assert "event_time < %(end_time)s" in where_clause
    assert params["start_time"] == start_time
    assert params["end_time"] == end_time
    
    # Test with only start time
    where_clause, params = source.config.get_sql_where_clause(start_time=start_time) # Access from config
    assert "event_time >= %(start_time)s" in where_clause
    assert "event_time < %(end_time)s" not in where_clause
    
    # Test with only end time
    where_clause, params = source.config.get_sql_where_clause(end_time=end_time) # Access from config
    assert "event_time >= %(start_time)s" not in where_clause
    assert "event_time < %(end_time)s" in where_clause


def test_validate_source(sample_source_config, mock_databricks_connection):
    """Test validating a source configuration."""
    mock_conn, mock_cursor = mock_databricks_connection
    
    # Configure the mock cursor to return a result for a simple SELECT query
    # (used by validate for checking table access)
    mock_cursor.description = [("col1",)] # Dummy description
    mock_cursor.fetchall.return_value = [(1,)]    # Dummy data
    
    source = DatabricksSQLSource(sample_source_config) # Changed to DatabricksSQLSource
    assert source.validate() is True # Validate now uses "SELECT * FROM ... LIMIT 1"
    
    # Verify the connection was established
    mock_conn.cursor.assert_called_once()
    # execute is called by _execute_query which is called by validate
    mock_cursor.execute.assert_called_once_with(
        f"SELECT * FROM {source.config.get_full_table_name()} LIMIT 1", 
        parameters={}
    )


def test_read_data(sample_source_config, mock_databricks_connection):
    """Test reading data from the source."""
    mock_conn, mock_cursor = mock_databricks_connection
    
    # Setup for validate call (first _execute_query)
    validate_description = [("col1",)]
    validate_fetchall = [(1,)]

    # Setup for read call (second _execute_query)
    read_description = [("transaction_id",), ("amount",), ("customer_id",)]
    read_fetchall = [ (1, 100.0, 101), (2, 200.0, 102) ]

    # Configure side_effect for multiple calls to fetchall and description
    mock_cursor.description = validate_description # Initial for validate
    mock_cursor.fetchall.return_value = validate_fetchall # Initial for validate

    def configure_read_call(*args, **kwargs):
        # After validate's query, change description and fetchall for read's query
        if "SELECT transaction_id, amount, customer_id" in args[0]:
            mock_cursor.description = read_description
            mock_cursor.fetchall.return_value = read_fetchall
    
    mock_cursor.execute.side_effect = configure_read_call


    source = DatabricksSQLSource(sample_source_config) # Changed to DatabricksSQLSource
    
    # Construct expected query parts for read()
    expected_fields = ", ".join(sample_source_config.fields)
    expected_table_name = sample_source_config.get_full_table_name()
    # Assuming partition_filters is {"date": "2023-01-01"} from sample_source_config
    # and timestamp_column is "event_time"
    # The actual where clause will depend on how sample_source_config is defined for partition_filters
    # For this test, let's assume the original where clause from the fixture is desired
    # The where clause from the original test was "WHERE date = %(date)s"
    # and params {'date': '2023-01-01'}
    # The new get_sql_where_clause will produce "WHERE date = %(date)s" if partition_filters={"date": "2023-01-01"}
    
    # If sample_source_config.partition_filters = {"date": "2023-01-01"}
    # and sample_source_config.timestamp_column = "event_time" (but read() not passing time filters)
    # then where_clause will be "WHERE date = %(date)s" and params will be {"date": "2023-01-01"}
    
    df = source.read() # This will call validate first, then the actual read query
    
    # Verify the results
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == read_description # Check against the read description
    
    # Verify execute calls
    # Call 1: validate() -> "SELECT * FROM ... LIMIT 1"
    # Call 2: read() -> "SELECT fields FROM ... WHERE ..."
    assert mock_cursor.execute.call_count == 2 
    
    # Detailed check of the read query call
    # Construct the expected query string based on DatabricksSQLSource.read() logic
    # This requires knowing sample_source_config.fields and .partition_filters
    # Assuming sample_source_config.fields = ["transaction_id", "amount", "customer_id"]
    # Assuming sample_source_config.partition_filters = {"date": "2023-01-01"}
    # Assuming sample_source_config.timestamp_column = "event_time" (but not used here)
    
    expected_read_query = (
        f"SELECT {expected_fields} FROM {expected_table_name} "
        f"WHERE date = %(date)s " # Note: trailing space for limit_clause if it were added
    ).strip() # Strip any trailing space if limit is not used
    
    # Check the arguments of the second call to mock_cursor.execute
    # The first call is from validate()
    actual_read_query_call = mock_cursor.execute.call_args_list[1]
    assert actual_read_query_call[0][0] == expected_read_query
    assert actual_read_query_call[1]['parameters'] == {"date": "2023-01-01"}


def test_get_schema(sample_source_config, mock_databricks_connection):
    """Test retrieving the schema of a table."""
    mock_conn, mock_cursor = mock_databricks_connection

    # Setup for validate call (first _execute_query)
    validate_description = [("col1",)]
    validate_fetchall = [(1,)]

    # Setup for get_schema call (second _execute_query for DESCRIBE)
    describe_description = [("col_name",), ("data_type",)]
    describe_fetchall = [
        ("transaction_id", "bigint"),
        ("amount", "double"),
        ("customer_id", "bigint")
    ]
    
    # Configure side_effect for multiple calls
    # Initial state for validate()
    mock_cursor.description = validate_description
    mock_cursor.fetchall.return_value = validate_fetchall

    def configure_describe_call(*args, **kwargs):
        if "DESCRIBE TABLE" in args[0]:
            mock_cursor.description = describe_description
            mock_cursor.fetchall.return_value = describe_fetchall
        # else it's the validate call, which is already set up initially

    mock_cursor.execute.side_effect = configure_describe_call
    
    source = DatabricksSQLSource(sample_source_config) # Changed to DatabricksSQLSource
    schema = source.get_schema() # This calls validate() first, then the DESCRIBE query
    
    # Verify the schema
    assert schema == {
        "transaction_id": "bigint",
        "amount": "double",
        "customer_id": "bigint"
    }
    
    # Verify execute calls
    # Call 1: validate() -> "SELECT * FROM ... LIMIT 1"
    # Call 2: get_schema() -> "DESCRIBE TABLE ..."
    assert mock_cursor.execute.call_count == 2 
    
    expected_describe_query = f"DESCRIBE TABLE {source.config.get_full_table_name()}"
    
    # Check the arguments of the second call to mock_cursor.execute
    actual_describe_query_call = mock_cursor.execute.call_args_list[1]
    assert actual_describe_query_call[0][0] == expected_describe_query
    assert actual_describe_query_call[1]['parameters'] == {}


def test_close_connection(sample_source_config, mock_databricks_connection):
    """Test closing the connection to Databricks."""
    mock_conn, _ = mock_databricks_connection
    
    source = DatabricksSQLSource(sample_source_config) # Changed to DatabricksSQLSource
    
    # Trigger connection creation by calling _get_connection
    # Note: validate() also calls _get_connection, but calling directly is fine for this test
    source._get_connection() 
    
    # Close the connection
    source.close()
    
    # Verify the connection was closed
    mock_conn.close.assert_called_once()
    assert source._connection is None
