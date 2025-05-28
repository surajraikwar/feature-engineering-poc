"""Pytest configuration and fixtures."""

import os
from pathlib import Path
from typing import Dict, Any

import pytest
from unittest.mock import MagicMock, patch

from domain.core.entity import Entity, Relation
from domain.core.registry import EntityRegistry
from domain.sources.base import SourceConfig, SourceType


@pytest.fixture
def sample_entity() -> Entity:
    """Create a sample entity for testing."""
    return Entity(
        name="customer",
        primary_key="customer_id",
        description="Customer information",
        relations=[
            Relation(
                to_entity="transaction",
                relation_type="one_to_many",
                foreign_key="customer_id"
            )
        ]
    )


@pytest.fixture
def sample_registry(sample_entity: Entity) -> EntityRegistry:
    """Create a sample registry with test entities."""
    registry = EntityRegistry()
    registry.add_entity(sample_entity)
    
    # Add a transaction entity
    transaction_entity = Entity(
        name="transaction",
        primary_key="transaction_id",
        description="Transaction information",
        is_leaf=True
    )
    registry.add_entity(transaction_entity)
    
    return registry


@pytest.fixture
def sample_source_config() -> Dict[str, Any]:
    """Create a sample source configuration."""
    return {
        "name": "test_source",
        "entity": "transaction",
        "type": SourceType.DELTA,
        "location": "test_catalog.test_schema.test_table",
        "timestamp_column": "event_time",
        "fields": ["transaction_id", "amount", "customer_id"],
        "partition_filters": {"date": "2023-01-01"}
    }


@pytest.fixture
def mock_databricks_connection():
    """Create a mock Databricks connection."""
    with patch('databricks.sql.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock the connection and cursor
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock the is_closed property
        type(mock_conn).is_closed = False
        
        yield mock_conn, mock_cursor
        
        # Cleanup
        if mock_conn:
            mock_conn.close()


@pytest.fixture(autouse=True)
def mock_environment_vars():
    """Mock environment variables for testing."""
    env_vars = {
        "DATABRICKS_SERVER_HOSTNAME": "test.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
        "DATABRICKS_TOKEN": "dapi1234567890abcdef1234567890ab",
        "DATABRICKS_CATALOG": "test_catalog",
        "DATABRICKS_SCHEMA": "test_schema"
    }
    
    with patch.dict(os.environ, env_vars):
        yield
