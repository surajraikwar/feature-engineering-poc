"""Databricks data source implementation."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union
import logging
import os

import pandas as pd
from databricks import sql
from databricks.sql.client import Connection as DatabricksConnection

from .base import Source, SourceConfig, SourceType
from ..core.registry import EntityRegistry

logger = logging.getLogger(__name__)

@dataclass
class DatabricksConfig:
    """Configuration for Databricks connection."""
    server_hostname: str
    http_path: str
    access_token: str
    catalog: Optional[str] = None
    schema: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> 'DatabricksConfig':
        """Create a DatabricksConfig from environment variables."""
        return cls(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN"),
            catalog=os.getenv("DATABRICKS_CATALOG"),
            schema=os.getenv("DATABRICKS_SCHEMA"),
        )
    
    def get_connection_params(self) -> Dict[str, str]:
        """Get connection parameters for Databricks SQL connector."""
        params = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
            "access_token": self.access_token,
        }
        if self.catalog:
            params["catalog"] = self.catalog
        if self.schema:
            params["schema"] = self.schema
        return params


@dataclass
class DeltaSourceConfig(SourceConfig):
    """Configuration for a Delta table source."""
    timestamp_column: Optional[str] = None
    partition_filters: Optional[Dict[str, str]] = field(default_factory=dict)
    
    def get_full_table_name(self) -> str:
        """Get the fully qualified table name."""
        parts = [part for part in self.location.split('.') if part]
        return '.'.join(parts)
    
    def get_sql_where_clause(
        self, 
        start_time: Optional[datetime] = None, 
        end_time: Optional[datetime] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """Generate a SQL WHERE clause for time-based filtering."""
        conditions = []
        params = {}
        
        if self.timestamp_column and (start_time or end_time):
            if start_time:
                conditions.append(f"{self.timestamp_column} >= %(start_time)s")
                params["start_time"] = start_time
            if end_time:
                conditions.append(f"{self.timestamp_column} < %(end_time)s")
                params["end_time"] = end_time
        
        for column, value in (self.partition_filters or {}).items():
            conditions.append(f"{column} = %({column})s")
            params[column] = value
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return f"WHERE {where_clause}", params


class DatabricksSource(Source[pd.DataFrame]):
    """A source that reads data from a Databricks Delta table."""
    
    def __init__(self, config: Union[Dict[str, Any], DeltaSourceConfig]):
        """Initialize the Databricks Delta source."""
        if isinstance(config, dict):
            config = DeltaSourceConfig(**config)
        super().__init__(config)
        self._connection: Optional[DatabricksConnection] = None
    
    def _get_connection(self) -> DatabricksConnection:
        """Get or create a Databricks SQL connection."""
        if self._connection is None or self._connection.is_closed:
            db_config = DatabricksConfig.from_env()
            self._connection = sql.connect(**db_config.get_connection_params())
        return self._connection
    
    def _execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute a SQL query and return the results as a pandas DataFrame."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query, parameters=params or {})
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Error executing query: {query}")
            raise
        finally:
            cursor.close()
    
    def read(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Read data from the Delta table."""
        self.initialize()
        
        table_name = self.config.get_full_table_name()
        fields = ", ".join(self.config.fields) if self.config.fields else "*"
        
        where_clause, params = self.config.get_sql_where_clause(start_time, end_time)
        params.update(kwargs)
        
        limit_clause = f"LIMIT {limit}" if limit is not None else ""
        
        query = f"""
            SELECT {fields}
            FROM {table_name}
            {where_clause}
            {limit_clause}
        """
        
        logger.debug(f"Executing query: {query} with params: {params}")
        return self._execute_query(query, params)
    
    def validate(self, registry: Optional[EntityRegistry] = None) -> bool:
        """Validate that the source is properly configured and accessible."""
        if registry is not None:
            entity = registry.get_entity(self.config.entity)
            if not entity:
                logger.error(f"Entity '{self.config.entity}' not found in registry")
                return False
        
        try:
            table_name = self.config.get_full_table_name()
            self._execute_query(f"DESCRIBE TABLE {table_name} LIMIT 1")
            return True
        except Exception as e:
            logger.error(f"Failed to access table {table_name}: {str(e)}")
            return False
    
    def get_schema(self) -> Dict[str, str]:
        """Get the schema of the Delta table."""
        self.initialize()
        table_name = self.config.get_full_table_name()
        df = self._execute_query(f"DESCRIBE TABLE {table_name}")
        
        schema = {}
        for _, row in df.iterrows():
            if row['col_name'] and row['data_type'] and not row['col_name'].startswith('#'):
                schema[row['col_name']] = row['data_type']
        
        return schema
    
    def get_latest_timestamp(self) -> Optional[datetime]:
        """Get the latest timestamp from the table."""
        if not self.config.timestamp_column:
            return None
            
        table_name = self.config.get_full_table_name()
        query = f"""
            SELECT MAX({self.config.timestamp_column}) as latest_timestamp
            FROM {table_name}
        """
        
        try:
            result = self._execute_query(query)
            return result.iloc[0]['latest_timestamp']
        except Exception as e:
            logger.error(f"Error getting latest timestamp: {e}")
            return None
    
    def close(self) -> None:
        """Close the connection to Databricks."""
        if self._connection is not None:
            if not self._connection.is_closed:
                self._connection.close()
            self._connection = None
        super().close()
    
    def __del__(self):
        """Ensure the connection is closed when the object is destroyed."""
        self.close()
