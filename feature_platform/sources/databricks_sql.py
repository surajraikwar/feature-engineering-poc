# feature_platform/sources/databricks_sql.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union # Keep Union for config flexibility
import logging
import os

import pandas as pd
from databricks import sql # Keep this specific import
from databricks.sql.client import Connection as DatabricksConnection # Keep this

from .base import Source, SourceConfig # Adjusted path if needed, but should be fine
# from ..core.registry import EntityRegistry # Keep if used by validate, remove if not
from feature_platform.core.registry import EntityRegistry # Assuming direct import works
from feature_platform.core.config import DatabricksConnectionConfig # Import the central config

logger = logging.getLogger(__name__)

@dataclass
class DatabricksSQLConnectionLocalConfig: # Renamed from DatabricksConfig
    """Configuration for Databricks connection FROM ENVIRONMENT for SQL connector."""
    server_hostname: str
    http_path: str
    access_token: str
    catalog: Optional[str] = None
    schema: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> 'DatabricksSQLConnectionLocalConfig':
        """Create a DatabricksSQLConnectionLocalConfig from environment variables."""
        # Critical env vars
        hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        path = os.getenv("DATABRICKS_HTTP_PATH")
        token = os.getenv("DATABRICKS_TOKEN")

        if not all([hostname, path, token]):
            # This configuration relies on these specific env vars.
            # If central DatabricksConnectionConfig is used, it might have different ways of sourcing these.
            raise ValueError(
                "DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN "
                "must be set in environment for DatabricksSQLConnectionLocalConfig.from_env()"
            )

        return cls(
            server_hostname=hostname,
            http_path=path,
            access_token=token,
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
class DeltaSQLSourceConfig(SourceConfig): # Renamed from DeltaSourceConfig for clarity
    """Configuration for a Delta table source accessed via SQL connector."""
    timestamp_column: Optional[str] = None
    partition_filters: Optional[Dict[str, str]] = field(default_factory=dict)
    # Add a field for the centralized connection config
    databricks_connection: Optional[DatabricksConnectionConfig] = None 
    
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
            conditions.append(f"{column} = %({column})s") # Parameterized query
            params[column] = value
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        return f"WHERE {where_clause}", params


class DatabricksSQLSource(Source[pd.DataFrame]): # Renamed from DatabricksSource
    """A source that reads data from a Databricks Delta table using the SQL Connector."""
    
    def __init__(self, config: Union[Dict[str, Any], DeltaSQLSourceConfig]): # Updated config type
        """Initialize the Databricks Delta SQL source."""
        if isinstance(config, dict):
            # If a dict is passed, we need to decide how to handle databricks_connection
            # For now, let's assume if databricks_connection is a dict, it's for DatabricksConnectionConfig
            conn_conf_dict = config.pop("databricks_connection", None)
            parsed_config = DeltaSQLSourceConfig(**config)
            if conn_conf_dict and isinstance(conn_conf_dict, dict):
                parsed_config.databricks_connection = DatabricksConnectionConfig(**conn_conf_dict)
            elif conn_conf_dict and isinstance(conn_conf_dict, DatabricksConnectionConfig):
                 parsed_config.databricks_connection = conn_conf_dict
            config = parsed_config
        
        super().__init__(config)
        self.config: DeltaSQLSourceConfig = config # Type hint for self.config
        self._connection: Optional[DatabricksConnection] = None
        self._db_sql_conn_local_config: Optional[DatabricksSQLConnectionLocalConfig] = None

    def _get_connection_params(self) -> Dict[str, str]:
        """Determines connection parameters either from central config or local env config."""
        if self.config.databricks_connection and self.config.databricks_connection.server_hostname:
            # Use the central config if provided and it has essential details
            # This assumes DatabricksConnectionConfig can provide what DatabricksSQLConnectionLocalConfig.get_connection_params needs
            # We might need to adjust DatabricksConnectionConfig or add a method to it if direct mapping isn't clean
            central_conn = self.config.databricks_connection
            if not all([central_conn.server_hostname, central_conn.http_path, central_conn.access_token]):
                raise ValueError("DatabricksConnectionConfig must provide server_hostname, http_path, and access_token for SQL connector.")

            params = {
                "server_hostname": central_conn.server_hostname,
                "http_path": central_conn.http_path,
                "access_token": central_conn.access_token,
            }
            # Ensure catalog and schema are accessed correctly, hasattr can be used for safety
            if hasattr(central_conn, 'catalog') and central_conn.catalog: 
                params["catalog"] = central_conn.catalog
            if hasattr(central_conn, 'schema') and central_conn.schema: 
                 params["schema"] = central_conn.schema
            return params
        else:
            # Fallback to environment-based local config
            if not self._db_sql_conn_local_config:
                self._db_sql_conn_local_config = DatabricksSQLConnectionLocalConfig.from_env()
            return self._db_sql_conn_local_config.get_connection_params()

    def _get_connection(self) -> DatabricksConnection:
        """Get or create a Databricks SQL connection."""
        if self._connection is None or self._connection.is_closed:
            params = self._get_connection_params()
            self._connection = sql.connect(**params)
        return self._connection
    
    def _execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None # Changed from parameters to params for consistency
    ) -> pd.DataFrame:
        """Execute a SQL query and return the results as a pandas DataFrame."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Using named parameters with %(name)s style for databricks.sql connector
            cursor.execute(query, parameters=params or {})
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            logger.error(f"Error executing query: {query} with params: {params}. Error: {e}")
            raise
        finally:
            cursor.close()
    
    def read(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        **kwargs # Allow passing additional query parameters if needed
    ) -> pd.DataFrame:
        """Read data from the Delta table using SQL connector."""
        self.initialize() # From base class, calls validate()
        
        table_name = self.config.get_full_table_name()
        fields_str = ", ".join(self.config.fields) if self.config.fields else "*"
        
        where_clause, query_params = self.config.get_sql_where_clause(start_time, end_time)
        # Allow overriding/adding params via kwargs for more flexibility if needed
        query_params.update(kwargs) 
        
        limit_clause = f"LIMIT {limit}" if limit is not None else ""
        
        query = f"SELECT {fields_str} FROM {table_name} {where_clause} {limit_clause}" # Ensure spaces
        
        logger.debug(f"Executing SQL query: {query} with params: {query_params}")
        return self._execute_query(query, params=query_params) # Ensure 'params' matches _execute_query
    
    def validate(self, registry: Optional[EntityRegistry] = None) -> bool:
        """Validate that the source is properly configured and accessible."""
        if registry is not None: # Entity registry validation (optional)
            entity = registry.get_entity(self.config.entity)
            if not entity:
                logger.error(f"Entity '{self.config.entity}' not found in registry for source '{self.config.name}'")
                return False
        
        try:
            table_name = self.config.get_full_table_name()
            # Use a simple query that doesn't fetch data, e.g., DESCRIBE or LIMIT 0/1
            # DESCRIBE TABLE might be too specific, a LIMIT 1 is common.
            self._execute_query(f"SELECT * FROM {table_name} LIMIT 1")
            logger.info(f"Successfully validated SQL connection to table {table_name} for source '{self.config.name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to access table {self.config.location} for source '{self.config.name}' using SQL connector: {e}")
            return False
    
    def get_schema(self) -> Dict[str, str]:
        """Get the schema of the Delta table using SQL connector."""
        self.initialize()
        table_name = self.config.get_full_table_name()
        # DESCRIBE TABLE is standard SQL and should work
        # Alternative: SELECT * FROM table LIMIT 0 and inspect cursor.description
        df = self._execute_query(f"DESCRIBE TABLE {table_name}")
        
        schema = {}
        # Column names in DESCRIBE output can vary (e.g., 'col_name', 'column_name', 'data_type', 'type')
        # Assuming 'col_name' and 'data_type' based on previous version. Adjust if different for your Databricks version.
        for _, row in df.iterrows():
            # Check if expected columns exist to avoid KeyError
            col_name = row.get('col_name') or row.get('column_name')
            data_type = row.get('data_type') or row.get('type')
            if col_name and data_type and not col_name.startswith('#'): # Skip comment rows
                schema[col_name] = data_type
        return schema
    
    def get_latest_timestamp(self) -> Optional[datetime]:
        """Get the latest timestamp from the table using SQL connector."""
        if not self.config.timestamp_column:
            logger.warning(f"No timestamp_column configured for source '{self.config.name}'. Cannot get latest timestamp.")
            return None
            
        table_name = self.config.get_full_table_name()
        query = f"SELECT MAX({self.config.timestamp_column}) as latest_timestamp FROM {table_name}"
        
        try:
            result_df = self._execute_query(query)
            if not result_df.empty and 'latest_timestamp' in result_df.columns:
                latest_val = result_df.iloc[0]['latest_timestamp']
                # Ensure it's a datetime object, pandas might return Timestamp
                if pd.isna(latest_val): return None
                return pd.to_datetime(latest_val)
            return None
        except Exception as e:
            logger.error(f"Error getting latest timestamp for source '{self.config.name}': {e}")
            return None
    
    def close(self) -> None:
        """Close the connection to Databricks SQL endpoint."""
        if self._connection is not None:
            if not self._connection.is_closed:
                self._connection.close()
                logger.info(f"Databricks SQL connection closed for source '{self.config.name}'.")
            self._connection = None
        super().close() # Call base class close if it has any logic
    
    def __del__(self):
        """Ensure the connection is closed when the object is destroyed."""
        self.close()
