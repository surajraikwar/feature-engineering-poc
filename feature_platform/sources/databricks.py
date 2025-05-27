"""Databricks data source implementation."""
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

@dataclass
class DatabricksSourceConfig:
    """Configuration for Databricks data source."""
    host: str
    token: str
    http_path: str
    catalog: Optional[str] = None
    schema: Optional[str] = None

class DatabricksSource:
    """Databricks data source implementation."""
    
    def __init__(self, config: DatabricksSourceConfig):
        """Initialize Databricks source with configuration."""
        self.config = config
        self._connection = None
    
    def connect(self):
        """Establish connection to Databricks."""
        try:
            from databricks import sql
            self._connection = sql.connect(
                server_hostname=self.config.host,
                http_path=self.config.http_path,
                access_token=self.config.token
            )
            return self._connection
        except ImportError as e:
            raise ImportError(
                "databricks-sql-connector is required for DatabricksSource. "
                "Install it with: pip install databricks-sql-connector"
            ) from e
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as a list of dictionaries."""
        if self._connection is None:
            self.connect()
            
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, parameters=params or {})
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()
    
    def close(self):
        """Close the connection to Databricks."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
