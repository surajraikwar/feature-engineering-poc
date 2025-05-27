from dataclasses import dataclass, field
from typing import Optional
import os

@dataclass
class DatabricksConnectionConfig:
    """Configuration for connecting to Databricks."""
    server_hostname: Optional[str] = field(default_factory=lambda: os.getenv("DATABRICKS_SERVER_HOSTNAME"))
    http_path: Optional[str] = field(default_factory=lambda: os.getenv("DATABRICKS_HTTP_PATH")) # For SQL Connector or REST API
    access_token: Optional[str] = field(default_factory=lambda: os.getenv("DATABRICKS_TOKEN"))
    # For Spark Connect client or dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
    # and dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
    # For direct cluster submission, these might not be directly used by spark.read, 
    # but good to have for other interactions (e.g. job submission API)
    # If running directly on a Databricks cluster, SparkSession is usually pre-configured.
    # These are more for client-mode connections or for REST API interactions.
    
    def __post_init__(self):
        # Basic validation, ensure critical fields are present if trying to connect
        # This can be expanded based on connection mode (e.g. client vs. cluster)
        if self.server_hostname and not self.access_token:
            # Depending on auth method, token might not be needed (e.g. Azure AD passthrough)
            # For simplicity, we'll assume token is generally needed for API/client access.
            pass # Allow for now, specific sources can validate more strictly.

        # Note: For Spark running on Databricks, spark session is often ambient.
        # For connecting *to* Databricks from outside (e.g. local Spark with connect server or REST API):
        # - server_hostname: e.g., "adb-xxxxxxxxxxxxxx.xx.azuredatabricks.net"
        # - access_token: Personal Access Token (PAT)
        # - http_path: (If using SQL Connector) SQL Warehouse HTTP Path
        # - For Spark Connect: sc://<server_hostname>:15001 (or custom port)
        # Pyspark itself, when configured for databricks, uses these env vars or spark confs.
