from pyspark.sql import SparkSession
import logging
import os
from typing import Optional # Added for Optional type hint

logger = logging.getLogger(__name__)

# Environment variables for Databricks Connect:
# DATABRICKS_HOST: Your Databricks workspace URL (e.g., https://adb-xxxx.azuredatabricks.net)
# DATABRICKS_TOKEN: Your Databricks Personal Access Token
# DATABRICKS_CLUSTER_ID: The ID of the cluster to connect to.
# SPARK_REMOTE: Optional. The Databricks Connect URL (e.g., sc://<workspace-host>:<port>). If not set,
#               the SparkSession.builder.remote() might infer from DATABRICKS_HOST and DATABRICKS_CLUSTER_ID
#               in newer pyspark versions, but explicit is often better.

class SparkSessionManager:
    """Manages the SparkSession lifecycle, supporting local mode and Databricks Connect."""

    def __init__(self, app_name: str = "FeaturePlatform", master_url: Optional[str] = None):
        """
        Initializes the SparkSessionManager.

        Args:
            app_name: The name of the Spark application.
            master_url: The master URL for Spark. 
                        If None, attempts to use SPARK_REMOTE env var.
                        Defaults to "local[*]" if neither is provided.
        """
        self.app_name = app_name
        self._spark_session: Optional[SparkSession] = None # Type hint more precisely

        # Determine master URL
        if master_url:
            self.master_url = master_url
            logger.info(f"Using provided master_url: {self.master_url}")
        elif os.getenv("SPARK_REMOTE"):
            self.master_url = os.getenv("SPARK_REMOTE")
            logger.info(f"Using SPARK_REMOTE environment variable for master_url: {self.master_url}")
        else:
            self.master_url = "local[*]"
            logger.info(f"Defaulting to local master_url: {self.master_url}")

    def get_session(self) -> SparkSession:
        """
        Gets or creates a SparkSession.
        Supports local mode or Databricks Connect.
        For Databricks Connect, ensure DATABRICKS_HOST, DATABRICKS_TOKEN, 
        and DATABRICKS_CLUSTER_ID environment variables are set.
        Pyspark version >= 3.4 is recommended for seamless Databricks Connect.
        """
        if self._spark_session is None:
            logger.info(f"Attempting to create SparkSession with app_name='{self.app_name}' and resolved master_url='{self.master_url}'")
            
            try:
                builder = SparkSession.builder.appName(self.app_name)

                if self.master_url.startswith("sc://"):
                    # Databricks Connect mode
                    # Ensure necessary environment variables (DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID) are set.
                    # PySpark 3.4+ uses the .remote() method.
                    # It implicitly uses DATABRICKS_HOST, DATABRICKS_TOKEN.
                    # DATABRICKS_CLUSTER_ID is also often needed if not part of the connect string or default cluster.
                    # Let's ensure we log if critical env vars are missing for connect.
                    required_env_vars = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CLUSTER_ID"]
                    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
                    if missing_vars:
                        logger.warning(
                            f"For Databricks Connect (master_url starting with 'sc://'), "
                            f"the following environment variables are typically required but not set: {', '.join(missing_vars)}. "
                            "Please ensure they are set."
                        )
                    logger.info(f"Configuring SparkSession for Databricks Connect using master: {self.master_url}")
                    # For PySpark 3.4+, setting the master to sc://... is the way for Databricks Connect.
                    # The Databricks Connect client handles the "remote" nature.
                    builder = builder.master(self.master_url)

                elif self.master_url == "local[*]":
                    logger.info("Configuring SparkSession for local mode.")
                    builder = builder.master(self.master_url)
                else:
                    # Could be other Spark master URLs (e.g., spark://host:port for standalone)
                    logger.info(f"Configuring SparkSession for generic master: {self.master_url}")
                    builder = builder.master(self.master_url)
                
                # Configure Spark with Delta Lake and memory settings
                builder = builder \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                    .config("spark.driver.memory", "2g") \
                    .config("spark.executor.memory", "2g") \
                    .config("spark.driver.maxResultSize", "1g") \
                    .config("spark.network.timeout", "300s") \
                    .config("spark.sql.shuffle.partitions", "4") \
                    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
                    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=400 -XX:G1HeapRegionSize=16m") \
                    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=400 -XX:G1HeapRegionSize=16m")
                
                # Initialize Delta Lake with the builder
                from delta import configure_spark_with_delta_pip
                builder = configure_spark_with_delta_pip(builder)
                
                # Create the Spark session
                self._spark_session = builder.getOrCreate()
                
                # Set log level to WARN to reduce output
                self._spark_session.sparkContext.setLogLevel("WARN")
                
                logger.info(f"SparkSession created successfully. Spark version: {self._spark_session.version}")
                
                if self.master_url.startswith("sc://"):
                    logger.info("Running in Databricks Connect mode.")
                    # Attempt to get cluster ID, may not always be available or set this way.
                    # Default to 'N/A' if the conf is not found.
                    cluster_id = self._spark_session.conf.get('spark.databricks.clusterUsageTags.clusterId', 'N/A')
                    logger.info(f"Connected to cluster ID: {cluster_id}")
                elif self.master_url == "local[*]":
                    logger.info("Running in local mode.")

            except Exception as e:
                logger.error(f"Failed to create SparkSession: {e}")
                logger.error(
                    "If using Databricks Connect, ensure your local environment is correctly "
                    "configured (e.g., `databricks-connect configure`, correct PySpark version, "
                    "and necessary environment variables like DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID)."
                )
                raise
        return self._spark_session

    def stop_session(self) -> None:
        """Stops the active SparkSession if it exists."""
        if self._spark_session is not None:
            logger.info("Stopping SparkSession.")
            try:
                self._spark_session.stop()
                self._spark_session = None # Reset after stopping
                logger.info("SparkSession stopped successfully.")
            except Exception as e:
                logger.error(f"Failed to stop SparkSession: {e}")
                raise # Optionally re-raise or handle

    def __enter__(self) -> SparkSession:
        """Context manager entry method to get the Spark session."""
        return self.get_session()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit method to stop the Spark session."""
        self.stop_session()

# Example usage (remains the same, but can now be tested with SPARK_REMOTE)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # To test Databricks Connect, set SPARK_REMOTE (e.g., "sc://your-workspace-url:15001")
    # and DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID in your environment.
    # If SPARK_REMOTE is not set, it will run in local mode.
    
    logger.info("--- SparkSessionManager Example ---")
    logger.info("Expected environment variables for Databricks Connect (if used):")
    logger.info("  DATABRICKS_HOST: Your Databricks workspace URL (e.g., https://adb-....azuredatabricks.net)")
    logger.info("  DATABRICKS_TOKEN: Your Databricks Personal Access Token")
    logger.info("  DATABRICKS_CLUSTER_ID: The ID of the cluster to connect to")
    logger.info("  SPARK_REMOTE: Optional. The Databricks Connect URL (e.g., sc://<workspace-host>:<port>)")
    logger.info("------------------------------------")

    # Using the context manager
    # Pass no master_url to test env var or local fallback
    with SparkSessionManager(app_name="MySparkApp") as spark:
        logger.info(f"Spark version: {spark.version}")
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        columns = ["name", "id"]
        df = spark.createDataFrame(data, columns)
        df.show()
        logger.info("Spark DataFrame created and shown successfully within context manager.")

    # Manual session management
    # Explicitly pass a master_url (e.g., for a specific local config or overriding env)
    # manager = SparkSessionManager(app_name="MyManualSparkApp", master_url="local[2]")
    # For this example, let's also use the auto-detection
    manager = SparkSessionManager(app_name="MyManualSparkApp")
    try:
        spark_manual = manager.get_session()
        logger.info(f"Spark version (manual): {spark_manual.version}")
        data_manual = [("David", 4), ("Eve", 5)]
        df_manual = spark_manual.createDataFrame(data_manual, columns)
        df_manual.show()
        logger.info("Spark DataFrame created and shown successfully with manual management.")
    finally:
        manager.stop_session()
    logger.info("Manual session example finished.")
