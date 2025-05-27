from pyspark.sql import SparkSession
import logging

logger = logging.getLogger(__name__)

class SparkSessionManager:
    """Manages the SparkSession lifecycle."""

    def __init__(self, app_name: str = "FeaturePlatform", master_url: str = "local[*]"):
        """
        Initializes the SparkSessionManager.

        Args:
            app_name: The name of the Spark application.
            master_url: The master URL for Spark. Defaults to "local[*]" for local mode.
        """
        self.app_name = app_name
        self.master_url = master_url
        self._spark_session: SparkSession = None

    def get_session(self) -> SparkSession:
        """
        Gets or creates a SparkSession.

        Returns:
            The active SparkSession.
        """
        if self._spark_session is None:
            logger.info(f"Creating SparkSession with app_name='{self.app_name}' and master_url='{self.master_url}'")
            try:
                self._spark_session = (
                    SparkSession.builder.appName(self.app_name)
                    .master(self.master_url)
                    .getOrCreate()
                )
                logger.info("SparkSession created successfully.")
            except Exception as e:
                logger.error(f"Failed to create SparkSession: {e}")
                raise
        return self._spark_session

    def stop_session(self) -> None:
        """Stops the active SparkSession if it exists."""
        if self._spark_session is not None:
            logger.info("Stopping SparkSession.")
            try:
                self._spark_session.stop()
                self._spark_session = None
                logger.info("SparkSession stopped successfully.")
            except Exception as e:
                logger.error(f"Failed to stop SparkSession: {e}")
                # Optionally re-raise or handle as needed
                raise

    def __enter__(self) -> SparkSession:
        """Context manager entry method to get the Spark session."""
        return self.get_session()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit method to stop the Spark session."""
        self.stop_session()

if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    
    # Using the context manager
    with SparkSessionManager(app_name="MySparkApp") as spark:
        logger.info(f"Spark version: {spark.version}")
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        columns = ["name", "id"]
        df = spark.createDataFrame(data, columns)
        df.show()
        logger.info("Spark DataFrame created and shown successfully within context manager.")

    # Manual session management
    manager = SparkSessionManager(app_name="MyManualSparkApp", master_url="local[2]")
    try:
        spark_manual = manager.get_session()
        logger.info(f"Spark version (manual): {spark_manual.version}")
        data_manual = [("David", 4), ("Eve", 5)]
        df_manual = spark_manual.createDataFrame(data_manual, columns)
        df_manual.show()
        logger.info("Spark DataFrame created and shown successfully with manual management.")
    finally:
        manager.stop_session()
        logger.info("Manual session stopped.")
