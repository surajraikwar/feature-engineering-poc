from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional, Dict, Any
import logging

from pyspark.sql.utils import AnalysisException

from domain.core.config import DatabricksConnectionConfig
from domain.core.spark import SparkSessionManager
from domain.sources.spark_base import SparkSource, SparkSourceConfig

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame

logger = logging.getLogger(__name__)

@dataclass
class DatabricksSparkSourceConfig(SparkSourceConfig):
    """Configuration for a Databricks Delta table source using Spark."""
    # location: Inherited, e.g., "catalog_name.schema_name.table_name" or "/path/to/delta_table"
    # format: Inherited, should typically be "delta" for Databricks
    # options: Inherited, for any specific Spark read options for Delta
    
    # Databricks-specific connection config (can be optional if Spark session is pre-configured)
    connection_config: Optional[DatabricksConnectionConfig] = field(default_factory=DatabricksConnectionConfig)
    options: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.format.lower() != "delta":
            logger.warning(f"DatabricksSparkSourceConfig is typically used with format 'delta', but found '{self.format}'.")
        # Ensure options is always a dictionary
        if self.options is None:
            self.options = {}


class DatabricksSparkSource(SparkSource):
    """
    A Spark-based source that reads data from a Databricks Delta table.
    Assumes SparkSession is configured to access Databricks 
    (either running on Databricks, or via Spark Connect, or appropriate Hadoop configs for S3/ADLS).
    """

    def __init__(self, config: DatabricksSparkSourceConfig, spark_manager: SparkSessionManager):
        super().__init__(config, spark_manager)
        # config type is narrowed by the call to super().__init__ in SparkSource
        self.config: DatabricksSparkSourceConfig = config 
        # The DatabricksConnectionConfig is available at self.config.connection_config
        # It can be used if direct interaction with Databricks APIs is needed,
        # or to pass specific options to Spark if not ambiently configured.

    def read(self, read_options: Optional[Dict[str, Any]] = None, **kwargs) -> "SparkDataFrame":
        """
        Read data from the Databricks Delta table.

        Args:
            read_options: Additional options for spark.read.
                          These are merged with self.config.options.
        
        Returns:
            A Spark DataFrame.
        """
        self.initialize() # Ensures source is validated and Spark session might be started implicitly
        
        spark = self.spark_manager.get_session()
        table_location = self.config.location 
        read_format = self.config.format

        # Merge options from config and runtime
        merged_options = {**self.config.options, **(read_options or {})}

        logger.info(f"Reading from Databricks location: '{table_location}' with format '{read_format}' and options: {merged_options}")
        
        try:
            df = spark.read.format(read_format).options(**merged_options).load(table_location)
            
            # --- Schema Validation Step ---
            if self.config.fields: # Check if fields metadata is provided
                actual_columns = set(df.columns)
                expected_columns = {field_dict['name'] for field_dict in self.config.fields}

                missing_in_df = expected_columns - actual_columns
                if missing_in_df:
                    error_messages = [f"Missing expected column in DataFrame: '{col}'" for col in missing_in_df]
                    for msg in error_messages:
                        logger.error(msg)
                    raise ValueError(f"Schema validation failed. Missing columns: {', '.join(sorted(list(missing_in_df)))}")

                extra_in_df = actual_columns - expected_columns
                if extra_in_df:
                    for col in sorted(list(extra_in_df)):
                        logger.warning(f"DataFrame contains an extra column not defined in source catalog fields: '{col}'")
                
                # Optional: Type validation could be added here in the future.
                # For now, only column name presence is validated.
                logger.info("Schema validation (column names) completed.")
            else:
                logger.info("No field definitions provided in source config; skipping schema validation.")
            
            return df
        except AnalysisException as e:
            logger.error(f"Failed to read from Databricks location '{table_location}': {e}")
            raise ValueError(f"Could not read data from {table_location}. Ensure table exists and Spark session has access.") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred while reading from '{table_location}': {e}")
            raise

    def validate(self) -> bool:
        """
        Validate that the source is properly configured and the table is accessible.
        Tries to read schema of the table.
        """
        spark = self.spark_manager.get_session() # Get or create session
        table_location = self.config.location
        read_format = self.config.format
        merged_options = self.config.options

        logger.info(f"Validating DatabricksSparkSource for location: '{table_location}'")
        try:
            # Try to infer schema as a way of checking if the table exists and is accessible
            spark.read.format(read_format).options(**merged_options).load(table_location).schema
            logger.info(f"Successfully validated access to {table_location}")
            return True
        except AnalysisException as e:
            logger.error(f"Validation failed for Databricks location '{table_location}'. Spark AnalysisException: {e}")
            return False
        except Exception as e:
            logger.error(f"Validation failed for Databricks location '{table_location}'. Unexpected error: {e}")
            return False

# Example of how it might be used (for illustration, not part of the class itself)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    # This example assumes a local Spark session and a Delta table at "tmp/delta_example"
    # 1. Create a dummy Delta table locally for testing
    try:
        with SparkSessionManager(app_name="DatabricksSparkSourceTestSetup") as local_spark:
            data = [("event1", "userA", 100, "2023-01-01T10:00:00Z"),
                    ("event2", "userB", 150, "2023-01-01T11:00:00Z")]
            columns = ["event_id", "user_id", "amount", "timestamp"]
            df_local = local_spark.createDataFrame(data, columns)
            df_local.write.format("delta").mode("overwrite").save("tmp/delta_example")
            logger.info("Dummy Delta table created at tmp/delta_example")

        # 2. Setup Configs and Source
        # For local testing, DatabricksConnectionConfig is not strictly needed by spark.read.delta if path is local
        # It would be more relevant if connecting to a remote Databricks workspace.
        conn_config = DatabricksConnectionConfig() # Uses env vars or defaults
        
        source_config_dict = {
            "name": "my_delta_source",
            "entity": "some_entity", # Assuming 'some_entity' is defined in an EntityRegistry
            "type": "delta", # This 'type' is from base SourceConfig, might not be directly used by SparkSource logic
            "location": "tmp/delta_example", # Path to the local Delta table
            "format": "delta",
            "fields": ["event_id", "user_id", "amount", "timestamp"], # Informational, actual schema comes from Delta
            "connection_config": conn_config 
        }
        # We need to ensure the config object passed to DatabricksSparkSource is DatabricksSparkSourceConfig
        databricks_source_config = DatabricksSparkSourceConfig(**source_config_dict)


        # 3. Initialize SparkSessionManager and DatabricksSparkSource
        spark_manager_instance = SparkSessionManager(app_name="DatabricksSparkSourceTest")
        databricks_source = DatabricksSparkSource(config=databricks_source_config, spark_manager=spark_manager_instance)

        # 4. Validate and Read
        if databricks_source.validate():
            logger.info("DatabricksSparkSource validation successful.")
            df_read = databricks_source.read()
            logger.info("Successfully read data from DatabricksSparkSource:")
            df_read.show()
        else:
            logger.error("DatabricksSparkSource validation failed.")

    except ImportError as ie:
        logger.error(f"Pyspark needs to be installed to run this example: {ie}")
    except Exception as ex:
        logger.error(f"An error occurred in the example: {ex}")
    finally:
        # Clean up dummy table and stop spark session
        if 'spark_manager_instance' in locals():
            spark_manager_instance.stop_session()
        try:
            import shutil
            shutil.rmtree("tmp/delta_example")
            logger.info("Cleaned up dummy Delta table at tmp/delta_example")
        except OSError:
            pass # Table might not have been created
        logger.info("Example finished.")
