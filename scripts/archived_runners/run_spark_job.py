import logging
from pyspark.sql.types import StructType, StructField, StringType, DateType # For dummy data
from datetime import date, datetime
import shutil # For cleaning up dummy delta path

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    from domain.core.spark import SparkSessionManager
    from domain.sources.databricks_spark import DatabricksSparkSource, DatabricksSparkSourceConfig
    from domain.core.config import DatabricksConnectionConfig
    from domain.features.transform import SimpleAgeCalculator, WithGreeting
except ImportError as e:
    logger.error(f"Failed to import necessary modules. Ensure PYTHONPATH is set correctly and all dependencies are installed: {e}")
    raise

# Define a local path for the dummy Delta table
DUMMY_DELTA_PATH = "tmp/example_delta_table_runner"

def create_dummy_delta_table(spark_session, path: str):
    """Creates a dummy Delta table at the given path for example purposes."""
    logger.info(f"Creating dummy Delta table at {path}...")
    try:
        data = [
            ("101", "Alice", date(1992, 3, 10)),
            ("102", "Bob", date(1988, 7, 21)),
            ("103", "Charlie", date(2001, 11, 5))
        ]
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("birth_date", DateType(), True)
        ])
        df = spark_session.createDataFrame(data, schema)
        df.write.format("delta").mode("overwrite").save(path)
        logger.info(f"Dummy Delta table created successfully at {path}.")
    except Exception as e:
        logger.error(f"Failed to create dummy Delta table: {e}")
        raise

def main(spark_manager_instance): # Accept the manager instance
    logger.info("Starting example Spark job...")
    spark = spark_manager_instance.get_session() # Get session from the passed manager

    try:
        # 1. Create a dummy Delta table for the source to read
        create_dummy_delta_table(spark, DUMMY_DELTA_PATH)

        # 2. Configure and instantiate DatabricksSparkSource
        conn_config = DatabricksConnectionConfig(
            server_hostname="dummy_host", 
            http_path="dummy_path",       
            access_token="dummy_token"    
        )

        source_config = DatabricksSparkSourceConfig(
            name="local_transactions_delta",
            entity="transaction", 
            type="delta", 
            location=DUMMY_DELTA_PATH, 
            format="delta",
            fields=["user_id", "name", "birth_date"], 
            connection_config=conn_config 
        )
        
        databricks_source = DatabricksSparkSource(config=source_config, spark_manager=spark_manager_instance) # Pass the manager

        # 3. Validate the source (optional, but good practice)
        if not databricks_source.validate():
            logger.error("DatabricksSparkSource validation failed. Exiting.")
            return

        # 4. Read data using the source
        logger.info("Reading data using DatabricksSparkSource...")
        input_df = databricks_source.read()
        logger.info("Data read successfully. Schema:")
        input_df.printSchema()
        logger.info("Input DataFrame:")
        input_df.show(truncate=False)

        # 5. Apply Feature Transformers
        age_calculator = SimpleAgeCalculator(birthdate_col="birth_date", output_col="age")
        logger.info(f"Applying {age_calculator}...")
        df_with_age = age_calculator.apply(input_df)
        logger.info("DataFrame with Age:")
        df_with_age.show(truncate=False)

        greeting_transformer = WithGreeting(name_col="name", greeting_col_name="user_greeting")
        logger.info(f"Applying {greeting_transformer}...")
        final_df = greeting_transformer.apply(df_with_age)
        logger.info("Final DataFrame with Greeting:")
        final_df.show(truncate=False)
        
        logger.info("Example Spark job completed successfully.")

    except Exception as e:
        logger.error(f"An error occurred during the Spark job: {e}", exc_info=True)
    finally:
        try:
            shutil.rmtree(DUMMY_DELTA_PATH)
            logger.info(f"Cleaned up dummy Delta table at {DUMMY_DELTA_PATH}.")
        except OSError as e:
            logger.warning(f"Could not remove dummy delta table directory {DUMMY_DELTA_PATH}: {e}")
    
if __name__ == "__main__":
    # Instantiate the SparkSessionManager here
    spark_manager = SparkSessionManager(app_name="FeaturePlatformRunnerJob", master_url="local[*]")
    
    try:
        # Pass the manager instance to main
        main(spark_manager_instance=spark_manager)
    finally:
        # Stop the session managed by this manager
        spark_manager.stop_session()
        logger.info("Spark session from main manager stopped.")
