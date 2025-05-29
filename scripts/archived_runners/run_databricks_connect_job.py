import logging
import os
from typing import Optional

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import necessary classes from the domain
try:
    from domain.core.spark import SparkSessionManager
    from domain.sources.databricks_spark import DatabricksSparkSource, DatabricksSparkSourceConfig
    from domain.core.config import DatabricksConnectionConfig # May not be explicitly needed if env vars are set
    from domain.features import (
        UserSpendAggregator,
        UserMonthlyTransactionCounter,
        UserCategoricalSpendAggregator
    )
except ImportError as e:
    logger.error(
        "Failed to import necessary modules from domain. "
        "Ensure the package is installed correctly (e.g., `pip install -e .`) "
        "and your PYTHONPATH is set up if needed. Error: %s", e
    )
    raise

def print_usage_guide():
    logger.info("""
    ---------------------------------------------------------------------------------------------
    Running the Databricks Connect Job
    ---------------------------------------------------------------------------------------------
    This script connects to a Databricks cluster using Databricks Connect to read a table,
    compute features, and display them.

    Prerequisites:
    1. Python Environment: Make sure 'pyspark' and 'feature-platform' (editable install) are installed.
    2. Databricks Connect Client: Installed and configured for your Databricks workspace.
       Run `databricks-connect configure` (or `databricks-connect test` for older versions).
       Ensure your PySpark version is compatible with your Databricks Runtime version.
    3. Environment Variables: Set the following environment variables in your terminal:
       - DATABRICKS_HOST: Your Databricks workspace URL (e.g., https://adb-xxxx.azuredatabricks.net)
       - DATABRICKS_TOKEN: Your Databricks Personal Access Token.
       - DATABRICKS_CLUSTER_ID: The ID of the cluster to connect to.
       - SPARK_REMOTE (Optional but recommended): The Databricks Connect URL 
         (e.g., sc://<your-workspace-host-without-https>:15001 or your configured port).
         If not set, SparkSessionManager will try to infer or use its default.

    Example Table Structure (jupiter.mm.fact_mm_transaction):
    - user_id (string): Identifier for the user.
    - timestamp (timestamp or date): Timestamp of the transaction.
    - amount (double or decimal): Amount of the transaction.
    - category (string): Category of the transaction (e.g., 'food and drinks').
    (Adjust column names in transformer instantiations if your schema differs.)

    To Run:
    python runner/run_databricks_connect_job.py
    ---------------------------------------------------------------------------------------------
    """)

def main():
    print_usage_guide()

    # Configuration for the Databricks source table
    databricks_table_location = "jupiter.mm.fact_mm_transaction"
    
    # --- Feature Transformer Configurations ---
    # Ensure these column names match your actual table schema for jupiter.mm.fact_mm_transaction
    user_id_column = "user_id" 
    timestamp_column = "timestamp" 
    amount_column = "amount" 
    category_column = "category" 
    target_category_for_spend = "food and drinks"

    spark_remote_url = os.getenv("SPARK_REMOTE")
    logger.info(f"SPARK_REMOTE environment variable is set to: {spark_remote_url if spark_remote_url else 'Not set'}")

    # Initialize SparkSessionManager.
    # The manager instance is created here.
    manager = SparkSessionManager(app_name="DatabricksConnectFeatureJob", master_url=spark_remote_url)

    # The 'with manager as active_session:' block ensures get_session() is called at the start
    # and stop_session() is called at the end. 'active_session' is the SparkSession object.
    with manager as active_session: 
        try:
            logger.info(f"Successfully initialized Spark Session. Version: {active_session.version}")
            if not active_session.conf.get("spark.master").startswith("sc://"):
                 logger.warning(
                    "Spark master does not appear to be a Databricks Connect URL ('sc://...'). "
                    "The job might run locally or fail if Databricks resources are expected. "
                    "Ensure SPARK_REMOTE is set correctly for Databricks Connect."
                 )

            connection_config = DatabricksConnectionConfig() 

            source_config = DatabricksSparkSourceConfig(
                name="databricks_mm_transactions",
                entity="transaction", 
                type="delta", 
                location=databricks_table_location,
                format="delta",
                fields=[user_id_column, timestamp_column, amount_column, category_column], 
                connection_config=connection_config
            )
            
            # DatabricksSparkSource expects the manager instance.
            databricks_source = DatabricksSparkSource(config=source_config, spark_manager=manager)
            
            # Validate and Read
            logger.info(f"Attempting to read from Delta table: {databricks_table_location}")
            # The databricks_source will use the session from the 'manager' it was given.
            if not databricks_source.validate(): 
                logger.error(f"Source validation failed for table {databricks_table_location}. Exiting.")
                return
            
            input_df = databricks_source.read()
            # At this point, input_df is a Spark DataFrame obtained via Databricks Connect.
            # For logging or operations that trigger actions, be mindful of data transfer if the table is large.
            # Example: input_df.count() or input_df.show() will execute Spark jobs on the cluster.
            logger.info(f"Attempting to count rows from {databricks_table_location}...")
            # Note: .count() can be slow on very large tables.
            # For a quick check, consider input_df.limit(1).count() or similar if only existence is needed.
            # However, for this script, we proceed with a full count to verify readability.
            num_rows = input_df.count()
            logger.info(f"Successfully read {num_rows} rows from {databricks_table_location}. Schema:")
            input_df.printSchema()
            logger.info("Showing sample of input data (first 5 rows):")
            input_df.show(5, truncate=False)

            # Apply Feature Transformers
            logger.info("Applying feature transformers...")

            # 1. User Spend Aggregator (30-day window)
            spend_aggregator = UserSpendAggregator(
                user_id_col=user_id_column,
                timestamp_col=timestamp_column,
                amount_col=amount_column,
                window_days=30
            )
            df_transformed = spend_aggregator.apply(input_df)
            logger.info("Applied UserSpendAggregator. Showing sample (first 5 rows with new spend columns):")
            # Select specific columns to make the output cleaner for this log message
            df_transformed.select(user_id_column, timestamp_column, amount_column, spend_aggregator.total_spend_output_col, spend_aggregator.avg_spend_output_col).show(5, truncate=False)

            # 2. User Monthly Transaction Counter
            monthly_counter = UserMonthlyTransactionCounter(
                user_id_col=user_id_column,
                timestamp_col=timestamp_column
            )
            df_transformed = monthly_counter.apply(df_transformed) 
            logger.info("Applied UserMonthlyTransactionCounter. Showing sample (first 5 rows with new month/count columns, distinct user-month pairs):")
            df_transformed.select(user_id_column, monthly_counter.output_col_year_month, monthly_counter.output_col_txn_count).distinct().show(5, truncate=False)

            # 3. User Categorical Spend Aggregator
            categorical_spend_aggregator = UserCategoricalSpendAggregator(
                user_id_col=user_id_column,
                amount_col=amount_column,
                category_col=category_column,
                target_category_value=target_category_for_spend
            )
            df_transformed = categorical_spend_aggregator.apply(df_transformed)
            logger.info(f"Applied UserCategoricalSpendAggregator for category '{target_category_for_spend}'. Showing sample (first 5 rows with new category spend column):")
            df_transformed.select(user_id_column, timestamp_column, amount_column, category_column, categorical_spend_aggregator.output_col).show(5, truncate=False)
            
            logger.info("Final transformed DataFrame sample (first 20 rows):")
            df_transformed.show(20, truncate=False)
            
            logger.info("Databricks Connect feature computation job completed successfully.")

        except Exception as e:
            logger.error(f"An error occurred during the Databricks Connect job: {e}", exc_info=True)
            logger.error(
                "Ensure your Databricks Connect setup is correct, environment variables are set, "
                "the specified table exists, and column names in the script match your table schema."
            )
        # The 'manager' session is automatically stopped when exiting the 'with' block.

if __name__ == "__main__":
    main()
