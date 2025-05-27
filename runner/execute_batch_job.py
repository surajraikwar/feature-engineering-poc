import argparse
import logging
import sys
import os # Ensure os is imported, it's used in main for SPARK_REMOTE and in __main__

from feature_platform.core.spark import SparkSessionManager
from feature_platform.jobs.config_loader import load_job_config, JobConfig

# --- Source Factory (Basic) ---
# In a more advanced setup, this would be part of a registry system.
from feature_platform.sources.databricks_spark import DatabricksSparkSource, DatabricksSparkSourceConfig
from feature_platform.sources.spark_base import SparkSource # For type hinting

# --- Transformer Factory (Basic - to be expanded in next step) ---
from feature_platform.features import (
    UserSpendAggregator,
    UserMonthlyTransactionCounter,
    UserCategoricalSpendAggregator,
    SimpleAgeCalculator, 
    WithGreeting,
    get_transformer # Import the new factory function
)
# from feature_platform.features.transform import FeatureTransformer # No longer needed directly here if get_transformer returns it

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__) # Use __name__ for logger hierarchy

# Basic Source Registry
SOURCE_REGISTRY = {
    "databricks_spark": DatabricksSparkSource
    # Add other sources here
}

def get_source_instance(job_config: JobConfig, spark_manager: SparkSessionManager) -> SparkSource:
    """Instantiates a SparkSource based on job configuration."""
    source_type = job_config.input_source.source_type
    source_class = SOURCE_REGISTRY.get(source_type)

    if not source_class:
        logger.error(f"Unsupported source_type: {source_type}. Available types: {list(SOURCE_REGISTRY.keys())}")
        raise ValueError(f"Unsupported source_type: {source_type}")

    # The config_loader.py uses Pydantic models. job_config.input_source.config is an InputSourceParams model.
    # DatabricksSparkSourceConfig needs specific fields: name, entity, type, location, format, fields, connection_config.
    # Some of these are not directly in InputSourceParams or need to be derived.
    
    # This is a simplified bridge. A more robust factory would handle this mapping better,
    # possibly with each SourceConfig class having a from_job_config method.
    if source_type == "databricks_spark":
        input_params = job_config.input_source.config # This is InputSourceParams model

        # Create DatabricksSparkSourceConfig instance
        # 'name' and 'entity' are not in InputSourceParams, so we use placeholders or derive them.
        # 'type' in DatabricksSparkSourceConfig usually refers to the source's own type like 'delta',
        # while job_config.input_source.source_type is 'databricks_spark'.
        # 'fields' are also not in InputSourceParams. We can pass None or get from job_config if added there.
        
        specific_source_config = DatabricksSparkSourceConfig(
            name=job_config.job_name or "unnamed_source", # Using job_name as a placeholder for source name
            entity="default_entity", # Placeholder, ideally this comes from job_config more explicitly
            type=input_params.format, # Assuming format (e.g. "delta") is the 'type' for DatabricksSparkSourceConfig
            location=input_params.location,
            format=input_params.format,
            options=input_params.options,
            # connection_config: InputSourceParams has connection_config as Optional[Dict].
            # DatabricksSparkSourceConfig expects Optional[DatabricksConnectionConfig].
            # For now, passing as dict; DatabricksSparkSourceConfig might handle it or need adjustment.
            # This is a known area for future refinement (config object hydration).
            connection_config=input_params.connection_config # type: ignore 
        )
        return DatabricksSparkSource(config=specific_source_config, spark_manager=spark_manager)
    else:
        # Fallback for other types - this part needs proper factory logic
        raise NotImplementedError(f"Source factory logic for {source_type} not fully implemented.")


def main():
    parser = argparse.ArgumentParser(description="Execute a feature engineering batch job.")
    parser.add_argument(
        "config_path",
        type=str,
        help="Path to the job configuration YAML file."
    )
    args = parser.parse_args()

    logger.info(f"Loading job configuration from: {args.config_path}")
    try:
        job_config = load_job_config(args.config_path)
    except Exception as e:
        logger.error(f"Failed to load or validate job configuration: {e}", exc_info=True)
        sys.exit(1)

    logger.info(f"Starting job: {job_config.job_name}")
    logger.debug(f"Job configuration details: {job_config.dict()}")

    spark_master_url = os.getenv("SPARK_REMOTE") 
    
    # The SparkSessionManager instance is 'manager'.
    # The 'with manager as active_spark_session:' block ensures get_session() is called at the start
    # and stop_session() is called at the end. 'active_spark_session' is the SparkSession object.
    manager = SparkSessionManager(app_name=job_config.job_name or "FeaturePlatformJob", master_url=spark_master_url)
    
    with manager as active_spark_session:
        try:
            logger.info(f"Spark session initialized. Version: {active_spark_session.version}")
            if active_spark_session.conf.get("spark.master").startswith("sc://"):
                logger.info("Running in Databricks Connect mode.")
            else:
                logger.info(f"Running in mode: {active_spark_session.conf.get('spark.master')}")

            # 1. Instantiate and read from Input Source
            logger.info(f"Initializing input source: {job_config.input_source.source_type}")
            # Pass the 'manager' (SparkSessionManager instance) to get_source_instance.
            source_instance = get_source_instance(job_config, manager) 
            
            logger.info(f"Reading data from source: {job_config.input_source.config.location}")
            df = source_instance.read()
            logger.info(f"Successfully read data. DataFrame schema:")
            df.printSchema()
            if logger.isEnabledFor(logging.DEBUG): 
                df.show(5, truncate=False)

            # 2. Apply Feature Transformers
            if not job_config.feature_transformers:
                logger.info("No feature transformers specified in the configuration.")
            else:
                logger.info("Applying feature transformers...")
                for tf_config in job_config.feature_transformers:
                    tf_name = tf_config.name
                    # tf_config.params is a FeatureTransformerParams Pydantic model
                    tf_params_dict = tf_config.params.dict() # Get params as dict
                    
                    try:
                        # Use the new factory function
                        transformer_instance = get_transformer(name=tf_name, params=tf_params_dict)
                        # get_transformer already logs initialization
                        logger.info(f"Applying transformer: {tf_name}")
                        df = transformer_instance.apply(df)
                        logger.info(f"Successfully applied {tf_name}. New schema:")
                        df.printSchema()
                        if logger.isEnabledFor(logging.DEBUG):
                             df.show(5, truncate=False)
                    except Exception as e: # get_transformer can raise ValueError, or apply() can raise others
                        logger.error(f"Error processing transformer {tf_name} with params {tf_params_dict}: {e}", exc_info=True)
                        raise # Re-raise to be caught by the outer try-except block for job failure

            # 3. Handle Output Sink
            logger.info(f"Handling output sink: {job_config.output_sink.sink_type}")
            sink_type = job_config.output_sink.sink_type
            sink_config = job_config.output_sink.config # This is OutputSinkParams model

            if sink_type == "display":
                logger.info(f"Displaying DataFrame (first {sink_config.num_rows} rows):")
                df.show(sink_config.num_rows, truncate=sink_config.truncate)
            elif sink_type in ["overwrite_delta", "append_delta", "delta_table"]: # "delta_table" as generic
                if not sink_config.path:
                    logger.error("Output sink type 'delta_table' requires 'path' in sink config.")
                    raise ValueError("Missing path for delta_table sink.")
                # Determine mode: if "overwrite_delta" or "append_delta" is used, it dictates mode.
                # Otherwise, use sink_config.mode (which defaults to "overwrite").
                mode = sink_config.mode
                if sink_type == "overwrite_delta": mode = "overwrite"
                elif sink_type == "append_delta": mode = "append"
                
                logger.info(f"Writing DataFrame to Delta table: {sink_config.path}, mode: {mode}")
                df.write.format("delta").mode(mode).options(**(sink_config.options or {})).save(sink_config.path)
                logger.info(f"Successfully wrote to Delta table: {sink_config.path}")
            elif sink_type in ["overwrite_parquet", "append_parquet", "parquet_files"]: # "parquet_files" as generic
                if not sink_config.path:
                    logger.error("Output sink type 'parquet_files' requires 'path' in sink config.")
                    raise ValueError("Missing path for parquet_files sink.")
                mode = sink_config.mode
                if sink_type == "overwrite_parquet": mode = "overwrite"
                elif sink_type == "append_parquet": mode = "append"

                writer = df.write.mode(mode).options(**(sink_config.options or {}))
                if sink_config.partition_by:
                    writer = writer.partitionBy(*sink_config.partition_by)
                writer.parquet(sink_config.path)
                logger.info(f"Successfully wrote to Parquet files at: {sink_config.path}")
            else:
                logger.warning(f"Unsupported sink_type: {sink_type}. DataFrame not written.")

            logger.info(f"Job '{job_config.job_name}' completed successfully.")
            # No explicit sys.exit(0) needed, will exit with 0 if no unhandled exceptions.

        except Exception as e:
            logger.error(f"An error occurred during job execution for '{job_config.job_name}': {e}", exc_info=True)
            sys.exit(1) # Exit with error code
        # The SparkSessionManager's __exit__ (from context manager 'manager') will stop the session.

if __name__ == "__main__":
    # Example: python runner/execute_batch_job.py configs/jobs/sample_financial_features_job.yaml
    # Ensure SPARK_REMOTE, DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID are set
    # in your environment if you want to run this against Databricks Connect.
    # Otherwise, it will default to local Spark mode if SPARK_REMOTE is not set.
    
    # Check if the default config exists, provide guidance if not.
    # This part is for user guidance when running the script directly.
    default_config_example = "configs/jobs/sample_financial_features_job.yaml"
    if not os.path.exists(default_config_example):
        logger.warning(f"Default example configuration '{default_config_example}' not found.")
        logger.warning("This script is best run with a job YAML configuration file.")
        logger.warning("You can create 'configs/jobs/sample_financial_features_job.yaml' based on prior steps.")
        
        # Create a minimal dummy config if the directory exists, for basic script parsing test.
        minimal_config_dir = "configs/jobs"
        minimal_config_path = os.path.join(minimal_config_dir, "minimal_test_job.yaml")
        
        if not os.path.exists(minimal_config_dir):
            try:
                os.makedirs(minimal_config_dir)
                logger.info(f"Created directory: {minimal_config_dir}")
            except OSError as e:
                logger.error(f"Could not create directory {minimal_config_dir}: {e}")
        
        if os.path.exists(minimal_config_dir) and not os.path.exists(minimal_config_path):
            with open(minimal_config_path, "w") as f:
                f.write("""
job_name: minimal_test_display_job
input_source:
  source_type: databricks_spark 
  config:
    location: "dummy/path/to/table" # This source will likely fail without a real Spark setup or dummy data
feature_transformers: []
output_sink:
  sink_type: display
  config:
    num_rows: 1
""")
            logger.info(f"Created '{minimal_config_path}'. You can try running with this for a basic syntax check:")
            logger.info(f"Example: python runner/execute_batch_job.py {minimal_config_path}")
        else:
            logger.info(f"Minimal config '{minimal_config_path}' already exists or its directory couldn't be made.")
    else:
        logger.info(f"Found default example config: {default_config_example}")
        logger.info(f"To run with it: python runner/execute_batch_job.py {default_config_example}")

    main()
