import argparse
import logging
import sys
import os # Ensure os is imported, it's used in main for SPARK_REMOTE and in __main__
from pathlib import Path

from feature_platform.core.spark import SparkSessionManager
from feature_platform.jobs.config_loader import load_job_config, JobConfig, JobInputSourceConfig
from feature_platform.core.source_registry import SourceRegistry
from feature_platform.core.source_definition import SourceDefinition, DatabricksSourceDetailConfig

# --- Source Factory (Basic) ---
from feature_platform.sources.databricks_spark import DatabricksSparkSource, DatabricksSparkSourceConfig
from feature_platform.sources.spark_base import SparkSource # For type hinting

# --- Transformer Factory ---
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

# Source Registry mapping catalog types to Python classes
SOURCE_REGISTRY = {
    "databricks": DatabricksSparkSource
    # Add other sources here, e.g. "file_system_csv": FileSystemCsvSource
}

# Initialize SourceRegistry for definitions
# Assuming 'source/' directory is relative to the execution path of this script
# or a well-known location. For robustness, this could be an env var or config.
SOURCE_DEFINITIONS_PATH = Path(os.getenv("FEATURE_PLATFORM_SOURCE_CATALOG_PATH", "source/"))
if not SOURCE_DEFINITIONS_PATH.exists() or not SOURCE_DEFINITIONS_PATH.is_dir():
    logger.warning(
        f"Source catalog directory '{SOURCE_DEFINITIONS_PATH.resolve()}' not found or not a directory. "
        "Source loading will likely fail. Set FEATURE_PLATFORM_SOURCE_CATALOG_PATH if needed."
    )
# Load definitions once at module level, or make it part of a class
try:
    SOURCE_CATALOG = SourceRegistry.from_yaml_dir(SOURCE_DEFINITIONS_PATH)
    logger.info(f"Loaded {len(SOURCE_CATALOG.get_all_source_definitions())} source definitions from {SOURCE_DEFINITIONS_PATH.resolve()}")
except Exception as e:
    logger.error(f"Failed to load source catalog from {SOURCE_DEFINITIONS_PATH.resolve()}: {e}", exc_info=True)
    SOURCE_CATALOG = SourceRegistry() # Empty registry to allow script to run but fail on source access


def create_source_from_job_config(job_input_source_config: JobInputSourceConfig, spark_manager: SparkSessionManager) -> SparkSource:
    """
    Instantiates a SparkSource based on job configuration by looking up details
    in the SourceRegistry (loaded from YAML catalog).
    """
    source_name = job_input_source_config.name
    source_version = job_input_source_config.version

    logger.info(f"Fetching source definition for: {source_name} (Version: {source_version or 'latest/default'})")
    source_def = SOURCE_CATALOG.get_source_definition(name=source_name, version=source_version)

    if not source_def:
        logger.error(f"Source definition not found for name='{source_name}', version='{source_version}'. "
                     f"Available sources: {[(s.name, s.version) for s in SOURCE_CATALOG.get_all_source_definitions()]}")
        raise ValueError(f"Source definition not found: {source_name} v{source_version}")

    source_type_from_catalog = source_def.type
    source_class = SOURCE_REGISTRY.get(source_type_from_catalog)

    if not source_class:
        logger.error(f"Unsupported source type '{source_type_from_catalog}' from catalog for '{source_name}'. "
                     f"Supported types in runner: {list(SOURCE_REGISTRY.keys())}")
        raise ValueError(f"Unsupported source type in runner: {source_type_from_catalog}")

    logger.info(f"Found source type '{source_type_from_catalog}' for '{source_name}', mapping to class {source_class.__name__}")

    # --- Prepare configuration for the specific source class ---
    if source_class == DatabricksSparkSource:
        if not isinstance(source_def.config, DatabricksSourceDetailConfig):
            raise TypeError(f"Expected DatabricksSourceDetailConfig for source '{source_name}', but found {type(source_def.config)}")
        
        db_detail_config: DatabricksSourceDetailConfig = source_def.config
        
        location_str: str
        # Default format, can be overridden if query implies a different handling
        # (though DatabricksSparkSource currently uses its config.format for spark.read.format)
        source_format = "delta" 

        if db_detail_config.query:
            location_str = db_detail_config.query
            # If location is a query, DatabricksSparkSource needs to handle it appropriately.
            # It might use spark.sql(location_str) instead of spark.read.format().load().
            # For now, we still pass a format; "sql" could be a convention.
            # DatabricksSparkSource's read method might need adjustment if format is "sql".
            source_format = "sql" # Conventional, DatabricksSparkSource needs to interpret this
            logger.info(f"Source '{source_name}' is a query. Location set to query string.")
        elif db_detail_config.table:
            if not db_detail_config.catalog or not db_detail_config.schema_name:
                raise ValueError(f"Databricks source '{source_name}' type 'table' requires catalog and schema.")
            location_str = f"{db_detail_config.catalog}.{db_detail_config.schema_name}.{db_detail_config.table}"
            logger.info(f"Source '{source_name}' is a table. Location: {location_str}")
        elif source_def.location: # Fallback to top-level location if provided
            location_str = source_def.location
            logger.info(f"Source '{source_name}' using top-level location: {location_str}")
        else:
            raise ValueError(f"Cannot determine location for Databricks source '{source_name}'. "
                             "Need 'query', 'table' (with catalog/schema), or top-level 'location'.")

        # Merge options: source_def options (if any) < job_input_source_config.load_params
        # Currently SourceDefinition.config (DatabricksSourceDetailConfig) doesn't have generic 'options'
        # DatabricksSparkSourceConfig expects 'options' for Spark read.
        # We use job_input_source_config.load_params as these runtime options.
        read_options = job_input_source_config.load_params or {}

        specific_source_config = DatabricksSparkSourceConfig(
            name=source_def.name,
            entity=source_def.entity,
            # 'type' for SparkSourceConfig often refers to its data type (delta, parquet)
            # For Databricks, this is typically 'delta', but could be other if location is a path to parquet etc.
            # source_def.type is "databricks". We use 'source_format' determined above.
            type=source_format, 
            location=location_str,
            format=source_format, # e.g. "delta", or "sql" if query
            options=read_options, # These are runtime read options from job config
            fields=source_def.fields or [], # Pass field definitions from catalog
            connection_config=None # Let DatabricksSparkSource use its default (env-based)
        )
        return DatabricksSparkSource(config=specific_source_config, spark_manager=spark_manager)
    else:
        # Fallback for other types - needs proper factory logic if more sources are added
        raise NotImplementedError(f"Source factory logic for {source_type_from_catalog} not fully implemented.")


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
            logger.info(f"Initializing input source: {job_config.input_source.name} (Version: {job_config.input_source.version})")
            source_instance = create_source_from_job_config(job_config.input_source, manager)
            
            # The read() method of DatabricksSparkSource already handles merging its config.options
            # with any runtime read_options passed to it. Since we've put job_input_source_config.load_params
            # into the config.options of the DatabricksSparkSourceConfig, we don't need to pass them again here.
            # However, if DatabricksSparkSource.read() was designed to *only* take read_options at runtime,
            # then we'd pass them: df = source_instance.read(read_options=job_config.input_source.load_params)
            # Given current DatabricksSparkSource, options are already in its config.
            logger.info(f"Reading data from source: {source_instance.config.name} (Location: {source_instance.config.location})")
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
        elif os.path.exists(minimal_config_path): # Corrected condition: if it exists and was not just created
             logger.info(f"Minimal config '{minimal_config_path}' already exists.")
        # No 'else' needed if dir couldn't be made, as that's logged above.

    elif os.path.exists(default_config_example): # Added check for existence before logging
        logger.info(f"Found default example config: {default_config_example}")
        logger.info(f"To run with it: python runner/execute_batch_job.py {default_config_example}")

    main()
