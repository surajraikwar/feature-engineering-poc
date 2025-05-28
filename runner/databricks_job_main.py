import argparse
import logging
import os
import sys
from pathlib import Path

# Assuming 'domain' is installed or in PYTHONPATH
from domain.jobs.config_loader import load_job_config, JobConfig
from domain.runner.execute_batch_job import run_feature_platform_job 
# Note: run_feature_platform_job is in execute_batch_job.py, which is in the same 'runner' directory.
# For Databricks execution, it's common to package the library and install it,
# or to ensure PYTHONPATH is set up correctly if running from a repo checkout.
# If execute_batch_job.py is intended to be a module, its name might need adjustment or an __init__.py in runner.
# For now, let's assume domain.runner.execute_batch_job is resolvable.

from domain.core.spark import SparkSessionManager

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

def main_databricks():
    """
    Main entry point for running Feature Platform jobs on Databricks.
    """
    parser = argparse.ArgumentParser(description="Execute a Feature Platform batch job on Databricks.")
    parser.add_argument(
        "--job-config-path",
        type=str,
        required=True,
        help="Path to the job configuration YAML file (e.g., DBFS path /dbfs/path/to/job.yaml or workspace path /Workspace/path/to/job.yaml)."
    )
    parser.add_argument(
        "--source-catalog-path",
        type=str,
        required=False, # Made optional as per instructions
        help="Optional: Path to the source catalog directory (e.g., /dbfs/path/to/source_catalog or /Workspace/path/to/source_catalog). If not provided, the default path in execute_batch_job.py will be used."
    )
    args = parser.parse_args()

    logger.info("Starting Databricks job for Feature Platform.")
    logger.info(f"Job configuration path: {args.job_config_path}")

    if args.source_catalog_path:
        os.environ["FP_SOURCE_CATALOG_PATH"] = args.source_catalog_path
        logger.info(f"Source catalog path explicitly set to: {args.source_catalog_path} via FP_SOURCE_CATALOG_PATH environment variable.")
    else:
        # Log the path that will be implicitly used by execute_batch_job.py if FP_SOURCE_CATALOG_PATH is not set
        # This assumes the default is "source/" relative to where execute_batch_job.py thinks its CWD is,
        # or where it resolves its default SOURCE_DEFINITIONS_PATH.
        # For clarity in Databricks jobs, providing --source-catalog-path is recommended.
        default_path_info = os.getenv("FP_SOURCE_CATALOG_PATH", "the default path configured within execute_batch_job.py (usually 'source/')")
        logger.info(f"Source catalog path not provided via argument. Using: {default_path_info}")


    try:
        job_config: JobConfig = load_job_config(args.job_config_path)
        logger.info(f"Successfully loaded job configuration for: {job_config.job_name}")
    except Exception as e:
        logger.error(f"Failed to load or validate job configuration from '{args.job_config_path}': {e}", exc_info=True)
        sys.exit(1)

    # Initialize SparkSessionManager.
    # When running on a Databricks cluster, SparkSessionManager is designed to use the existing Spark session.
    # No master_url is passed, allowing SparkSessionManager to auto-detect Databricks environment.
    manager = SparkSessionManager(app_name=job_config.job_name or "FeaturePlatformDatabricksJob")

    try:
        with manager as active_spark_session:
            logger.info(f"SparkSessionManager active. Spark version: {active_spark_session.version}")
            logger.info(f"Running job: {job_config.job_name}")
            
            run_feature_platform_job(job_config, manager)
            
            logger.info(f"Job '{job_config.job_name}' completed successfully.")
        sys.exit(0) # Successful execution
    except Exception as e:
        logger.error(f"An error occurred during the execution of job '{job_config.job_name}': {e}", exc_info=True)
        sys.exit(1) # Exit with error code

if __name__ == "__main__":
    main_databricks()
