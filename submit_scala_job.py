#!/usr/bin/env python3
"""
Script to build and submit a Scala job to Databricks.
"""

import os
import sys
import json
import logging
import requests
import subprocess
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('databricks_job_submission.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
VOLUME_BASE_PATH = "/Volumes/temp/feature_platform_testing/suraj"
JAR_FILE = "target/scala-2.12/feature-platform.jar"

def build_project() -> bool:
    """Build the Scala project using sbt assembly."""
    logger.info("Building Scala project...")
    try:
        result = subprocess.run(
            ["sbt", "assembly"],
            cwd=os.getcwd(),
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"Build failed: {result.stderr}")
            return False
            
        logger.info("Build completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error building project: {str(e)}")
        return False

def upload_file(host: str, token: str, local_path: str, remote_path: str) -> bool:
    """Upload a file to Databricks FileStore."""
    try:
        # Ensure host ends with a slash
        if not host.endswith('/'):
            host = f"{host}/"
            
        # First, delete the file if it exists
        response = requests.post(
            f"{host}api/2.0/workspace/delete",
            headers={"Authorization": f"Bearer {token}"},
            json={"path": remote_path}
        )
        
        # Create parent directories if they don't exist
        parent_dir = os.path.dirname(remote_path)
        if parent_dir:
            response = requests.post(
                f"{host}api/2.0/workspace/mkdirs",
                headers={"Authorization": f"Bearer {token}"},
                json={"path": parent_dir}
            )
            if response.status_code not in (200, 400):  # 400 means directory already exists
                logger.error(f"Failed to create directory {parent_dir}: {response.text}")
                return False
        
        # Read the file content
        with open(local_path, 'rb') as f:
            content = f.read()
        
        # Upload the file using workspace/import
        response = requests.post(
            f"{host}api/2.0/workspace/import",
            headers={"Authorization": f"Bearer {token}"},
            files={"files": (os.path.basename(remote_path), content)},
            data={
                "path": remote_path,
                "format": "AUTO",
                "overwrite": "true"
            }
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to upload file: {response.text}")
            return False
            
        logger.info(f"Successfully uploaded {local_path} to {remote_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error uploading file: {str(e)}", exc_info=True)
        return False

def submit_job(
    host: str,
    token: str,
    cluster_id: str,
    job_name: str,
    jar_path: str,
    job_config_path: str,
    main_class: str = "com.featureplatform.runner.FeatureJobRunner"
) -> Dict[str, Any]:
    """Submit a Scala job to Databricks."""
    try:
        # Upload job config to FileStore
        job_config_filename = os.path.basename(job_config_path)
        job_config_remote_path = f"/FileStore/feature-platform/jobs/{job_config_filename}"
        
        logger.info(f"Uploading job config: {job_config_path} -> {job_config_remote_path}")
        if not upload_file(host, token, job_config_path, job_config_remote_path):
            return {"error": "Failed to upload job config to FileStore"}
        
        # Upload JAR file to FileStore
        jar_remote_path = f"/FileStore/feature-platform/jars/{os.path.basename(jar_path)}"
        
        logger.info(f"Uploading JAR file: {jar_path} -> {jar_remote_path}")
        if not upload_file(host, token, jar_path, jar_remote_path):
            return {"error": "Failed to upload JAR file to FileStore"}
        
        # Prepare job configuration
        job_payload = {
            "run_name": job_name,
            "existing_cluster_id": cluster_id,
            "libraries": [
                {
                    "jar": f"dbfs:{jar_remote_path}"
                }
            ],
            "spark_jar_task": {
                "main_class_name": main_class,
                "parameters": [
                    f"dbfs:{job_config_remote_path}"
                ]
            },
            "max_retries": 1,
            "timeout_seconds": 3600,
            "spark_version": "12.2.x-scala2.12"
        }
        
        # Submit job
        logger.info("Submitting job to Databricks...")
        logger.debug(f"Job payload: {json.dumps(job_payload, indent=2)}")
        
        response = requests.post(
            f"{host}api/2.0/jobs/runs/submit",
            headers={"Authorization": f"Bearer {token}"},
            json=job_payload
        )
        
        logger.debug(f"Response status: {response.status_code}")
        logger.debug(f"Response content: {response.text}")
        
        if response.status_code != 200:
            return {
                "error": f"Failed to submit job: {response.text}",
                "status_code": response.status_code
            }
            
        result = response.json()
        run_id = result.get("run_id")
        
        logger.info(f"Job submitted successfully. Run ID: {run_id}")
        return {
            "run_id": run_id,
            "run_url": f"{host}?o=1234567890123456#job/{run_id}/run/1"
        }
        
    except Exception as e:
        logger.error(f"Error submitting job: {str(e)}", exc_info=True)
        return {"error": f"Error submitting job: {str(e)}"}

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Submit a Scala job to Databricks")
    parser.add_argument("--job-config", required=True, help="Path to the job configuration file")
    parser.add_argument("--job-name", required=True, help="Name of the job")
    parser.add_argument("--skip-build", action="store_true", help="Skip building the project")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Get environment variables
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
    
    if not all([host, token, cluster_id]):
        logger.error("Missing required environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID")
        sys.exit(1)
    
    # Build the project if not skipped
    if not args.skip_build:
        if not build_project():
            logger.error("Build failed, exiting...")
            sys.exit(1)
    
    # Verify JAR file exists
    if not os.path.exists(JAR_FILE):
        logger.error(f"JAR file not found at {JAR_FILE}. Build the project first or use --skip-build if already built.")
        sys.exit(1)
    
    # Submit the job
    result = submit_job(
        host=host,
        token=token,
        cluster_id=cluster_id,
        job_name=args.job_name,
        jar_path=JAR_FILE,
        job_config_path=args.job_config
    )
    
    if "error" in result:
        logger.error(f"Job submission failed: {result['error']}")
        sys.exit(1)
    
    logger.info(f"Job submitted successfully. Run URL: {result.get('run_url')}")

if __name__ == "__main__":
    main()
