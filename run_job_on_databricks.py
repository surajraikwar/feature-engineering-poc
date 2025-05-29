#!/usr/bin/env python3
"""
Run Feature Platform Job on Databricks

This script provides a single entry point for:
1. Building the feature platform wheel file
2. Uploading necessary files to Databricks Volume
3. Submitting and monitoring the job

Usage:
    python run_job_on_databricks.py --job-config configs/jobs/your_job_config.yaml [options]

Environment variables required:
    DATABRICKS_HOST: Your Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
    DATABRICKS_TOKEN: Your Databricks access token
    DATABRICKS_CLUSTER_ID: ID of the cluster to run the job on
"""

import os
import sys
import time
import json
import yaml
import shutil
import logging
import argparse
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('databricks_job_runner.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
VOLUME_BASE_PATH = "/temp/feature_platform_testing/libraries"
WHEEL_FILE = "feature_platform-0.1.0-py3-none-any.whl"


def build_wheel() -> bool:
    """Build the wheel file for the feature platform."""
    logger.info("Building wheel file...")
    try:
        # Clean up previous builds
        if os.path.exists('build'):
            shutil.rmtree('build')
        if os.path.exists('dist'):
            shutil.rmtree('dist')
        
        # Build the wheel
        result = subprocess.run(
            [sys.executable, 'setup.py', 'bdist_wheel'],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            logger.error(f"Failed to build wheel: {result.stderr}")
            return False
            
        logger.info("Wheel file built successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error building wheel: {str(e)}", exc_info=True)
        return False


def upload_file(host: str, token: str, local_path: str, volume_path: str) -> bool:
    """Upload a file to Databricks Volume."""
    try:
        # Ensure volume_path is absolute and properly formatted
        if not volume_path.startswith(('dbfs:', '/Volumes/')):
            if not volume_path.startswith('/'):
                volume_path = f"{VOLUME_BASE_PATH.rstrip('/')}/{volume_path}"
            volume_path = f"/Volumes/{volume_path.lstrip('/')}"
            
        logger.debug(f"Preparing to upload {local_path} to {volume_path}")
        
        # Ensure the target directory exists in the volume
        dir_path = os.path.dirname(volume_path)
        if not dir_path.startswith('dbfs:'):
            dir_path = f"dbfs:{dir_path}"
            
        # The actual upload path should be in dbfs format
        upload_path = volume_path if volume_path.startswith('dbfs:') else f"dbfs:{volume_path}"
        
        logger.debug(f"Uploading {local_path} to {volume_path}")
        
        # Read file content
        with open(local_path, 'rb') as f:
            file_content = f.read()
        
        # Upload file using Files API
        api_url = f"{host}/api/2.0/fs/files{volume_path}?overwrite=true"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/octet-stream'
        }
        
        response = requests.put(api_url, headers=headers, data=file_content)
        
        # 200/201/204 are all success status codes for upload
        if response.status_code in (200, 201, 204):
            logger.info(f"Successfully uploaded {local_path} to {volume_path}")
            return True
        else:
            logger.error(f"Failed to upload {local_path}. Status: {response.status_code}, Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error uploading {local_path}: {str(e)}", exc_info=True)
        return False


def submit_job(
    host: str,
    token: str,
    cluster_id: str,
    job_name: str,
    job_config_path: str,
    source_catalog_path: Optional[str] = None
) -> Dict[str, Any]:
    """Submit a job to Databricks."""
    try:
        # Upload job config
        job_config_volume_path = f"jobs/{os.path.basename(job_config_path)}"
        if not upload_file(host, token, job_config_path, job_config_volume_path):
            return {"error": "Failed to upload job config to volume"}
        
        # Upload runner script
        runner_script_path = "runner/databricks_job_main.py"
        
        # Verify runner script exists locally
        if not os.path.exists(runner_script_path):
            error_msg = f"Runner script not found at {os.path.abspath(runner_script_path)}"
            logger.error(error_msg)
            return {"error": error_msg}
            
        logger.debug(f"Uploading runner script from {os.path.abspath(runner_script_path)}")
        if not upload_file(host, token, runner_script_path, runner_script_path):
            return {"error": "Failed to upload runner script to volume"}
            
        logger.debug(f"Runner script uploaded to {runner_script_path}")
        
        # Upload source catalog if provided
        if source_catalog_path and os.path.isdir(source_catalog_path):
            catalog_name = os.path.basename(source_catalog_path.rstrip('/'))
            catalog_volume_path = f"catalogs/{catalog_name}"
            
            # This is a simplified version - for directories, you'd need to implement a recursive upload
            logger.warning("Directory upload not fully implemented. Please ensure source catalog is already in the volume.")
        
        # Prepare job configuration
        job_payload = {
            "run_name": job_name,
            "existing_cluster_id": cluster_id,
            "libraries": [
                {
                    "whl": f"dbfs:{VOLUME_BASE_PATH.lstrip('/')}/{WHEEL_FILE}"
                }
            ],
            "spark_python_task": {
                "python_file": f"dbfs:{VOLUME_BASE_PATH.lstrip('/')}/{runner_script_path}",
                "parameters": [
                    "--job-config", f"dbfs:{VOLUME_BASE_PATH.lstrip('/')}/{job_config_volume_path}"
                ]
            },
            "python_named_params": {
                "python_version": "3"
            },
            "max_retries": 1,
            "timeout_seconds": 3600,
            "spark_python_version": "3"
        }
        
        # Submit job
        logger.info("Submitting job to Databricks...")
        logger.debug(f"Job payload: {json.dumps(job_payload, indent=2)}")
        
        try:
            response = requests.post(
                f"{host}/api/2.1/jobs/runs/submit",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json=job_payload,
                timeout=30
            )
            
            # Log the full response for debugging
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response content: {response.text}")
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Error submitting job: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}

        if response.status_code != 200:
            error_msg = f"Failed to submit job: {response.text}"
            logger.error(error_msg)
            return {"error": error_msg}
        
        run_id = response.json().get("run_id")
        if not run_id:
            error_msg = f"No run_id in response: {response.json()}"
            logger.error(error_msg)
            return {"error": error_msg}
            
        job_url = f"{host}/#job/{run_id}"
        logger.info(f"Job submitted successfully with run_id: {run_id}")
        logger.info(f"View job run at: {job_url}")
        
        return {
            "run_id": run_id,
            "job_url": job_url,
            "success": True
        }
        
    except Exception as e:
        error_msg = f"Error submitting job: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": error_msg}


def main() -> int:
    """Main function to handle command line arguments and execute the workflow."""
    parser = argparse.ArgumentParser(description='Run Feature Platform Job on Databricks')
    
    # Required arguments
    parser.add_argument('--job-config', 
                      type=str, 
                      required=True, 
                      help='Path to the job configuration YAML file')
    
    # Optional arguments
    parser.add_argument('--job-name',
                      type=str,
                      help='Name of the job (default: derived from config filename)')
    
    parser.add_argument('--source-catalog',
                      type=str,
                      help='Path to the source catalog directory (optional)')
    
    parser.add_argument('--skip-wheel-build',
                      action='store_true',
                      help='Skip building the wheel file (assumes it exists in dist/)')
    
    parser.add_argument('--debug',
                      action='store_true',
                      help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Set log level
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # Get environment variables
    host = os.getenv('DATABRICKS_HOST')
    token = os.getenv('DATABRICKS_TOKEN')
    cluster_id = os.getenv('DATABRICKS_CLUSTER_ID')
    
    # Validate required environment variables
    missing_vars = []
    if not host:
        missing_vars.append('DATABRICKS_HOST')
    if not token:
        missing_vars.append('DATABRICKS_TOKEN')
    if not cluster_id:
        missing_vars.append('DATABRICKS_CLUSTER_ID')
    
    if missing_vars:
        error_msg = f"Error: The following required environment variables are not set: {', '.join(missing_vars)}"
        logger.error(error_msg)
        return 1
    
    # Set job name if not provided
    job_name = args.job_name
    if not job_name:
        job_name = os.path.splitext(os.path.basename(args.job_config))[0]
        job_name = job_name.replace('_', ' ').title()
    
    logger.info(f"Starting job: {job_name}")
    
    # Build wheel if not skipped
    if not args.skip_wheel_build:
        if not build_wheel():
            return 1
    
    # Upload wheel file
    wheel_path = f"dist/{WHEEL_FILE}"
    if not os.path.exists(wheel_path):
        logger.error(f"Wheel file not found at {wheel_path}. Build it first or use --skip-wheel-build if already built.")
        return 1
    
    if not upload_file(host, token, wheel_path, WHEEL_FILE):
        logger.error("Failed to upload wheel file")
        return 1
    
    # Submit the job
    result = submit_job(
        host=host,
        token=token,
        cluster_id=cluster_id,
        job_name=job_name,
        job_config_path=args.job_config,
        source_catalog_path=args.source_catalog
    )
    
    if "error" in result:
        logger.error(f"Job submission failed: {result['error']}")
        return 1
    
    logger.info(f"Job submitted successfully. Run ID: {result.get('run_id')}")
    logger.info(f"View job at: {result.get('job_url')}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
