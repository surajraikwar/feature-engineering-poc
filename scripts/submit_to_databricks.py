"""
Script to submit a feature generation job to Databricks.
"""

import argparse
import json
import os
import requests
import logging
from pathlib import Path
import time
import base64

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_dbfs_directory(host, token, path):
    """Create a directory in DBFS if it doesn't exist."""
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }
    
    # Normalize path: remove leading/trailing slashes and prefix with /
    if path.startswith("/dbfs/"):
        path = path[5:]  # Remove /dbfs/ prefix
    elif path.startswith("dbfs:/"):
        path = path[6:]  # Remove dbfs:/ prefix
    
    if not path.startswith("/"):
        path = "/" + path
        
    url = f"{host}/api/2.0/dbfs/mkdirs"
    data = {"path": path}
    
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        logger.info(f"Created directory in DBFS: {path}")
    else:
        logger.warning(f"Failed to create directory in DBFS: {path}. Status: {response.status_code}, Response: {response.text}")


def upload_file_to_dbfs(host, token, local_path, dbfs_path, overwrite=True):
    """Upload a file to DBFS."""
    # Read the file content
    with open(local_path, 'rb') as file:
        content = file.read()
    
    # Normalize the DBFS path
    if dbfs_path.startswith("/dbfs/"):
        dbfs_path = dbfs_path[5:]  # Remove /dbfs/ prefix
    elif dbfs_path.startswith("dbfs:/"):
        dbfs_path = dbfs_path[6:]  # Remove dbfs:/ prefix
    
    if not dbfs_path.startswith("/"):
        dbfs_path = "/" + dbfs_path
    
    # Create parent directory if needed
    parent_dir = os.path.dirname(dbfs_path)
    if parent_dir:
        create_dbfs_directory(host, token, parent_dir)
    
    # Prepare headers for API call
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }
    
    # Prepare data for initial API call
    data = {
        'path': dbfs_path,
        'overwrite': overwrite,
    }
    
    # Create a handle for the upload
    create_response = requests.post(
        f"{host}/api/2.0/dbfs/create",
        headers=headers,
        json=data
    )
    
    if create_response.status_code != 200:
        logger.error(f"Failed to create DBFS handle. Status: {create_response.status_code}, Response: {create_response.text}")
        return False
    
    handle = create_response.json()['handle']
    
    # Upload the file in chunks
    chunk_size = 1024 * 1024  # 1 MB chunks
    for i in range(0, len(content), chunk_size):
        chunk = content[i:i + chunk_size]
        # Base64 encode the chunk
        encoded_chunk = base64.b64encode(chunk).decode('utf-8')
        
        # Prepare data for add-block API call
        block_data = {
            'handle': handle,
            'data': encoded_chunk,
        }
        
        # Add the block
        add_response = requests.post(
            f"{host}/api/2.0/dbfs/add-block",
            headers=headers,
            json=block_data
        )
        
        if add_response.status_code != 200:
            logger.error(f"Failed to add block to DBFS. Status: {add_response.status_code}, Response: {add_response.text}")
            return False
    
    # Close the handle
    close_data = {
        'handle': handle,
    }
    
    close_response = requests.post(
        f"{host}/api/2.0/dbfs/close",
        headers=headers,
        json=close_data
    )
    
    if close_response.status_code != 200:
        logger.error(f"Failed to close DBFS handle. Status: {close_response.status_code}, Response: {close_response.text}")
        return False
    
    logger.info(f"Successfully uploaded {local_path} to DBFS:{dbfs_path}")
    return True


def submit_job_to_databricks(host, token, cluster_id, job_name, job_config_path, source_catalog_path):
    """Submit a job to Databricks."""
    # Derive DBFS paths
    job_config_name = os.path.basename(job_config_path)
    dbfs_job_config_path = f"/dbfs/FileStore/feature-platform/configs/jobs/{job_config_name}"
    
    # Upload job config to DBFS
    upload_result = upload_file_to_dbfs(
        host=host,
        token=token,
        local_path=job_config_path,
        dbfs_path=dbfs_job_config_path
    )
    
    if not upload_result:
        logger.error("Failed to upload job config to DBFS.")
        return None
    
    # Prepare headers for API call
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }
    
    # Prepare the job definition
    job_data = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "feature_generation",
                "description": "Generate features from source data",
                "spark_python_task": {
                    "python_file": "dbfs:/FileStore/feature-platform/runner/databricks_job_main.py",
                    "parameters": [
                        "--job-config-path", dbfs_job_config_path.replace("/dbfs", "dbfs:"),
                        "--source-catalog-path", source_catalog_path.replace("/dbfs", "dbfs:")
                    ]
                },
                "existing_cluster_id": cluster_id
            }
        ]
    }
    
    # Create and run the job
    job_response = requests.post(
        f"{host}/api/2.1/jobs/create",
        headers=headers,
        json=job_data
    )
    
    if job_response.status_code != 200:
        logger.error(f"Failed to create job. Status: {job_response.status_code}, Response: {job_response.text}")
        return None
    
    job_id = job_response.json()['job_id']
    logger.info(f"Created job with ID: {job_id}")
    
    # Run the job
    run_data = {
        "job_id": job_id
    }
    
    run_response = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=headers,
        json=run_data
    )
    
    if run_response.status_code != 200:
        logger.error(f"Failed to run job. Status: {run_response.status_code}, Response: {run_response.text}")
        return None
    
    run_id = run_response.json()['run_id']
    logger.info(f"Started job run with ID: {run_id}")
    logger.info(f"Job run URL: {host}/#job/{job_id}/run/{run_id}")
    
    return run_id


def main():
    """Main function to submit a job to Databricks."""
    parser = argparse.ArgumentParser(description="Submit a feature generation job to Databricks.")
    parser.add_argument("--host", type=str, help="Databricks host URL")
    parser.add_argument("--token", type=str, help="Databricks API token")
    parser.add_argument("--cluster-id", type=str, help="Databricks cluster ID")
    parser.add_argument("--job-name", type=str, required=True, help="Name for the Databricks job")
    parser.add_argument("--job-config", type=str, required=True, help="Path to the job configuration YAML file")
    parser.add_argument("--source-catalog", type=str, required=True, help="Path to the source catalog directory")
    args = parser.parse_args()
    
    # Get credentials from environment if not provided
    host = args.host or os.getenv("DATABRICKS_HOST")
    token = args.token or os.getenv("DATABRICKS_TOKEN")
    cluster_id = args.cluster_id or os.getenv("DATABRICKS_CLUSTER_ID")
    
    if not host or not token or not cluster_id:
        logger.error("Missing Databricks credentials. Provide them as arguments or set environment variables.")
        return
    
    # Normalize paths
    job_config_path = os.path.abspath(args.job_config)
    source_catalog_path = os.path.abspath(args.source_catalog)
    
    # Submit the job
    run_id = submit_job_to_databricks(
        host=host,
        token=token,
        cluster_id=cluster_id,
        job_name=args.job_name,
        job_config_path=job_config_path,
        source_catalog_path=source_catalog_path
    )
    
    if run_id:
        logger.info(f"Job submitted successfully with run ID: {run_id}")
        logger.info(f"Check job status at: {host}/#job/{run_id}")
    else:
        logger.error("Failed to submit job to Databricks.")


if __name__ == "__main__":
    main()
