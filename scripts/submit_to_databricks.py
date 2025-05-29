import requests
import logging
import os
import argparse
import json
import time # Keep time for potential polling logic later
from pathlib import Path # Added for pathlib.Path
# import base64 # No longer needed for upload_file_to_volume

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Placeholder for now, might be adapted or removed later
def create_dbfs_directory(host: str, token: str, dbfs_path: str) -> bool:
    """
    Creates a directory in DBFS if it doesn't already exist.
    (This function might be adapted for Volumes or become less relevant if PUT creates dirs)
    """
    api_url = f"{host}/api/2.0/dbfs/mkdirs"
    headers = {'Authorization': f'Bearer {token}'}
    payload = {'path': dbfs_path}
    
    try:
        response = requests.post(api_url, headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f"Directory '{dbfs_path}' created or already exists.")
            return True
        else:
            logger.error(f"Failed to create directory '{dbfs_path}'. Status: {response.status_code}, Response: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Error creating directory '{dbfs_path}': {e}")
        return False

def upload_file_to_volume(host: str, token: str, local_path: str, volume_path: str, overwrite: bool = True) -> bool:
    """
    Uploads a local file to a Databricks Unity Catalog Volume using a single PUT request.

    Args:
        host: Databricks workspace host URL (e.g., https://your-workspace.databricks.com).
        token: Databricks Personal Access Token.
        local_path: Path to the local file to upload.
        volume_path: Full path within the Unity Catalog Volume 
                     (e.g., /Volumes/catalog_name/schema_name/volume_name/path/to/file.ext).
        overwrite: Whether to overwrite the file if it already exists on the Volume.

    Returns:
        True if upload was successful, False otherwise.
    """
    if not os.path.exists(local_path):
        logger.error(f"Local file not found: {local_path}")
        return False

    # Ensure volume_path starts with /Volumes/ as per Databricks API requirements for /api/2.0/fs/files
    # The /api/2.0/fs/files endpoint directly maps to the workspace filesystem,
    # and for volumes, this means the path must start with /Volumes/
    if not volume_path.startswith("/Volumes/"):
        logger.error(f"Invalid volume_path: '{volume_path}'. Must start with /Volumes/.")
        # Example: /Volumes/main/default/my_volume/path/to/file.txt
        # Forcing it here can be an option, but it's better if caller provides it correctly.
        # However, to be robust, let's try to prefix if a common mistake is made (e.g. missing leading /)
        # For this implementation, strict check is better as per prompt.
        return False

    api_url = f"{host}/api/2.0/fs/files{volume_path}" 
    
    if overwrite:
        api_url += "?overwrite=true"

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/octet-stream'
    }

    try:
        with open(local_path, 'rb') as f:
            file_content = f.read()

        logger.info(f"Uploading {local_path} to Volume: {api_url} ({len(file_content)} bytes)...")
        response = requests.put(api_url, headers=headers, data=file_content)

        # Successful upload can be 201 (Created) or 200 (OK, if overwritten)
        if response.status_code == 201 or response.status_code == 200:
            logger.info(f"Successfully uploaded {local_path} to Volume path: {volume_path}. Status: {response.status_code}")
            return True
        else:
            logger.error(f"Failed to upload {local_path} to {volume_path}. Status: {response.status_code}, Response: {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException while uploading {local_path} to {volume_path}: {e}")
        return False
    except IOError as e:
        logger.error(f"IOError reading local file {local_path}: {e}")
        return False

def upload_directory_to_volume(host: str, token: str, local_dir_path: str, target_volume_parent_dir: str, overwrite: bool = True) -> bool:
    """
    Uploads all files from a local directory to a target directory within a Databricks Unity Catalog Volume.
    The source directory itself will be created under target_volume_parent_dir.

    Args:
        host: Databricks workspace host URL.
        token: Databricks Personal Access Token.
        local_dir_path: Path to the local directory to upload.
        target_volume_parent_dir: The parent directory path in the Unity Catalog Volume 
                                  (e.g., /Volumes/catalog_name/schema_name/volume_name).
                                  The local_dir_path's basename will be appended to this.
        overwrite: Whether to overwrite files if they already exist in the Volume.

    Returns:
        True if all files were uploaded successfully, False otherwise.
    """
    local_dir = Path(local_dir_path)
    if not local_dir.exists() or not local_dir.is_dir():
        logger.error(f"Local directory not found or is not a directory: {local_dir_path}")
        return False

    if not target_volume_parent_dir.startswith("/Volumes/"):
        logger.error(f"Invalid target_volume_parent_dir: '{target_volume_parent_dir}'. Must start with /Volumes/.")
        return False
    
    target_volume_base_path = Path(target_volume_parent_dir)

    logger.info(f"Starting upload of directory '{local_dir_path}' to Volume path '{target_volume_base_path / local_dir.name}'")

    for local_file_path in local_dir.rglob("*"):
        if local_file_path.is_file():
            relative_path = local_file_path.relative_to(local_dir)
            # Construct target path: /Volumes/catalog/schema/volume/local_dir_name/relative/path/to/file.ext
            target_volume_file_path = target_volume_base_path / local_dir.name / relative_path
            
            logger.info(f"Attempting to upload file '{local_file_path}' to '{target_volume_file_path}'")
            
            success = upload_file_to_volume(
                host=host,
                token=token,
                local_path=str(local_file_path),
                volume_path=str(target_volume_file_path),
                overwrite=overwrite
            )
            if not success:
                logger.error(f"Failed to upload file '{local_file_path}' to '{target_volume_file_path}'. Aborting directory upload.")
                return False
    
    logger.info(f"Successfully uploaded all files from directory '{local_dir_path}' to Volume path '{target_volume_base_path / local_dir.name}'.")
    return True

# Refactored submit_job_to_databricks
def submit_job_to_databricks(host: str, token: str, cluster_id: str, job_name: str, 
                          job_config_path: str, source_catalog_path: str = None, 
                          dbfs_job_config_path: str = None) -> dict:
    """
    Submits a job to Databricks with the specified configuration.
    
    Args:
        host: Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
        token: Databricks access token
        cluster_id: ID of the cluster to run the job on
        job_name: Name for the job
        job_config_path: Local path to the job configuration YAML file
        source_catalog_path: Path to the source catalog directory (optional)
        dbfs_job_config_path: DBFS path where the job config should be stored (optional)
        
    Returns:
        dict: A dictionary containing the job submission result or error information
    """
    logger.info("Starting job submission to Databricks...")
    
    try:
        # Convert relative paths to absolute paths
        job_config_path = os.path.abspath(job_config_path)
        
        # Validate job config file exists
        if not os.path.isfile(job_config_path):
            error_msg = f"Job config file not found: {job_config_path}"
            logger.error(error_msg)
            return {"error": error_msg}
        
        # Validate source catalog directory if provided
        if source_catalog_path:
            source_catalog_path = os.path.abspath(source_catalog_path)
            if not os.path.isdir(source_catalog_path):
                error_msg = f"Source catalog directory not found: {source_catalog_path}"
                logger.error(error_msg)
                return {"error": error_msg}
        
        # Set default DBFS path if not provided
        if not dbfs_job_config_path:
            dbfs_job_config_path = f"/FileStore/feature-platform/configs/jobs/{os.path.basename(job_config_path)}"
        
        # Define DBFS paths
        dbfs_runner_script = "/FileStore/feature-platform/runner/databricks_job_main.py"
        dbfs_source_catalog = "/FileStore/feature-platform/source"


        # Upload job config to DBFS
        logger.info(f"Uploading job config {job_config_path} to {dbfs_job_config_path}...")
        try:
            # Create parent directory if it doesn't exist
            dbfs_config_dir = os.path.dirname(dbfs_job_config_path)
            create_dbfs_directory(host, token, dbfs_config_dir)
            
            # Upload the file
            if not upload_file_to_volume(host, token, job_config_path, dbfs_job_config_path):
                error_msg = "Failed to upload job config to DBFS"
                logger.error(error_msg)
                return {"error": error_msg}
        except Exception as e:
            error_msg = f"Error uploading job config: {str(e)}"
            logger.error(error_msg)
            return {"error": error_msg}
        
        # Upload source catalog if provided
        if source_catalog_path:
            logger.info(f"Uploading source catalog from {source_catalog_path} to {dbfs_source_catalog}...")
            try:
                # Create target directory
                create_dbfs_directory(host, token, dbfs_source_catalog)
                
                # Upload directory contents
                if not upload_directory_to_volume(host, token, source_catalog_path, dbfs_source_catalog):
                    error_msg = "Failed to upload source catalog to DBFS"
                    logger.error(error_msg)
                    return {"error": error_msg}
            except Exception as e:
                error_msg = f"Error uploading source catalog: {str(e)}"
                logger.error(error_msg)
                return {"error": error_msg}
        
        # Prepare the job payload
        job_payload = {
            "run_name": job_name,
            "existing_cluster_id": cluster_id,
            "libraries": [
                {
                    "jar": "dbfs:/FileStore/feature-platform/lib/feature-platform-assembly-0.1.0-SNAPSHOT.jar"
                },
                {
                    "pypi": {
                        "package": "pydantic<2.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "pyyaml"
                    }
                }
            ],
            "spark_python_task": {
                "python_file": dbfs_runner_script,
                "parameters": [
                    "--job-config",
                    dbfs_job_config_path
                ]
            },
            "max_retries": 1,
            "timeout_seconds": 0,
            "email_notifications": {}
        }
        
        # Add source catalog to parameters if provided
        if source_catalog_path:
            job_payload["spark_python_task"]["parameters"].extend([
                "--source-catalog",
                dbfs_source_catalog
            ])

        # Submit the job to Databricks
        run_now_url = f"{host}/api/2.1/jobs/runs/submit"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        try:
            logger.info("Submitting job to Databricks...")
            logger.debug(f"Job payload: {json.dumps(job_payload, indent=2)}")
            
            response = requests.post(run_now_url, headers=headers, json=job_payload)
            response.raise_for_status()
            
            result = response.json()
            run_id = result.get('run_id')
            
            if not run_id:
                error_msg = f"Failed to get run_id from response: {result}"
                logger.error(error_msg)
                return {"error": error_msg}
                
            logger.info(f"Job submitted successfully with run_id: {run_id}")
            job_url = f"{host}/#job/{run_id}/run/{run_id}"
            logger.info(f"View the job run at: {job_url}")
            
            return {
                "run_id": run_id,
                "job_url": job_url,
                "success": True
            }
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Error submitting job to Databricks: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details = e.response.json()
                    error_msg += f"\nError details: {json.dumps(error_details, indent=2)}"
                except:
                    error_msg += f"\nResponse: {e.response.text}"
            logger.error(error_msg)
            return {"error": error_msg}
        
    except Exception as e:
        error_msg = f"Unexpected error in job submission: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": error_msg}

def main():
    """
    Main function to handle command line arguments and submit the job.
    
    Usage:
        python submit_to_databricks.py --job-name NAME --job-config PATH [--source-catalog PATH] [--dbfs-job-config PATH]
    
    Environment variables required:
        DATABRICKS_HOST: Your Databricks workspace URL (e.g., https://your-workspace.cloud.databricks.com)
        DATABRICKS_TOKEN: Your Databricks access token
        DATABRICKS_CLUSTER_ID: ID of the cluster to run the job on
    """
    parser = argparse.ArgumentParser(description='Submit a job to Databricks')
    
    # Required arguments
    parser.add_argument('--job-name', 
                       type=str, 
                       required=True, 
                       help='Name of the job')
    
    parser.add_argument('--job-config', 
                       type=str, 
                       required=True, 
                       help='Local path to the job configuration YAML file')
    
    # Optional arguments
    parser.add_argument('--source-catalog', 
                       type=str, 
                       help='Path to the source catalog directory (optional)')
    
    parser.add_argument('--dbfs-job-config', 
                       type=str, 
                       help='DBFS path where the job config should be stored (optional)')
    
    parser.add_argument('--debug', 
                       action='store_true',
                       help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('databricks_job_submission.log')
        ]
    )
    
    logger = logging.getLogger(__name__)
    
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
        print(error_msg)
        logger.error(error_msg)
        return 1
    
    logger.info(f"Starting job submission: {args.job_name}")
    
    try:
        # Submit the job to Databricks
        result = submit_job_to_databricks(
            host=host,
            token=token,
            cluster_id=cluster_id,
            job_name=args.job_name,
            job_config_path=args.job_config,
            source_catalog_path=args.source_catalog,
            dbfs_job_config_path=args.dbfs_job_config
        )
        
        # Handle the result
        if "error" in result:
            error_msg = f"Job submission failed: {result['error']}"
            print(f"Error: {error_msg}")
            logger.error(error_msg)
            return 1
        else:
            success_msg = f"Job submitted successfully with run ID: {result.get('run_id')}"
            print(success_msg)
            if 'job_url' in result:
                print(f"View the job run at: {result['job_url']}")
            logger.info(success_msg)
            return 0
            
    except Exception as e:
        error_msg = f"Unexpected error during job submission: {str(e)}"
        print(f"Error: {error_msg}")
        logger.exception(error_msg)
        return 1

if __name__ == "__main__":
    sys.exit(main())
