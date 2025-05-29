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
def submit_job_to_databricks(host: str, token: str, cluster_id: str, job_name: str, job_config_path: str, source_catalog_path: str) -> dict:
    """
    Uploads necessary project artifacts to a Databricks Unity Catalog Volume,
    then creates and runs a new job on Databricks using these artifacts.
    """
    logger.info("Starting job submission process to Databricks...")

    # 1. Define UC Volume Base Path
    UC_VOLUME_BASE_PATH = os.environ.get("DATABRICKS_UC_VOLUME_PATH", "/Volumes/main/default/feature_platform_vol")
    if not UC_VOLUME_BASE_PATH.startswith("/Volumes/"):
        logger.error(f"DATABRICKS_UC_VOLUME_PATH (or default) '{UC_VOLUME_BASE_PATH}' must start with /Volumes/.")
        return {"error": "Invalid DATABRICKS_UC_VOLUME_PATH configuration."}
    logger.info(f"Using UC Volume base path: {UC_VOLUME_BASE_PATH}")

    # 2. Build Wheel Prerequisite Logging
    logger.info("This script expects the project wheel to be built in dist/ (e.g., using 'python -m build').")

    # 3. Define Local and Target Volume Paths for Artifacts
    repo_root = Path(__file__).parent.parent # Assuming scripts/ is one level down from repo root
    
    local_runner_script = repo_root / "runner" / "databricks_job_main.py"
    if not local_runner_script.exists():
        logger.error(f"Local runner script not found: {local_runner_script}")
        return {"error": "Local runner script not found."}

    dist_dir = repo_root / "dist"
    local_wheel_files = list(dist_dir.glob("*.whl"))
    if not local_wheel_files:
        logger.error(f"No wheel file found in {dist_dir}. Please build the project first.")
        return {"error": "Wheel file not found."}
    local_wheel_path = local_wheel_files[0] # Take the first one found
    logger.info(f"Found local wheel: {local_wheel_path}")

    # job_config_path and source_catalog_path are provided as arguments (absolute paths)
    local_job_config_path = Path(job_config_path)
    if not local_job_config_path.exists():
        logger.error(f"Local job config file not found: {local_job_config_path}")
        return {"error": "Local job config file not found."}

    local_source_catalog_dir = Path(source_catalog_path)
    if not local_source_catalog_dir.is_dir():
        logger.error(f"Local source catalog path is not a directory: {local_source_catalog_dir}")
        return {"error": "Local source catalog path is not a directory."}

    # Define target Volume paths
    target_volume_runner_script = f"{UC_VOLUME_BASE_PATH}/runner/databricks_job_main.py"
    target_volume_wheel = f"{UC_VOLUME_BASE_PATH}/libs/{local_wheel_path.name}"
    target_volume_job_config = f"{UC_VOLUME_BASE_PATH}/configs/jobs/{local_job_config_path.name}"
    # upload_directory_to_volume uploads local_source_catalog_dir into target_volume_source_catalog_parent
    # e.g. if local_source_catalog_dir is 'source', it becomes /Volumes/.../source
    target_volume_source_catalog_parent = UC_VOLUME_BASE_PATH 
    # The path used in job parameters will be UC_VOLUME_BASE_PATH + local_source_catalog_dir.name
    target_volume_source_catalog_for_job_param = f"{UC_VOLUME_BASE_PATH}/{local_source_catalog_dir.name}"


    # 4. Upload Artifacts to UC Volume
    logger.info(f"Uploading runner script {local_runner_script} to {target_volume_runner_script}...")
    if not upload_file_to_volume(host, token, str(local_runner_script), target_volume_runner_script):
        logger.error("Failed to upload runner script.")
        return {"error": "Failed to upload runner script."}

    logger.info(f"Uploading job config {local_job_config_path} to {target_volume_job_config}...")
    if not upload_file_to_volume(host, token, str(local_job_config_path), target_volume_job_config):
        logger.error("Failed to upload job config.")
        return {"error": "Failed to upload job config."}

    logger.info(f"Uploading wheel {local_wheel_path} to {target_volume_wheel}...")
    if not upload_file_to_volume(host, token, str(local_wheel_path), target_volume_wheel):
        logger.error("Failed to upload wheel.")
        return {"error": "Failed to upload wheel."}
    
    logger.info(f"Uploading source catalog directory {local_source_catalog_dir} to parent {target_volume_source_catalog_parent}...")
    if not upload_directory_to_volume(host, token, str(local_source_catalog_dir), target_volume_source_catalog_parent):
        logger.error("Failed to upload source catalog directory.")
        return {"error": "Failed to upload source catalog directory."}

    # 5. Construct Job Definition JSON for /api/2.1/jobs/create
    job_task_key = f"{job_name}_task"
    job_data = {
        "name": job_name,
        "tasks": [
            {
                "task_key": job_task_key,
                "spark_python_task": {
                    "python_file": target_volume_runner_script, # Full UC Volume path
                    "parameters": [
                        "--job-config-path",
                        target_volume_job_config, # Full UC Volume path
                        "--source-catalog-path",
                        target_volume_source_catalog_for_job_param # Full UC Volume path
                    ]
                },
                "existing_cluster_id": cluster_id,
                "libraries": [
                    {"whl": target_volume_wheel} # Full UC Volume path
                ]
            }
        ],
        "format": "MULTI_TASK" # Required for new job format
    }

    # 6. API Call to Create Job
    create_job_api_url = f"{host}/api/2.1/jobs/create"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    
    logger.info(f"Creating job '{job_name}' with configuration: {json.dumps(job_data, indent=2)}")
    try:
        response_create = requests.post(create_job_api_url, headers=headers, json=job_data)
        response_create.raise_for_status()
        job_id = response_create.json().get("job_id")
        logger.info(f"Job '{job_name}' created successfully. Job ID: {job_id}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTPError creating job: {e}. Response: {e.response.text if e.response else 'No response text'}")
        return {"error": f"HTTPError creating job: {e.response.text if e.response else str(e)}"}
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException creating job: {e}")
        return {"error": f"RequestException creating job: {str(e)}"}

    if not job_id:
        logger.error("Failed to create job or retrieve job_id.")
        return {"error": "Failed to create job or retrieve job_id."}

    # 7. API Call to Run Job
    run_now_api_url = f"{host}/api/2.1/jobs/run-now"
    run_payload = {"job_id": job_id}
    logger.info(f"Triggering run for job ID {job_id} with payload: {json.dumps(run_payload, indent=2)}")
    try:
        response_run = requests.post(run_now_api_url, headers=headers, json=run_payload)
        response_run.raise_for_status()
        run_info = response_run.json()
        logger.info(f"Job run triggered successfully. Run ID: {run_info.get('run_id')}, Number in Job: {run_info.get('number_in_job')}")
        return run_info # Contains run_id, number_in_job etc.
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTPError running job: {e}. Response: {e.response.text if e.response else 'No response text'}")
        return {"error": f"HTTPError running job: {e.response.text if e.response else str(e)}", "job_id": job_id}
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException running job: {e}")
        return {"error": f"RequestException running job: {str(e)}", "job_id": job_id}


# Placeholder for main execution logic
def main():
    parser = argparse.ArgumentParser(description="Submit files and jobs to Databricks.")
    parser.add_argument('--host', required=True, help="Databricks workspace host (e.g., https://your-workspace.databricks.com)")
    parser.add_argument('--token', required=True, help="Databricks Personal Access Token")
    parser.add_argument('--local-path', help="Local file or directory path to upload.")
    parser.add_argument('--volume-path', help="Target Unity Catalog Volume path (e.g., /Volumes/catalog/schema/volume/path/to/file_or_dir).")
    parser.add_argument('--cluster-id', required=True, help="Databricks Cluster ID to run the job on.")
    parser.add_argument('--job-name', required=True, help="Name for the Databricks job.")
    parser.add_argument('--job-config-path', required=True, help="Path to the local job configuration YAML file.")
    parser.add_argument('--source-catalog-path', required=True, help="Path to the local source catalog directory (e.g., 'source').")
    # --local-path and --volume-path are removed as uploads are now handled by submit_job_to_databricks
    parser.add_argument('--overwrite', action='store_true', help="Overwrite existing files in the Volume during artifact upload.")


    args = parser.parse_args()

    logger.info("Starting Databricks job submission process...")

    # The job_config_path and source_catalog_path from args are already absolute after main uses os.path.abspath
    # However, submit_job_to_databricks will re-evaluate based on its own location if not absolute.
    # For clarity and robustness, ensure they are absolute before passing.
    abs_job_config_path = os.path.abspath(args.job_config_path)
    abs_source_catalog_path = os.path.abspath(args.source_catalog_path)


    if not os.path.exists(abs_job_config_path):
        logger.error(f"Job config file not found: {abs_job_config_path}")
        return
    if not os.path.isdir(abs_source_catalog_path): # Check if it's a directory
        logger.error(f"Source catalog path is not a valid directory: {abs_source_catalog_path}")
        return

    job_submission_result = submit_job_to_databricks(
        host=args.host,
        token=args.token,
        cluster_id=args.cluster_id,
        job_name=args.job_name,
        job_config_path=abs_job_config_path,
        source_catalog_path=abs_source_catalog_path
        # Overwrite for uploads is True by default in upload functions, could pass args.overwrite if needed
    )

    if job_submission_result and job_submission_result.get("run_id"):
        logger.info(f"Job submitted and run successfully. Run ID: {job_submission_result.get('run_id')}, Job ID: {job_submission_result.get('job_id')}")
    elif job_submission_result and job_submission_result.get("job_id"):
        logger.warning(f"Job created with ID: {job_submission_result.get('job_id')}, but failed to start run or run info not available. Error: {job_submission_result.get('error')}")
    else:
        logger.error(f"Job submission failed. Result: {job_submission_result}")
    
    logger.info("Databricks submission process finished.")

if __name__ == "__main__":
                logger.error(f"Failed to load or submit job config {args.job_config_path}: {e}")
    
    logger.info("Databricks submission process finished.")

if __name__ == "__main__":
    main()
