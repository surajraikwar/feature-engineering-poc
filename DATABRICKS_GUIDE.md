# Running as a Databricks Job

This section provides guidance for setting up and running the Feature Platform as a job on Databricks.

## Overview

Running feature engineering pipelines as Databricks jobs offers several advantages:
*   **Leverages Cluster Resources**: Utilizes the scalable compute resources of a Databricks cluster.
*   **Avoids Local Issues**: Circumvents potential SSL/network configuration complexities often encountered with local Databricks Connect setups.
*   **Integration with Databricks Workflows**: Allows Feature Platform jobs to be easily scheduled and integrated into larger data processing workflows managed by Databricks.

The primary entry point for Databricks jobs is the `runner/databricks_job_main.py` script. This script is specifically designed to be run within the Databricks environment and correctly initializes the `SparkSessionManager` to use the existing Spark session provided by the Databricks job cluster.

## Code Deployment / Library Installation

There are several ways to make the `domain` code available to your Databricks job:

*   **Option A: Install from Git (Recommended via Databricks Repos)**
    *   **Databricks Repos**: The most straightforward method. Clone your `domain` repository directly into your Databricks workspace using Databricks Repos.
        *   The Databricks job can then be configured to execute the `runner/databricks_job_main.py` script directly from this cloned repository path (e.g., `/Workspace/Repos/<user_or_shared_folder>/domain/runner/databricks_job_main.py`).
        *   The Python import system within Databricks Repos typically handles the package's presence correctly if your scripts are run from the root of the cloned repo or if the repo root is added to `PYTHONPATH`.
    *   **Pip Install from Git**: Alternatively, you can install the package onto the job cluster via `pip` if the repository is accessible (e.g., a private repo with an access token or a public repo). This is done by adding a library to your cluster configuration or job task:
        ```
        pip install git+https://<your_git_provider.com>/<user_or_org>/domain.git@<branch_or_tag>#egg=domain
        ```
        Replace `<...>` placeholders with your specific Git repository URL and desired branch/tag.

*   **Option B: Build and Upload a Wheel**
    1.  **Build the Wheel Locally**:
        ```bash
        # Ensure 'build' package is installed: pip install build
        python -m build
        ```
        This command will generate a `.whl` file in the `dist/` directory (e.g., `dist/domain-0.1.0-py3-none-any.whl`).
    2.  **Upload the Wheel**:
        *   Upload the generated `.whl` file to DBFS (e.g., `dbfs:/FileStore/libraries/domain-0.1.0-py3-none-any.whl`).
        *   Install this wheel as a cluster library or as a job-dependent library.

*   **Option C: Syncing Code to DBFS (Less Common for Libraries)**
    *   While individual Python files or directories can be synced to DBFS (e.g., using `databricks fs cp ...`) and then added to the `PYTHONPATH` for a job, installing the code as a package (Option A or B) is generally preferred for better dependency management and cleaner execution.

## Cluster Configuration

*   **Runtime**: Choose a recent Databricks Runtime ML version. These runtimes come pre-packaged with many common ML libraries, including Spark, Delta Lake, and compatible versions of PySpark. Ensure the chosen runtime's Spark version aligns with any specific Spark features used by the platform (e.g., Spark 3.3+ for certain Delta Lake capabilities if those are critical).
*   **Node Type**: Select appropriate node types based on expected workload.
    *   General purpose: Standard series (e.g., D-series on Azure, m-series on AWS).
    *   Memory-intensive: Memory-optimized instances if dealing with very large DataFrames that cause memory pressure (though Spark is designed to spill to disk).
*   **Libraries**:
    *   **`domain` Package**: Ensure it's installed using one of the methods from "Code Deployment".
    *   **Other Dependencies**: The `domain` package specifies its core dependencies (like `pyyaml`, `pydantic`, `delta-spark`, `databricks-sql-connector`) in its `pyproject.toml` (or `setup.py`).
        *   If installing via `pip` (from Git or wheel), these should be pulled in automatically.
        *   `pyspark` is provided by the Databricks runtime itself and should not be installed separately.
        *   `delta-spark` is typically included in Databricks runtimes.
        *   Double-check if `pyyaml`, `pydantic`, and `databricks-sql-connector` need to be added as explicit cluster libraries if they are not correctly installed as dependencies of the main package. This is usually only an issue if not building/installing the package correctly.

## Configuration File Management

Your job configurations (`configs/jobs/*.yaml`) and Source Catalog definitions (`source/**/*.yaml`) need to be accessible by the Databricks job.

*   **Databricks Repos**: If you are using Databricks Repos to manage your `domain` code, the simplest approach is to include your `configs/` and `source/` directories within the same repository. The paths used in the job parameters can then be relative to the repository root.
    *   Example: If your repo is cloned as `/Workspace/Repos/my_user/my_domain_project/`, and `databricks_job_main.py` is run from the `my_domain_project/` root, paths like `configs/jobs/my_job.yaml` and `source/` would work.
*   **DBFS**: Alternatively, upload these configuration directories to DBFS.
    *   Example:
        *   Source Catalog: `dbfs:/FileStore/domain_configs/source/`
        *   Job Configs: `dbfs:/FileStore/domain_configs/configs/jobs/`
    *   You would then provide these DBFS paths as parameters to the job.

## Databricks Job Task Configuration

When setting up your Databricks job:

*   **Type**: Choose "Python file".
*   **Path/Source**:
    *   If using Databricks Repos: Provide the workspace path to `runner/databricks_job_main.py` within your cloned repository.
        *   Example: `/Workspace/Repos/<user_or_shared_folder>/domain/runner/databricks_job_main.py`
    *   If using a wheel and DBFS for the script: Provide the DBFS path if you've also uploaded the `runner` scripts there, or ensure the runner script is part of your library wheel if it's structured as an entry point (though `databricks_job_main.py` is a standalone script). Usually, if using a wheel, the script might be part of a package that's callable. For `databricks_job_main.py` as a script, Repos or DBFS path for the script itself is more common.
*   **Parameters**: These are passed as arguments to `runner/databricks_job_main.py`.
    *   **Example using DBFS paths for configs**:
        ```json
        [
            "--job-config-path", "/dbfs/FileStore/domain_configs/configs/jobs/sample_financial_features_job.yaml",
            "--source-catalog-path", "/dbfs:/FileStore/domain_configs/source/"
        ]
        ```
    *   **Example using relative paths (if running from a Databricks Repo checkout and configs are in the repo)**: Assuming the job's working directory is the root of the repo.
        ```json
        [
            "--job-config-path", "configs/jobs/sample_financial_features_job.yaml",
            "--source-catalog-path", "source/"
        ]
        ```
    *   The `--source-catalog-path` parameter is optional. If omitted, the `databricks_job_main.py` script will rely on the `FP_SOURCE_CATALOG_PATH` environment variable or the default path ("source/") used by `execute_batch_job.py` (which `databricks_job_main.py` calls internally). For clarity in Databricks jobs, explicitly providing this path is recommended.

## Environment Variables (Optional)

*   **`FP_SOURCE_CATALOG_PATH`**: Instead of using the `--source-catalog-path` script parameter, you can configure this as a Spark environment variable at the cluster level (under "Advanced Options" -> "Spark" in the cluster configuration UI).
    *   Example: `FP_SOURCE_CATALOG_PATH=/dbfs/FileStore/domain_configs/source/`
*   **Databricks Connection Variables**: When running directly on a Databricks cluster (not using Databricks Connect from an external client), environment variables like `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID`, `SPARK_REMOTE` are generally **not** needed for the `SparkSessionManager`, as it will detect the Databricks environment and use the managed Spark session. These are primarily for client-side connections.
```
