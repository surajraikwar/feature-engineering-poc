# Databricks Guide for Scala Feature Engineering Project

## 1. Overview

This guide provides instructions for running the Scala-based feature engineering jobs on Databricks. The main entry point for these jobs is the `com.example.featureplatform.runner.JobRunner` class. These jobs read source data, apply a series of feature transformations, and write the output, all defined by YAML configuration files.

## 2. Prerequisites

*   **Application JAR:** The Scala project must be packaged into an executable JAR file (a "fat JAR" or "uber JAR") using `sbt assembly`.
*   **Databricks Workspace:** An active Databricks workspace.
*   **Configuration Files:**
    *   Job configuration YAML files.
    *   Source catalog directory containing source definition YAML files.
    These files must be accessible from the Databricks environment (e.g., uploaded to DBFS or available via Databricks Repos).

## 3. Packaging the Application (JAR)

To package the application, navigate to the root of the `feature-engineering-scala` project in your terminal and run:

```bash
sbt assembly
```

This command compiles the Scala code and packages it into a single JAR file that includes all necessary dependencies (except those marked as "provided", like Spark and Delta, which are available on Databricks runtimes).

The output JAR will typically be found in:
`target/scala-2.12/feature-engineering-scala-assembly-0.1.0-SNAPSHOT.jar`
(The exact name may vary based on the project version and Scala version.)

## 4. Code Deployment / Library Installation on Databricks

The recommended way to deploy the Scala application JAR to Databricks is by uploading it to DBFS.

**Steps to Upload JAR to DBFS:**

1.  Navigate to your Databricks workspace.
2.  Go to "Data" (or "DBFS" in older UIs).
3.  Choose a location on DBFS to upload your JAR (e.g., `dbfs:/FileStore/jars/`).
4.  Upload the assembled JAR (e.g., `feature-engineering-scala-assembly-0.1.0-SNAPSHOT.jar`) using the Databricks UI or the Databricks CLI.
    *   Example using Databricks CLI:
        ```bash
        databricks fs cp target/scala-2.12/feature-engineering-scala-assembly-0.1.0-SNAPSHOT.jar dbfs:/FileStore/jars/feature-engineering-scala-assembly.jar
        ```

## 5. Cluster Configuration on Databricks

*   **Runtime:** Choose a Databricks Runtime that includes Scala 2.12 and a compatible Spark version (e.g., Spark 3.4 or newer). Databricks Runtime 13.x LTS or newer is recommended.
*   **Node Type:** Select appropriate node types based on your workload requirements (e.g., memory-optimized, compute-optimized).
*   **Libraries:**
    *   The application JAR uploaded to DBFS must be attached to the cluster.
        *   Go to your cluster configuration.
        *   Under the "Libraries" tab, click "Install New".
        *   Select "DBFS/S3" as the library source.
        *   Provide the path to your JAR on DBFS (e.g., `dbfs:/FileStore/jars/feature-engineering-scala-assembly.jar`).
    *   **Important:** Spark and Delta Lake dependencies are provided by the Databricks runtime. Ensure that these dependencies in your `build.sbt` are marked as `provided` (e.g., `"org.apache.spark" %% "spark-sql" % sparkVersion % "provided"`) when building the JAR for Databricks deployment. This prevents classpath conflicts.

## 6. Configuration File Management

Your job configuration files and source catalog directory need to be accessible from the Databricks job.

**Option A: Upload to DBFS (Recommended)**

1.  **Job Configurations:**
    *   Upload your job YAML files (e.g., `my_job.yaml`) to a location on DBFS.
    *   Example: `dbfs:/FileStore/feature-configs/jobs/my_job.yaml`
2.  **Source Catalog:**
    *   Upload your entire source catalog directory (e.g., the `source_definitions` directory containing `source1_v1.yaml`, etc.) to DBFS.
    *   Example: `dbfs:/FileStore/feature-configs/source_catalog/` (this path should point to the parent directory of your individual source definition files).

**Option B: Databricks Repos**

If your configuration files are managed within the same Git repository as your Scala code, you can use Databricks Repos.
1.  Clone your Git repository into your Databricks workspace using Repos.
2.  The paths to your configuration files will then be relative to the root of your repository when accessed from a job (e.g., `/Workspace/Repos/<your-repo-name>/src/test/resources/sample_job_config.yaml`).

## 7. Databricks Job Task Configuration

When creating a new job or editing an existing one in Databricks:

1.  **Type:** Select "JAR".
2.  **Main class:** Enter the fully qualified name of the main class:
    `com.example.featureplatform.runner.JobRunner`
3.  **JAR:**
    *   If you uploaded the JAR to DBFS, select "DBFS/S3" and provide the path (e.g., `dbfs:/FileStore/jars/feature-engineering-scala-assembly.jar`).
4.  **Parameters:**
    *   These are the command-line arguments passed to the `main` method of `JobRunner`. Provide them as a JSON array of strings.
    *   The `JobRunner` expects arguments in the following order:
        1.  Path to the job configuration YAML file.
        2.  Path to the root of the source catalog directory.
        3.  (Optional) Spark master URL (usually not needed on Databricks as the job runs on the cluster's Spark).

    *   **Example using DBFS paths:**
        ```json
        [
            "/dbfs/FileStore/feature-configs/jobs/your_job_name.yaml",
            "/dbfs/FileStore/feature-configs/source_catalog/"
        ]
        ```
    *   **Note:** When referencing files on DBFS, always use the `/dbfs/` prefix.

## 8. Spark Configuration in Databricks

*   The `SparkSessionManager` in the Scala application is designed to use the Spark session provided by the Databricks environment. You generally do not need to set a master URL.
*   Essential Spark configurations for Delta Lake (e.g., `spark.sql.extensions`, `spark.sql.catalog.spark_catalog`) are already set by the `SparkSessionManager`.
*   Any additional job-specific Spark configurations can be set in the "Spark Config" section of the Databricks cluster configuration or job configuration.

## 9. Logging

*   Application logs (using SLF4J, configured by `logback.xml` in the JAR) will be directed to the Spark driver logs.
*   You can view these logs in the Databricks UI:
    *   Navigate to your cluster.
    *   Go to "Driver Logs".
    *   Standard output and standard error streams, as well as logs from Log4j (which SLF4J is typically bridged to in Spark environments), will be available there.
*   The Spark UI can also be accessed for monitoring job stages and execution details.

This guide should help you deploy and run your Scala feature engineering jobs on Databricks effectively.
