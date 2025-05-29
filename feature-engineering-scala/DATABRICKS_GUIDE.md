# Databricks Guide for Scala Feature Engineering Project (Unity Catalog Focused)

## 1. Overview

This guide provides instructions for running the Scala-based feature engineering jobs on Databricks, with a focus on leveraging Unity Catalog (UC) for managing artifacts, configurations, and data. The main entry point for these jobs is the `com.example.featureplatform.runner.JobRunner` class. These jobs read source data, apply a series of feature transformations, and write the output, all defined by YAML configuration files.

## 2. Prerequisites

*   **Application JAR:** The Scala project must be packaged into an executable JAR file (a "fat JAR" or "uber JAR") using `sbt assembly`.
*   **Databricks Workspace:** An active Databricks workspace with Unity Catalog enabled.
*   **Unity Catalog Resources:**
    *   A Catalog and Schema created in UC for storing artifacts and configurations (e.g., `your_catalog.your_schema`).
    *   A UC Volume created within this schema for storing JARs and configuration files (e.g., `your_volume`).
*   **Configuration Files:**
    *   Job configuration YAML files.
    *   Source catalog directory containing source definition YAML files.
    These files must be accessible from the Databricks environment, ideally via UC Volumes.
*   **Permissions:** The principal (user or service principal) running the Databricks job will need appropriate UC permissions (e.g., `USE CATALOG`, `USE SCHEMA`, `READ VOLUME`, `WRITE VOLUME`, `CREATE TABLE`, `SELECT` on source tables, etc.).

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

The recommended way to deploy the Scala application JAR to Databricks when using Unity Catalog is by uploading it to a **UC Volume**.

**Steps to Upload JAR to a UC Volume:**

1.  Navigate to your Databricks workspace.
2.  Go to "Data" -> "Volumes".
3.  Select or create a suitable Volume (e.g., `your_catalog.your_schema.your_volume`).
4.  Create a directory within the Volume for JARs if desired (e.g., `jars/`).
5.  Upload the assembled JAR (e.g., `feature-engineering-scala-assembly-0.1.0-SNAPSHOT.jar`) to this Volume path using the Databricks UI ("Upload to this volume") or the Databricks CLI.
    *   Example path on UC Volume: `/Volumes/your_catalog/your_schema/your_volume/jars/feature-engineering-scala-assembly.jar`
    *   Example using Databricks CLI (note: syntax for UC Volumes might differ slightly, refer to Databricks docs for precise commands for volume uploads):
        ```bash
        # Ensure your CLI is configured for UC paths if applicable
        databricks fs cp target/scala-2.12/feature-engineering-scala-assembly-0.1.0-SNAPSHOT.jar /Volumes/your_catalog/your_schema/your_volume/jars/feature-engineering-scala-assembly.jar
        ```

## 5. Cluster Configuration on Databricks

*   **Runtime:** Choose a Databricks Runtime that is Unity Catalog enabled and includes Scala 2.12 and a compatible Spark version (e.g., Spark 3.4 or newer). Databricks Runtime 13.x LTS or newer is recommended.
*   **Access Mode:** The cluster must use an "User Isolation" or "Single User" access mode that is compatible with Unity Catalog. "No Isolation Shared" clusters have limitations with UC.
*   **Node Type:** Select appropriate node types based on your workload requirements.
*   **Libraries:**
    *   The application JAR uploaded to a UC Volume must be attached to the cluster.
        *   Go to your cluster configuration.
        *   Under the "Libraries" tab, click "Install New".
        *   Select "File path/ADLS Gen2" as the library source (this option is used for UC Volume paths).
        *   Provide the path to your JAR on the UC Volume (e.g., `/Volumes/your_catalog/your_schema/your_volume/jars/feature-engineering-scala-assembly.jar`).
    *   **Important:** Spark and Delta Lake dependencies are provided by the Databricks runtime. Ensure that these dependencies in your `build.sbt` are marked as `provided` (e.g., `"org.apache.spark" %% "spark-sql" % sparkVersion % "provided"`) when building the JAR for Databricks deployment. This prevents classpath conflicts.

## 6. Configuration File Management

Your job configuration files and source catalog directory need to be accessible from the Databricks job. Using **UC Volumes** is the recommended approach.

1.  **Job Configurations:**
    *   Upload your job YAML files (e.g., `my_job.yaml`) to a directory within your UC Volume.
    *   Example: `/Volumes/your_catalog/your_schema/your_volume/configs/jobs/my_job.yaml`
2.  **Source Catalog:**
    *   Upload your entire source catalog directory (e.g., the `source_definitions` directory containing `source1_v1.yaml`, etc.) to a directory within your UC Volume.
    *   Example: `/Volumes/your_catalog/your_schema/your_volume/source_catalog/` (this path should point to the parent directory of your individual source definition files).

**Alternative: Databricks Repos**
If your configuration files are managed within the same Git repository as your Scala code, you can use Databricks Repos. The job would then reference paths within the repo (e.g., `/Workspace/Repos/<your-repo-name>/configs/jobs/my_job.yaml`). These paths are also accessible via standard file system APIs on clusters with appropriate permissions.

## 7. Databricks Job Task Configuration

When creating a new job or editing an existing one in Databricks:

1.  **Type:** Select "JAR".
2.  **Main class:** Enter the fully qualified name of the main class:
    `com.example.featureplatform.runner.JobRunner`
3.  **JAR:**
    *   Select "File path" (or similar option that allows UC Volume paths) and provide the path to your JAR on the UC Volume (e.g., `/Volumes/your_catalog/your_schema/your_volume/jars/feature-engineering-scala-assembly.jar`).
4.  **Parameters:**
    *   These are the command-line arguments passed to the `main` method of `JobRunner`. Provide them as a JSON array of strings.
    *   The `JobRunner` expects arguments in the following order:
        1.  Path to the job configuration YAML file.
        2.  Path to the root of the source catalog directory.
        3.  (Optional) Spark master URL (usually not needed on Databricks).

    *   **Example using UC Volume paths:**
        ```json
        [
            "/Volumes/your_catalog/your_schema/your_volume/configs/jobs/your_job.yaml",
            "/Volumes/your_catalog/your_schema/your_volume/source_catalog/"
        ]
        ```

## 8. Data Sources and Sinks with Unity Catalog

*   **Source Definitions (`SourceDefinition` YAMLs):**
    *   When defining sources that are UC tables, use the three-level namespace: `catalog_name.schema_name.table_name`. This can be specified in the `config.table` field.
    *   For data stored in UC Volumes (e.g., Delta tables or Parquet files managed in a Volume), use the `/Volumes/catalog_name/schema_name/volume_name/path/to/data` path in the `location` field.
    *   The `DatabricksSparkSource` component is designed to work with both `spark.table("catalog.schema.table")` and `spark.read.format(...).load("/Volumes/...")`.
*   **Output Sinks (`JobConfig` YAML):**
    *   Similarly, for output sinks of type `delta` (or `delta_table`), the `path` in `output_sink.config` should point to a UC Volume path (e.g., `/Volumes/your_catalog/your_schema/your_volume/output_data/my_feature_table`) or a UC table name (`your_catalog.your_schema.my_feature_table`) if writing directly to a managed table. The `JobRunner` currently uses `.save(path)` which is suitable for path-based Delta tables. For writing to managed UC tables, ensure the path corresponds to the table's location or use `.saveAsTable("catalog.schema.table")`.

## 9. Unity Catalog Permissions

Ensure the compute cluster (or the user/service principal associated with the job) has the necessary permissions on the Unity Catalog securables:
*   **For JARs and Configurations on Volumes:**
    *   `USE CATALOG` on the target catalog.
    *   `USE SCHEMA` on the target schema.
    *   `READ VOLUME` on the volume storing JARs and configurations.
*   **For Data Sources:**
    *   `USE CATALOG` and `USE SCHEMA` for the data's catalog and schema.
    *   `SELECT TABLE` on source tables.
    *   `READ VOLUME` if reading files directly from a volume.
*   **For Output Sinks:**
    *   `USE CATALOG` and `USE SCHEMA` for the output location.
    *   `CREATE TABLE` if writing to new tables.
    *   `MODIFY` on existing tables if overwriting/appending.
    *   `WRITE VOLUME` if writing files/Delta tables to a volume.

## 10. Logging

*   Application logs (using SLF4J, configured by `logback.xml` in the JAR) will be directed to the Spark driver logs.
*   You can view these logs in the Databricks UI:
    *   Navigate to your cluster.
    *   Go to "Driver Logs".
    *   Standard output and standard error streams, as well as logs from Log4j (which SLF4J is typically bridged to in Spark environments), will be available there.
*   The Spark UI can also be accessed for monitoring job stages and execution details.

This guide should help you deploy and run your Scala feature engineering jobs on Databricks effectively.
