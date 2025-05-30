# Scala Feature Engineering Platform

## Overview

This project provides a configuration-driven framework for executing feature engineering pipelines using Apache Spark, implemented in Scala. It is designed to be a robust and scalable solution for defining, managing, and applying feature transformations to various data sources. This project is a Scala-based successor to a similar Python implementation, aiming to leverage Scala's strengths for Spark-based data processing.

The platform reads job definitions and source specifications from YAML configuration files, processes data using Spark DataFrames, applies a series of defined feature transformations, and writes the results to specified output sinks.

## Project Structure

A high-level overview of the project directory structure:

```
feature-engineering-scala/
├── build.sbt                   # SBT build definition file
├── project/                    # SBT plugins (e.g., sbt-assembly)
├── configs/                    # Example configuration directory (not part of JAR)
│   ├── jobs/                   # Example job YAML configurations
│   │   └── sample_job_config.yaml
│   └── source_definitions/     # Example source catalog
│       └── source1_v1.yaml
├── src/
│   ├── main/
│   │   ├── scala/com/example/featureplatform/
│   │   │   ├── config/         # Configuration loading and case class models (Models.scala, ConfigLoader.scala)
│   │   │   ├── features/       # Feature transformation logic (FeatureTransformer.scala, TransactionTransformers.scala, TransformerFactory.scala)
│   │   │   ├── runner/         # Main job execution logic (JobRunner.scala)
│   │   │   ├── sources/        # Data source reading logic (DataSource.scala, DatabricksSparkSource.scala)
│   │   │   └── spark/          # SparkSession management (SparkSessionManager.scala)
│   │   ├── resources/          # Application resources (e.g., logback.xml)
│   └── test/
│       ├── scala/              # Scala test files
│       └── resources/          # Test resources (e.g., sample YAML configs, test data)
├── target/                     # Compiled code and packaged JARs (created by sbt)
├── DATABRICKS_GUIDE.md         # Guide for running jobs on Databricks
├── DESIGN.md                   # Project design documentation
├── LICENSE                     # Project license
└── README.md                   # This file
```
*(Note: `configs/` and `source_definitions/` are typically managed outside the packaged JAR, e.g., on DBFS or in a Git repo for Databricks Repos when deploying to Databricks. For local testing, they can be local paths.)*

## Core Components

*   **`config.Models`**: Defines Scala case classes that map to the structure of YAML configuration files (e.g., `JobConfig`, `SourceDefinition`, `FeatureTransformerConfig`).
*   **`config.JobConfigLoader` & `config.SourceRegistry`**: Utilities for loading and parsing job configurations and source definitions from YAML files using Circe.
*   **`spark.SparkSessionManager`**: Manages the creation and retrieval of SparkSession instances, ensuring they are configured with necessary support (e.g., Delta Lake).
*   **`sources.DataSourceReader` (trait) & `sources.DatabricksSparkSource`**: Defines a contract for reading data sources and provides an implementation for reading from Databricks sources (tables, queries, file paths like Delta or Parquet).
*   **`features.FeatureTransformer` (trait)**: A base trait for all feature transformations.
*   **Specific Transformers (e.g., `features.TransactionIndicatorDeriver`)**: Concrete implementations of `FeatureTransformer` that perform specific data manipulations.
*   **`features.TransformerFactory`**: Instantiates feature transformers based on configuration.
*   **`runner.JobRunner`**: The main application entry point (`main` method) that orchestrates the entire feature engineering job execution flow.

## Prerequisites

*   **Java Development Kit (JDK):** JDK 11 or JDK 17 recommended for Spark 3.5.x and Scala 2.12.x. (JDK 21 was used during development with specific JVM flags).
*   **SBT (Scala Build Tool):** Version 1.x (e.g., 1.9.x or newer).

## Setup & Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd feature-engineering-scala
    ```
2.  SBT handles dependency management automatically. No separate virtual environment setup is typically needed for Scala projects.

## Building the Project

*   **Compile the project:**
    ```bash
    sbt compile
    ```
*   **Run tests:**
    ```bash
    sbt test
    ```
*   **Package the application (create a fat JAR for deployment):**
    ```bash
    sbt assembly
    ```
    The output JAR will typically be located at `target/scala-2.12/feature-engineering-scala-assembly-X.Y.Z.jar` (version numbers may vary).

## Running Jobs Locally (Example)

To run jobs locally, you'll need:
1.  A job configuration YAML file.
2.  A source catalog directory with source definition YAML files.
3.  Sample data accessible via local paths if your source definitions point to local files (e.g., Parquet files).
4.  A correctly configured Spark environment for local execution.

**Using `sbt runMain`:**
   This method uses sbt to run the application. Ensure Spark dependencies in `build.sbt` are NOT marked as `provided` (or use a sub-project for local testing that includes them). The `build.sbt` in this project is configured with `javaOptions` and `fork := true` to aid local execution with newer JDKs.

   ```bash
   sbt "runMain com.example.featureplatform.runner.JobRunner /path/to/your/job_config.yaml /path/to/your/source_catalog_dir local[*]"
   ```
   Replace `/path/to/your/job_config.yaml` and `/path/to/your/source_catalog_dir` with actual paths. `local[*]` tells Spark to run locally using all available cores.

**Using `spark-submit` with the assembled JAR:**
   This is the standard way to run Spark applications. Ensure Spark dependencies in `build.sbt` ARE marked as `provided` when building the assembly JAR for this, as `spark-submit` provides the Spark environment.

   ```bash
   # Ensure SPARK_HOME is set or spark-submit is in your PATH
   spark-submit --class com.example.featureplatform.runner.JobRunner \
     target/scala-2.12/feature-engineering-scala-assembly-YOUR_VERSION.jar \
     /path/to/your/job_config.yaml /path/to/your/source_catalog_dir
   ```
   *(Note: The current `build.sbt` is configured for local running by including Spark JARs. To build for `spark-submit` against a cluster that provides Spark, set Spark/Delta dependencies to `% "provided"` before `sbt assembly`.)*

## API Documentation (Scaladoc)

To generate API documentation for the project:
```bash
sbt doc
```
The documentation will be generated in `target/scala-2.12/api/` (the exact path might vary based on your Scala version). Open the `index.html` file in this directory to view the documentation. Basic Scaladoc comments have been added to key classes and methods.

## Configuration

The behavior of the jobs is driven by YAML configuration files:
*   **Job Configuration Files:** (e.g., `configs/jobs/my_job.yaml`) define a single job, including:
    *   `job_name`, `description`
    *   `input_source` (name and version of the source to read)
    *   `feature_transformers` (a list of transformers to apply, with their names and parameters)
    *   `output_sink` (type, path, mode, and other sink-specific configurations)
*   **Source Catalog (Source Definitions):** (e.g., `source_definitions/my_source_v1.yaml`) defines data sources:
    *   `name`, `version`, `description`, `type`, `entity`
    *   `location` (e.g., path to a Delta table or Parquet files)
    *   `fields` (schema definition, optional but used for validation)
    *   `config` (source-type specific details, e.g., for Databricks: catalog, schema, table, query)
    *   `quality_checks`, `metadata`

These configuration files are loaded at runtime by `JobRunner`.

## Running on Databricks

For detailed instructions on packaging, deploying, and running jobs on Databricks, please refer to [DATABRICKS_GUIDE.md](DATABRICKS_GUIDE.md).

## Development

*   **Code Style:** Consistent code formatting is encouraged. Consider integrating a tool like ScalaFmt.
*   **Testing:** Unit and integration tests should be added under `src/test/scala/`.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
