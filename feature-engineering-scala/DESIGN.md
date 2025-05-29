# Design Document: Scala Feature Engineering Platform

## 1. Introduction and Goals

This document outlines the design of the Scala Feature Engineering Platform. The primary goal is to provide a robust, scalable, and configuration-driven framework for feature engineering tasks using Apache Spark and Scala. This platform allows data scientists and engineers to define, manage, and execute complex feature transformations consistently across various data sources.

This project is a Scala-based implementation, drawing inspiration from common patterns in feature engineering pipelines and aiming to leverage Scala's strengths for Spark-based data processing, such as static typing and functional programming paradigms.

**Key Goals:**

*   **Configuration-Driven:** Jobs, data sources, and transformations are defined in YAML configuration files, promoting reusability and ease of modification without code changes.
*   **Modularity:** A clear separation of concerns between data source reading, feature transformation logic, and job execution.
*   **Extensibility:** Easy to add new feature transformers and data source types.
*   **Spark Integration:** Leverages Apache Spark for distributed data processing and scalability.
*   **Schema Management:** Basic schema definition and validation capabilities.
*   **Databricks Compatibility:** Designed for easy deployment and execution on Databricks.

## 2. Architecture Overview

The platform follows a modular architecture:

```
+------------------------+      +-----------------------+      +--------------------------+
| YAML Configuration     |----->| JobRunner (Main App)  |<---->| SparkSessionManager      |
| (Jobs, Sources)        |      +-----------------------+      +--------------------------+
+------------------------+               |
                                         | (uses)
                                         V
+------------------------+      +-----------------------+      +--------------------------+
| SourceRegistry         |<-----| DatabricksSparkSource |      | TransformerFactory       |
| (Loads Source Defs)    |      | (Reads DataFrame)     |----->| (Creates Transformers)   |
+------------------------+      +-----------------------+      +--------------------------+
                                         | (DataFrame)                    | (FeatureTransformer)
                                         V                                V
                               +-------------------------------------------------+
                               | Sequential Feature Transformation (DataFrame ops) |
                               +-------------------------------------------------+
                                         | (Transformed DataFrame)
                                         V
                               +--------------------------+
                               | Output Sink              |
                               | (e.g., Delta, Display)   |
                               +--------------------------+
```

**Workflow:**

1.  `JobRunner` is invoked with paths to a job configuration file and a source catalog directory.
2.  `JobConfigLoader` reads and parses the specified job YAML.
3.  `SourceRegistry` loads all source definitions from the source catalog directory.
4.  `JobRunner` retrieves the required `SourceDefinition` from the `SourceRegistry`.
5.  A `DataSourceReader` (e.g., `DatabricksSparkSource`) reads the input DataFrame from the specified source using Spark, managed by `SparkSessionManager`.
6.  `JobRunner` iterates through the list of feature transformations defined in the job configuration.
7.  For each transformation, `TransformerFactory` instantiates the appropriate `FeatureTransformer`.
8.  The transformer's `apply` method is called to transform the DataFrame.
9.  The final transformed DataFrame is written to the configured output sink.

## 3. Core Modules and Components (Scala Implementation)

The project is organized into several Scala packages:

*   **`com.example.featureplatform.config`**:
    *   **`Models.scala`**: Contains all Scala case classes (e.g., `JobConfig`, `SourceDefinition`, `FeatureTransformerConfig`, `DatabricksSourceDetailConfig`) that represent the structure of the YAML configuration files. These classes use `Option` for optional fields and appropriate Scala types (e.g., `String`, `Int`, `Double`, `Boolean`, `List`, `Map[String, io.circe.Json]`). Circe decoders/encoders are defined (often semi-automatically) in companion objects for these case classes.
    *   **`JobConfigLoader.scala`**: An object responsible for loading a single `JobConfig` from a YAML file using Circe for parsing and decoding.
    *   **`SourceRegistry.scala`**: A class and companion object for loading and managing all `SourceDefinition`s from a directory of YAML files. It stores definitions in a map for efficient retrieval.

*   **`com.example.featureplatform.sources`**:
    *   **`DataSource.scala`**: Defines the `DataSourceReader` trait, which specifies the contract (`read` method) for all data source readers.
    *   **`DatabricksSparkSource.scala`**: An implementation of `DataSourceReader` for reading data from various Databricks-related sources. It can handle:
        *   Reading from tables (e.g., `catalog.schema.table`).
        *   Executing SQL queries (`spark.sql(...)`).
        *   Reading from file paths (e.g., Delta tables, Parquet files) using `spark.read.format(...).load(...)`.
        It also includes basic schema validation against fields defined in `SourceDefinition`.

*   **`com.example.featureplatform.features`**:
    *   **`FeatureTransformer.scala`**: Defines the base `FeatureTransformer` trait, which requires an `apply(DataFrame): DataFrame` method and `extends Serializable`.
    *   **`TransactionTransformers.scala`** (and potentially others): Contains concrete implementations of various feature transformers (e.g., `TransactionIndicatorDeriver`, `UserSpendAggregator`). Each transformer:
        *   Extends `FeatureTransformer`.
        *   Takes a `params: Map[String, io.circe.Json]` in its constructor to configure its behavior.
        *   Uses Spark DataFrame API and functions from `org.apache.spark.sql.functions` in its `apply` method to perform the transformation.
        *   Includes logging using SLF4J.
    *   **`TransformerFactory.scala`**: An object that instantiates `FeatureTransformer` implementations based on a `name` string (from `FeatureTransformerConfig`) and passes the parameters.

*   **`com.example.featureplatform.runner`**:
    *   **`JobRunner.scala`**: The main application object with the `main` method. It orchestrates the entire job execution flow: argument parsing, configuration loading, Spark session initialization, data reading, sequential application of transformers, and writing to an output sink. It uses `Either[Throwable, T]` for error handling.

*   **`com.example.featureplatform.spark`**:
    *   **`SparkSessionManager.scala`**: An object responsible for obtaining or creating a `SparkSession`. It ensures that the Spark session is configured with necessary extensions, particularly for Delta Lake support.

## 4. Data Models (Configuration Case Classes)

The platform's behavior is primarily driven by YAML configurations, which are deserialized into Scala case classes defined in `com.example.featureplatform.config.Models.scala`.

*   **`SourceDefinition`**:
    *   `name: String`, `version: String`, `description: Option[String]`, `type: String` (e.g., "delta", "parquet", "databricks_table"), `entity: String`
    *   `location: Option[String]` (path to data or table name for some types)
    *   `fields: Option[List[FieldDefinition]]` (for schema definition and validation)
    *   `config: DatabricksSourceDetailConfig` (currently specific to Databricks, could be a sealed trait for other source types)
    *   `quality_checks: Option[List[QualityCheckDefinition]]`, `metadata: Option[MetadataDefinition]`
*   **`DatabricksSourceDetailConfig`**: (Nested within `SourceDefinition.config`)
    *   `catalog: Option[String]`, `schema: Option[String]`, `table: Option[String]`, `query: Option[String]`, `incremental: Option[Boolean]`
*   **`FieldDefinition`**:
    *   `name: String`, `type: String`, `description: Option[String]`, `required: Option[Boolean]`, `default: Option[String]`
*   **`JobConfig`**:
    *   `job_name: Option[String]`, `description: Option[String]`
    *   `input_source: JobInputSourceConfig` (defines the primary input source by name and version)
    *   `feature_transformers: List[FeatureTransformerConfig]` (sequence of transformations to apply)
    *   `output_sink: OutputSinkConfig` (defines where and how to write the output)
*   **`JobInputSourceConfig`**:
    *   `name: String`, `version: Option[String]`, `load_params: Option[Map[String, Json]]`
*   **`FeatureTransformerConfig`**:
    *   `name: String` (maps to a specific transformer implementation in `TransformerFactory`)
    *   `params: Map[String, Json]` (parameters passed to the transformer's constructor)
*   **`OutputSinkConfig` & `OutputSinkParams`**:
    *   `sink_type: String` (e.g., "delta_table", "display")
    *   `config: OutputSinkParams` (contains path, mode, options, etc. for the sink)

YAML parsing and decoding into these case classes are handled by the Circe library, with decoders typically derived semi-automatically in companion objects.

## 5. Key Workflows

*   **Job Execution (`JobRunner.main`)**:
    1.  Parse command-line arguments (job config path, source catalog path).
    2.  Initialize SparkSession via `SparkSessionManager`.
    3.  Load `JobConfig` using `JobConfigLoader`.
    4.  Load `SourceRegistry` using `SourceRegistry.loadFromDirectory`.
    5.  Retrieve the specific `SourceDefinition` for the job's input source.
    6.  Instantiate a `DataSourceReader` (e.g., `DatabricksSparkSource`).
    7.  Call `dataSourceReader.read()` to get the initial DataFrame.
    8.  Iterate through `feature_transformers` list from `JobConfig`:
        a.  For each `FeatureTransformerConfig`, use `TransformerFactory.getTransformer()` to get an instance of `FeatureTransformer`.
        b.  Call `transformer.apply()` on the current DataFrame to get the transformed DataFrame.
    9.  Process the `output_sink` configuration:
        a.  Write the final transformed DataFrame to the specified sink (e.g., Delta table, display).
    10. Handle any errors encountered during the process using `Either`.
    11. Stop the SparkSession.

*   **Source Loading (`DatabricksSparkSource.read`)**:
    1.  Examine `DatabricksSourceDetailConfig` and `SourceDefinition.location`/`type`.
    2.  If `query` is defined, execute `spark.sql(query)`.
    3.  Else if `table` is defined, execute `spark.table(tableName)` (with logic to construct fully qualified name).
    4.  Else if `location` is defined, execute `spark.read.format(type).load(location)`.
    5.  If `SourceDefinition.fields` are provided, validate the schema of the loaded DataFrame against these fields (check for presence of columns).

*   **Transformer Application (`TransformerFactory` and `FeatureTransformer.apply`)**:
    1.  `TransformerFactory` maps a string name from `FeatureTransformerConfig.name` to a specific Scala transformer class constructor.
    2.  The `config.params: Map[String, Json]` are passed to the transformer's constructor.
    3.  The transformer constructor parses these Json params into typed Scala values (e.g., String, Double, List) with defaults.
    4.  The `apply` method of the transformer implements its logic using Spark DataFrame operations.

## 6. Design Principles and Choices

*   **Immutability and Functional Constructs:** Scala's preference for immutability is leveraged where practical (e.g., DataFrame transformations). Error handling uses `Either` to avoid exceptions in functional flows.
*   **Type Safety:** Scala's static typing helps catch errors at compile time. Case classes provide type-safe configuration models.
*   **Configuration as Code (YAML & Case Classes):** YAML provides human-readable configuration, while Circe and case classes provide type-safe parsing and access in Scala.
*   **Dependency Management (SBT):** SBT manages project dependencies, compilation, testing, and packaging (assembly).
*   **Spark API (Scala):** Utilizes the native Scala API for Apache Spark, which is generally the most performant and expressive.
*   **Logging (SLF4J & Logback):** Standard logging framework for Scala applications.
*   **Modularity and Extensibility:**
    *   New data sources can be added by implementing `DataSourceReader`.
    *   New feature transformations can be added by implementing `FeatureTransformer` and registering them in `TransformerFactory`.
*   **Error Handling:** Consistent use of `Either[Throwable, T]` for operations that can fail, allowing for cleaner error propagation and handling in `JobRunner`.

## 7. Setup and Usage

Refer to [README.md](README.md) for detailed setup, build, and local execution instructions.

## 8. Running as a Databricks Job

Refer to [DATABRICKS_GUIDE.md](DATABRICKS_GUIDE.md) for instructions on deploying and running jobs on Databricks.

## 9. Future Considerations / Potential Improvements

*   **SourceReaderFactory:** Implement a factory for `DataSourceReader` instances based on `SourceDefinition.type` to support multiple source systems (e.g., JDBC, Kafka) beyond `DatabricksSparkSource`.
*   **Advanced Schema Validation:** More sophisticated schema validation, including type checking and evolution strategies.
*   **Unit and Integration Testing:** Expand test coverage for transformers, configuration loading, and job execution flows.
*   **CI/CD:** Set up CI/CD pipelines for automated building, testing, and deployment.
*   **Parameter Interpolation:** Allow dynamic parameter interpolation in configuration files (e.g., for dates, environment variables).
*   **Metrics and Monitoring:** Integrate with monitoring systems to track job progress and data quality.
*   **ScalaFmt/Style Checking:** Enforce consistent code style.
