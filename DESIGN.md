# Low-Level Design (LLD) - Feature Platform

## 1. Introduction and Overview

The Feature Platform is designed to provide a robust and streamlined framework for defining, managing, executing, and monitoring feature engineering pipelines. Its primary purpose is to simplify the complexities associated with feature creation and lifecycle management in machine learning workflows, fostering collaboration between data scientists and ML engineers.

The platform aims to achieve this by offering a structured approach to data source definition, feature transformation, and job execution, with a strong emphasis on reusability, modularity, and data integrity.

### 1.1. High-Level Goals

The development of the Feature Platform is guided by the following high-level goals:

1.  **Modularity:** Enable the definition and management of data sources and features as independent, well-defined components. This allows for easier maintenance, updates, and understanding of individual parts of the system.
    *   Data sources are defined centrally in a versioned catalog.
    *   Feature transformations are encapsulated as reusable classes.

2.  **Reusability:** Promote the reuse of both feature transformation logic and data source definitions across multiple projects, models, or teams.
    *   Standardized feature transformers can be applied to different datasets.
    *   Common data sources can be referenced by multiple jobs without re-definition.

3.  **Configuration-Driven Execution:** Allow for the definition and execution of feature engineering tasks through declarative YAML configuration files. This separates the "what" (the job definition) from the "how" (the underlying execution logic).

4.  **Data Quality and Schema Adherence:** Establish mechanisms to ensure the quality and structural integrity of data.
    *   Source definitions include schema information (field names, types).
    *   Data sources perform basic schema validation (e.g., column name checks) upon reading data.
    *   (Future) Extend to more comprehensive data quality checks (e.g., type validation, null checks, custom rules).

5.  **Clear Separation of Concerns:** Maintain a distinct separation between:
    *   **Data Definition:** How and where data sources are defined (Source Catalog).
    *   **Transformation Logic:** The implementation of feature engineering steps (Feature Transformers).
    *   **Job Execution:** The orchestration and running of feature pipelines (Job Runner and configurations).

### 1.2. Target Users and Use Cases

The Feature Platform is primarily targeted at:

*   **Data Scientists:** To easily discover, define, and experiment with features without needing to delve deep into data engineering complexities for every task.
*   **Machine Learning (ML) Engineers:** To productionize, manage, and monitor feature pipelines, ensuring reliability, scalability, and data quality in ML systems.

Common use cases include:
*   Creating training datasets for ML models.
*   Generating batch features for inference.
*   Establishing a centralized and versioned "feature store" or catalog of available data sources and their schemas.
*   Ensuring consistency in feature generation across development and production environments.

## 2. Architecture Diagram

The following diagram illustrates the high-level architecture and data flow within the Feature Platform when a batch job is executed:

```mermaid
graph TD
    subgraph "User Interaction & Job Definition"
        U[User/Trigger] -->|Executes with| JYAML["Job Configuration YAML\n(configs/jobs/*.yaml)\nDefines: input_source (name, version, load_params),\ntransformers, output_sink"];
    end

    subgraph "Job Execution Core"
        JRUNNER["Main Job Runner\n(runner/execute_batch_job.py)"]
        SPARKMAN["SparkSessionManager\n(domain/core/spark.py)"];
    end

    subgraph "Source Catalog & Definition"
        SRCREG["SourceRegistry\n(domain/core/source_registry.py)"];
        SRCCATYAML["Source Catalog YAMLs\n(source/**/*.yaml)\nDefines: connection details, format,\nschema (fields), entity, type"];
        SRCDEFMODEL["SourceDefinition Pydantic Models\n(domain/core/source_definition.py)"];
    end

    subgraph "Data Handling & Transformation"
        DATASRC["Data Source Instance\n(e.g., DatabricksSparkSource\ndomain/sources/*)"];
        DATA["Data\n(e.g., Spark DataFrame)"];
        TFACTORY["Transformer Factory\n(get_transformer\ndomain/features/factory.py)"];
        TRANSFORMERS["FeatureTransformer Instances\n(domain/features/*)"];
        OUTSINK["Output Sink\n(e.g., Delta Table, Display)"];
    end

    JYAML -->|Loaded by| JRUNNER;
    
    JRUNNER -->|1. Gets Source Def for\n(input_source.name, input_source.version)| SRCREG;
    SRCREG -->|Loads & Parses| SRCCATYAML;
    SRCCATYAML -->|Validated by & Parsed into| SRCDEFMODEL;
    SRCDEFMODEL -->|Structure for| SRCREG;
    SRCREG -->|Returns SourceDefinition| JRUNNER;
    
    JRUNNER -->|2. Initializes with SourceDefinition\n& job_input_source.load_params| DATASRC;
    DATASRC -->|Uses| SPARKMAN;
    SPARKMAN -->|Provides SparkSession| DATASRC;
    DATASRC -->|3. Reads (influenced by load_params)| DATA;
    
    JRUNNER -->|4. Gets Transformers| TFACTORY;
    TFACTORY -->|Creates| TRANSFORMERS;
    TRANSFORMERS -->|5. Apply to| DATA;
    
    DATA -->|6. Written to| OUTSINK;

    classDef component fill:#f9f,stroke:#333,stroke-width:2px;
    classDef data fill:#lightgrey,stroke:#333,stroke-width:1px;
    classDef process fill:#lightblue,stroke:#333,stroke-width:2px;

    class U,JYAML component;
    class JRUNNER,SPARKMAN,SRCREG,SRCDEFMODEL,DATASRC,TFACTORY,TRANSFORMERS,OUTSINK process;
    class SRCCATYAML,DATA data;
```

**Flow Description:**

1.  **Job Initiation:** A user or an automated trigger executes `runner/execute_batch_job.py`, providing the path to a Job Configuration YAML file.
2.  **Job Configuration Loading:** `execute_batch_job.py` loads and parses the Job YAML. This file specifies the `input_source` (by `name` and `version`, and includes job-specific `load_params`), the sequence of `feature_transformers`, and the `output_sink`.
3.  **Source Definition Retrieval:**
    *   The runner interacts with the `SourceRegistry` to obtain the `SourceDefinition` for the `input_source` specified in the job YAML.
    *   The `SourceRegistry` scans the `source/` directory (Source Catalog), finds the corresponding source YAML file, and parses it using `SourceDefinition` Pydantic models for validation and structure.
4.  **Data Source Instantiation:**
    *   Using the retrieved `SourceDefinition` (which contains detailed configuration like table names, paths, formats, and schema) and the `load_params` from the job YAML, the runner instantiates the appropriate data source class (e.g., `DatabricksSparkSource`).
    *   The data source instance utilizes the `SparkSessionManager` to acquire an active Spark session if needed.
5.  **Data Reading:** The data source instance reads the raw data, applying any `load_params` (e.g., date filters, specific query parameters) to customize the data retrieval. This produces a Spark DataFrame.
6.  **Schema Validation (Implicit):** As part of the `read()` operation (e.g., in `DatabricksSparkSource`), if the `SourceDefinition` contained a list of fields, a basic schema validation (column name check) is performed against the loaded DataFrame.
7.  **Feature Transformation:**
    *   `execute_batch_job.py` uses the `TransformerFactory` (`get_transformer`) to create instances of `FeatureTransformer`s based on the `feature_transformers` section of the job YAML.
    *   These transformers are applied sequentially to the DataFrame.
8.  **Output Handling:** The final transformed DataFrame is written to the specified `output_sink` (e.g., displayed, saved to a Delta table or Parquet files).

This architecture emphasizes a configuration-driven approach, promoting reusability of source definitions and transformation logic.

## 3. Core Modules and Components

This section details the primary modules and their key components within the Feature Platform.

### 3.1. `domain/core/`

This directory houses the foundational elements of the platform.

*   **`source_definition.py`**:
    *   **Purpose**: Defines the Pydantic models used to parse, validate, and represent the structure of data source configuration YAML files found in the `source/` catalog. These models ensure that source definitions adhere to a predefined schema.
    *   **Key Models**:
        *   `SourceDefinition`: The top-level model for a source, encompassing its name, version, type (e.g., "databricks"), associated entity, connection/access configuration, field schema, metadata, and quality checks.
        *   `FieldDefinition`: Describes an individual field within a source's schema (name, type, description, required status, default value).
        *   `DatabricksSourceDetailConfig`: A specific configuration model for Databricks sources, detailing properties like catalog, schema, table name, or query.
        *   `MetadataDefinition`: Captures metadata about the source definition (creation/update timestamps, author, tags).
        *   `QualityCheckDefinition`: Defines data quality checks to be performed on the source (e.g., uniqueness, nullity, custom conditions).

*   **`source_registry.py`**:
    *   **Purpose**: Provides the `SourceRegistry` class, which is responsible for discovering, loading, validating, and managing `SourceDefinition` objects from the YAML-based Source Catalog (`source/` directory).
    *   **Key Class**: `SourceRegistry`
        *   `from_yaml_dir(dir_path)`: A class method that scans the specified directory for `*.yaml` files, parses each into one or more `SourceDefinition` objects, and populates a new `SourceRegistry` instance.
        *   `add_source_definition(source_def)`: Adds a `SourceDefinition` object to the registry, keyed by its name and version.
        *   `get_source_definition(name, version=None)`: Retrieves a specific `SourceDefinition` by its name and optionally by version. If version is omitted, it may apply logic to fetch a default or "latest" version (currently raises an error if multiple versions exist and none is specified).
        *   `get_all_source_definitions()`: Returns all loaded source definitions.

*   **`entity.py`**:
    *   **Purpose**: Defines Pydantic models for representing data entities and their relationships within the platform's conceptual model.
    *   **Key Models**:
        *   `Entity`: Describes a business or data concept (e.g., "customer", "transaction"), including its primary key and fields.
        *   `Relation`: Defines relationships between entities (e.g., one-to-many).

*   **`registry.py`**:
    *   **Purpose**: Contains the `EntityRegistry` class, responsible for loading and managing `Entity` definitions from YAML files located in the `registry/entity/` directory.
    *   **Key Class**: `EntityRegistry` (analogous to `SourceRegistry` but for entities).

*   **`config.py`**:
    *   **Purpose**: Centralizes common configuration objects, particularly for external service connections.
    *   **Key Class**: `DatabricksConnectionConfig`: A dataclass that holds connection parameters for Databricks (e.g., `server_hostname`, `http_path`, `access_token`, `catalog`, `schema`). It sources these values primarily from environment variables, providing a consistent way for different components to access Databricks.

*   **`spark.py`**:
    *   **Purpose**: Provides utilities for managing Spark sessions.
    *   **Key Class**: `SparkSessionManager`: Manages the lifecycle of a `SparkSession`. It can initialize a local Spark session or configure one for Databricks Connect (using environment variables like `SPARK_REMOTE`, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID`). It's designed to be used as a context manager or via direct `get_session()` and `stop_session()` calls.

### 3.2. `domain/sources/`

This module contains the abstractions and concrete implementations for accessing data from various systems.

*   **Base Classes**:
    *   `base.py`:
        *   `SourceConfig`: A basic dataclass for common source configuration attributes (name, entity, type, location, fields as `List[str]`, description).
        *   `Source`: An abstract base class (ABC) for all data sources, defining a common interface (e.g., `read()`, `validate()`, `initialize()`, `close()`).
    *   `spark_base.py`:
        *   `SparkSourceConfig`: Extends `SourceConfig` for Spark-specific sources. Notably, it overrides `fields` to be `Optional[List[Dict[str, Any]]]` to accommodate structured field definitions from the `SourceDefinition` (e.g., name, type, description per field). It also includes `format` and `options` for Spark reads.
        *   `SparkSource`: An ABC for sources that return Spark DataFrames. Requires a `SparkSessionManager`.

*   **Concrete Implementations**:
    *   `databricks_spark.py`:
        *   `DatabricksSparkSourceConfig`: Configuration specific to `DatabricksSparkSource`, inheriting from `SparkSourceConfig`. May include Databricks-specific connection details if not ambiently configured.
        *   `DatabricksSparkSource`: Reads data from Databricks environments (typically Delta tables, but can also execute SQL queries if the `location` in its config is a query string and `format` is "sql") into Spark DataFrames. Its configuration is derived from a `SourceDefinition` object (including `location`, `format`, `options`, and `fields`).
        *   **Schema Validation**: During its `read()` operation, if `config.fields` (from `SourceDefinition`) is provided, it performs a basic schema validation by comparing expected column names with the actual columns in the loaded DataFrame. It raises an error for missing columns and logs warnings for extra columns.

    *   `databricks_sql.py`:
        *   `DeltaSQLSourceConfig`: Configuration for `DatabricksSQLSource`. It includes `timestamp_column` and `partition_filters` for query generation, and an optional `databricks_connection` (an instance of `DatabricksConnectionConfig`) for connection parameters.
        *   `DatabricksSQLSource`: Reads data from Databricks tables using the Databricks SQL Connector, returning Pandas DataFrames. It constructs SQL queries based on its configuration.
        *   **Connection Logic**: Prioritizes connection parameters from the `DatabricksConnectionConfig` object if provided in its configuration. If not, it falls back to environment variables (via `DatabricksSQLConnectionLocalConfig`) for `server_hostname`, `http_path`, and `access_token`.

### 3.3. `domain/features/`

This module is dedicated to feature transformation logic.

*   **`transform.py`**:
    *   `FeatureTransformer`: An abstract base class (ABC) defining the interface for all feature transformers. Its core method is `apply(df)`, which takes a DataFrame and returns a transformed DataFrame.
    *   Contains example generic transformers like `SimpleAgeCalculator` and `WithGreeting`.

*   **`factory.py`**:
    *   `TRANSFORMER_REGISTRY`: A dictionary that maps string names (e.g., "UserSpendAggregator") to `FeatureTransformer` classes. Transformers register themselves here.
    *   `get_transformer(name, params)`: A factory function that looks up a transformer class by `name` in the `TRANSFORMER_REGISTRY` and instantiates it with the provided `params`.

*   **Example Transformers (e.g., `financial_transformers.py`)**:
    *   These files contain domain-specific or more complex feature transformers (like `UserSpendAggregator`, `UserMonthlyTransactionCounter`, `UserCategoricalSpendAggregator`). They inherit from `FeatureTransformer` and implement their specific `apply` logic.

### 3.4. `domain/jobs/`

This module focuses on the definition and loading of batch job configurations.

*   **`config_loader.py`**:
    *   **Purpose**: Provides Pydantic models for defining the structure of job YAML files and a utility function to load and validate these configurations.
    *   **Key Pydantic Models**:
        *   `JobConfig`: The top-level model for a job definition. It includes `job_name`, `description`, `input_source`, a list of `feature_transformers`, and an `output_sink`.
        *   `JobInputSourceConfig`: Defines how a job specifies its input data source. Crucially, it uses `name` (of the source in the catalog), `version` (optional), and `load_params` (optional dictionary for job-specific runtime parameters like filters or query arguments to be passed to the source's read method).
        *   `FeatureTransformerConfig`: Describes a single transformer to be applied in the job, including its `name` (for lookup in the `TRANSFORMER_REGISTRY`) and `params` to initialize it.
        *   `OutputSinkConfig`: Defines the output destination, including `sink_type` (e.g., "display", "delta_table") and type-specific `config` parameters.

### 3.5. `runner/execute_batch_job.py`

*   **Role**: This script is the main entry point for executing batch feature engineering jobs defined by YAML configurations.
*   **Orchestration**:
    1.  Loads the specified job YAML configuration file using `load_job_config` from `domain.jobs.config_loader`.
    2.  Initializes the `SparkSessionManager` to ensure a Spark session is available (configured for local or Databricks Connect based on environment variables).
    3.  Uses the `SourceRegistry` (from `domain.core.source_registry`) to retrieve the detailed `SourceDefinition` for the input source specified in the job config's `input_source.name` and `input_source.version`.
    4.  Instantiates the appropriate data source class (e.g., `DatabricksSparkSource`) using the `SourceDefinition` and passes `input_source.load_params` for runtime customization of data reading.
    5.  Reads the data into a DataFrame. The source itself may perform schema validation if configured with field definitions.
    6.  Iterates through the `feature_transformers` list in the job configuration, using the `TransformerFactory` (`get_transformer`) to instantiate each transformer by name with its parameters.
    7.  Applies each transformer sequentially to the DataFrame.
    8.  Handles the output by writing the final transformed DataFrame to the destination specified in the `output_sink` section of the job configuration.

### 3.6. Configuration Files

These YAML files define the "what" of the data and processing, allowing the Python code to handle the "how".

*   **`source/**/*.yaml` (Source Catalog)**:
    *   **Role**: These files form a centralized, versioned catalog of detailed data source definitions. They are designed to be reusable across multiple jobs.
    *   **Structure**: Each file typically defines a single source and includes:
        *   `name`: Unique name for the source.
        *   `version`: Version string (e.g., "v1.0.0").
        *   `type`: The kind of source system (e.g., "databricks", "snowflake", "s3"). This helps the runner determine which source Python class to use.
        *   `entity`: The primary data entity this source provides (e.g., "transactions", "users").
        *   `config`: A dictionary with source-type-specific connection and access details (e.g., for "databricks": catalog, schema, table name, or a SQL query).
        *   `fields`: A list of dictionaries defining the expected schema (each with `name`, `type`, `description`, `required`). Used for documentation and schema validation.
        *   `metadata`: Creation/update info, tags.
        *   `quality_checks`: Definitions for data quality rules.
    *   These files are loaded and managed by the `SourceRegistry`.

*   **`configs/jobs/**/*.yaml` (Job Definitions)**:
    *   **Role**: These files define specific batch jobs. Each job specifies its input, the transformations to apply, and where to output the results.
    *   **Structure**:
        *   `job_name`, `description`.
        *   `input_source`: Contains `name` and `version` to reference a definition in the Source Catalog (`source/` files), and `load_params` for job-specific read-time arguments.
        *   `feature_transformers`: A list, where each item has a `name` (referencing a registered transformer) and `params` for its initialization.
        *   `output_sink`: Defines the `sink_type` and its specific `config`.
    *   These files are loaded by `config_loader.py` and orchestrated by `runner/execute_batch_job.py`.

## 4. Data Models (Pydantic Configurations)

The Feature Platform relies heavily on Pydantic models to define, validate, and manage configurations. These models serve as data contracts throughout the system, ensuring consistency and providing clear structures for YAML files.

### 4.1. `SourceDefinition` (from `domain.core.source_definition`)

*   **Role**: This is a Pydantic model that represents a single, detailed data source configuration. Each YAML file within the `source/` catalog (e.g., `source/my_source/v1/my_source.yaml`) is parsed and validated into a `SourceDefinition` object. It provides a structured Python representation of a data source's properties.
*   **Loading**: `SourceDefinition` objects are loaded from their YAML representations by the `SourceRegistry`.
*   **Key Attributes**:
    *   `name: str`: The unique name of the data source (e.g., "customer_transactions", "user_profiles").
    *   `version: str`: The version of this source definition (e.g., "v1.0.0", "v1.2").
    *   `type: str`: Specifies the kind of data source system (e.g., "databricks", "snowflake", "s3_csv"). This is used by the job runner to determine which source implementation class to use.
    *   `entity: str`: The name of the primary data entity this source provides data for (e.g., "transaction", "user"). This links the source to the conceptual data model.
    *   `location: Optional[str]`: A generic location string. For some source types (like file-based sources), this might be a path or URI. For others (like Databricks tables), specific location details are often in the `config` block, but this can serve as a fallback or primary identifier.
    *   `fields: Optional[List[FieldDefinition]]`: A list defining the expected schema of the data source. Each item is a `FieldDefinition` object specifying a column's `name`, `type`, `description`, `required` status, and `default` value. This is crucial for schema validation and documentation.
    *   `config: SourceTypeSpecificConfig` (a Union type): A nested Pydantic model containing configuration details specific to the `type` of the source.
        *   Example: For a "databricks" source, this would typically be a `DatabricksSourceDetailConfig` object.
        *   `DatabricksSourceDetailConfig`: Contains attributes like `catalog` (Databricks catalog name), `schema_name` (schema/database name, aliased as "schema"), `table` (table name), or `query` (a SQL query to execute).
    *   `quality_checks: Optional[List[QualityCheckDefinition]]`: A list defining data quality checks to be performed on the source data. Each `QualityCheckDefinition` specifies the `type` of check, the `field` it applies to, and any `condition`.
    *   `metadata: Optional[MetadataDefinition]`: Contains metadata about the source definition itself, such as creation/update timestamps (`created_at`, `updated_at`), authors (`created_by`, `updated_by`), and descriptive `tags`.

### 4.2. `JobConfig` (from `domain.jobs.config_loader`)

*   **Role**: This Pydantic model represents the complete configuration for a single batch feature engineering job. It is loaded from a job YAML file (typically located in `configs/jobs/`). The `JobConfig` object structures how a job reads data, what transformations it applies, and where it sends the results.
*   **Key Attributes**:
    *   `job_name: Optional[str]`: An optional name for the job (e.g., "financial_features_v1", "user_activity_aggregation").
    *   `description: Optional[str]`: An optional description of what the job does.
    *   `input_source: JobInputSourceConfig`: Defines the input data for the job. This is a nested Pydantic model with the following key sub-fields:
        *   `name: str`: The name of the data source to use, which must correspond to a `SourceDefinition` `name` in the Source Catalog (`source/` files).
        *   `version: Optional[str]`: The specific version of the `SourceDefinition` to use from the catalog. If omitted, the `SourceRegistry` may have logic to select a default or latest version (or raise an error if ambiguous).
        *   `load_params: Optional[Dict[str, Any]]`: A dictionary of job-specific runtime parameters that are passed to the data source's `read()` method. These can be used to customize data retrieval for a particular job run, such as specifying date range filters, query parameters, or other dynamic options, without altering the underlying `SourceDefinition` in the catalog.
    *   `feature_transformers: List[FeatureTransformerConfig]`: A list defining the sequence of feature transformations to be applied to the input data. Each item in the list is a `FeatureTransformerConfig` object:
        *   `name: str`: The registered name of the feature transformer class (e.g., "UserSpendAggregator"). This name is used by the `TransformerFactory` to instantiate the correct transformer.
        *   `params: Dict[str, Any]`: A dictionary of parameters that are passed to the `__init__` method of the feature transformer class.
    *   `output_sink: OutputSinkConfig`: Defines where the final transformed DataFrame should be written or how it should be handled. This is a nested Pydantic model with the following key sub-fields:
        *   `sink_type: str`: Specifies the type of output sink (e.g., "display", "delta_table", "parquet_files").
        *   `config: Dict[str, Any]`: A dictionary of configuration parameters specific to the chosen `sink_type` (e.g., for "display", `num_rows`; for "delta_table", `path` and `mode`).

These Pydantic models are critical for ensuring that all configurations are well-structured, validated, and easily usable within the Python codebase, bridging the gap between human-readable YAML and the platform's internal logic.

## 5. Key Workflows

This section describes the sequence of operations for major processes within the Feature Platform.

### 5.1. Job Execution Flow

This workflow outlines the steps involved when a user executes a batch feature engineering job.

1.  **Trigger**: The process begins when a user runs the main job execution script:
    ```bash
    python runner/execute_batch_job.py <path_to_job_config.yaml>
    ```

2.  **Job Configuration Loading**:
    *   `runner/execute_batch_job.py` reads the specified job YAML file.
    *   It uses `load_job_config` (from `domain.jobs.config_loader`) to parse and validate the YAML content into a `JobConfig` Pydantic object.

3.  **Spark Session Initialization**:
    *   The `SparkSessionManager` (from `domain.core.spark`) is initialized. This ensures a Spark session is available, configured for either local execution or Databricks Connect based on environment variables (e.g., `SPARK_REMOTE`).

4.  **Input Source Preparation**:
    *   The runner accesses `job_config.input_source`, which is a `JobInputSourceConfig` object containing `name`, `version` (optional), and `load_params` (optional).
    *   It calls `SourceRegistry.from_yaml_dir("source/")` to create a `SourceRegistry` instance, loading all source definitions from the Source Catalog.
    *   It then calls `source_registry_instance.get_source_definition(name, version)` using the `name` and `version` from `job_config.input_source` to retrieve the specific `SourceDefinition` for the job.
    *   Based on the `SourceDefinition.type` (e.g., "databricks"), the runner looks up the corresponding source implementation class (e.g., `DatabricksSparkSource`) from its internal `SOURCE_REGISTRY` (a map of type strings to source classes).
    *   A specific configuration object for the source class (e.g., `DatabricksSparkSourceConfig`) is prepared. This config object is populated using:
        *   Details from the retrieved `SourceDefinition` (e.g., location, format, predefined options, fields).
        *   Runtime parameters from `job_config.input_source.load_params`, which can override or supplement options from the `SourceDefinition`.
    *   An instance of the source class is created, e.g., `DatabricksSparkSource(specific_config, spark_manager)`.

5.  **Data Reading and Schema Validation**:
    *   The `source_instance.read()` method is called.
    *   This method loads data from the source system.
    *   If `SourceDefinition.fields` were provided, the `DatabricksSparkSource` (as an example) performs basic schema validation (column name and presence checks) against the loaded DataFrame. Errors are raised for missing expected columns, and warnings are logged for extra columns.

6.  **Feature Transformation**:
    *   The resulting DataFrame is passed sequentially through the feature transformers specified in `job_config.feature_transformers`.
    *   For each transformer configuration in the list:
        *   `get_transformer(name, params)` (from `domain.features.factory`) is called with the transformer's `name` and `params` from the job config.
        *   The factory function looks up the transformer class in its `TRANSFORMER_REGISTRY` and instantiates it.
        *   The `apply(dataframe)` method of the instantiated transformer is called, processing the DataFrame.
    *   The output DataFrame from one transformer becomes the input for the next.

7.  **Output Sink**:
    *   The final transformed DataFrame is written to the destination defined in `job_config.output_sink` (e.g., displayed to console, saved to a Delta table).

8.  **Spark Session Termination**:
    *   The `SparkSessionManager` stops the Spark session, releasing resources.

### 5.2. Source Definition and Loading Flow

This workflow describes how data source definitions are created, managed, and loaded.

1.  **Definition**: A user defines a new data source or a new version of an existing source by creating/editing a YAML file within the `source/` directory structure (e.g., `source/my_data_source/v1.2/my_data_source.yaml`).
    *   The YAML structure must conform to the schema defined by the `SourceDefinition` Pydantic model in `domain.core.source_definition`. This includes fields like `name`, `version`, `type`, `entity`, `config` (source-type specific details), `fields` (schema), `metadata`, and `quality_checks`.

2.  **Registry Initialization**: The `SourceRegistry.from_yaml_dir(directory_path)` class method is called. This typically happens:
    *   At the beginning of `runner/execute_batch_job.py` to load all available source definitions.
    *   When `scripts/validate_source_catalog.py` is run to check all definitions.

3.  **Discovery and Parsing**:
    *   The `SourceRegistry` recursively scans the specified `directory_path` (e.g., "source/") for all `*.yaml` files.
    *   Each discovered YAML file is read.
    *   The raw YAML content is parsed into a Python dictionary (or a list of dictionaries if the YAML file contains multiple documents).

4.  **Validation and Model Conversion**:
    *   Each dictionary obtained from parsing is then validated against the `SourceDefinition` Pydantic model.
    *   If validation is successful, an instance of `SourceDefinition` is created.
    *   If validation fails (e.g., missing required fields, incorrect types), Pydantic raises a `ValidationError`, which is typically logged by the `SourceRegistry`, and that specific definition might be skipped or cause the process to fail, depending on the context.

5.  **Storage in Registry**:
    *   Valid `SourceDefinition` objects are stored within the `SourceRegistry` instance, typically in a dictionary, indexed by a tuple of `(name, version)`. This allows for efficient retrieval.

6.  **Usage**:
    *   The populated `SourceRegistry` instance can then be used to retrieve specific `SourceDefinition` objects via `get_source_definition(name, version)`.
    *   The `scripts/validate_source_catalog.py` script uses this entire flow (steps 2-5) to iterate through all YAML files in the catalog and report any validation errors, thereby ensuring the integrity of the Source Catalog.

### 5.3. Feature Transformation Application Flow

This workflow details how feature transformers are defined, registered, and applied during job execution.

1.  **Definition**:
    *   Feature transformers are Python classes that inherit from the `FeatureTransformer` abstract base class (defined in `domain/features/transform.py`).
    *   Each transformer class must implement an `apply(self, dataframe)` method, which takes a DataFrame as input and returns a transformed DataFrame.
    *   Transformers can accept parameters in their `__init__` method, which are provided from the job configuration.

2.  **Registration**:
    *   Transformer classes are registered in the `TRANSFORMER_REGISTRY` (a dictionary) located in `domain/features/factory.py`.
    *   This registry maps a unique string name (e.g., "UserSpendAggregator") to the transformer class itself. Registration typically happens at module import time (e.g., by decorating the class or explicitly adding it to the registry).

3.  **Job Configuration**:
    *   A job YAML file specifies a list of feature transformations to be applied under the `feature_transformers` key.
    *   Each item in this list is a configuration object (parsed into `FeatureTransformerConfig`) containing:
        *   `name: str`: The string name that matches a key in the `TRANSFORMER_REGISTRY`.
        *   `params: Dict[str, Any]`: A dictionary of parameters to be passed to the transformer's constructor.

4.  **Execution-Time Application**:
    *   During job execution, `runner/execute_batch_job.py` iterates through the `feature_transformers` list from the `JobConfig` object.
    *   For each transformer configuration:
        *   The `get_transformer(name, params)` factory function (from `domain/features/factory.py`) is called with the `name` and `params`.
        *   `get_transformer` looks up the transformer class associated with the `name` in the `TRANSFORMER_REGISTRY`.
        *   It instantiates the retrieved class, passing the `params` to its `__init__` method.
        *   The `apply(dataframe)` method of this newly created transformer instance is then called with the current state of the job's DataFrame.
    *   The DataFrame returned by the `apply` method becomes the input for the next transformer in the sequence. If it's the last transformer, the resulting DataFrame is passed to the output sink.

## 6. Design Principles & Choices

This section highlights key design decisions made during the development of the Feature Platform and their underlying rationale.

### 6.1. Catalog-Driven Source Management

*   **Choice**: Centralizing detailed data source definitions in versioned YAML files within the `source/` directory (the "Source Catalog"). These definitions are parsed and managed by the `SourceRegistry` using `SourceDefinition` Pydantic models.
*   **Rationale**:
    *   **Reusability & Consistency**: Promotes the reuse of source definitions across multiple jobs and teams, ensuring consistent access patterns and configurations.
    *   **Decoupling**: Decouples job configurations from the specifics of data source connections and structures. Job files become simpler, referencing sources by name and version, making them more portable and easier to manage.
    *   **Rich Metadata**: Allows for rich metadata (e.g., detailed field schemas with types and descriptions, data quality rules, ownership, update frequency) to be associated directly with each data source definition.
    *   **Versioning**: Enables versioning of source definitions, allowing jobs to pin to specific versions or evolve with source changes controllably.
    *   **Discoverability**: The catalog serves as a central point for discovering available data sources.

### 6.2. Configuration-Driven Job Execution

*   **Choice**: Using YAML files (e.g., in `configs/jobs/`) to define the entirety of a batch feature engineering job, including input source references (name, version, runtime parameters), the sequence of transformations, and output sink details. These are parsed by `JobConfig` Pydantic models.
*   **Rationale**:
    *   **Declarative Approach**: Makes job definitions declarative, focusing on "what" to do rather than "how." This improves readability and maintainability.
    *   **Ease of Management**: YAML files are easy for both humans and machines to read and write. They can be version-controlled, reviewed, and managed as part of a GitOps workflow.
    *   **Flexibility**: Allows users to construct complex pipelines by combining pre-defined sources and transformers without writing Python code for each job.
    *   **No Hardcoding**: Avoids hardcoding job logic within Python scripts, making the system more adaptable to new requirements.

### 6.3. Pydantic for Configuration Validation

*   **Choice**: Extensive use of Pydantic models (e.g., `SourceDefinition`, `JobConfig`, and their nested sub-models) for defining the schema of all YAML configurations and validating them during parsing.
*   **Rationale**:
    *   **Data Integrity**: Ensures that configuration files adhere to the expected structure and data types, catching errors early before job execution.
    *   **Clear Contracts**: Pydantic models serve as clear data contracts for what constitutes a valid source or job definition.
    *   **Developer Experience**: Provides clear, typed Python objects to work with after parsing YAML, improving code readability and reducing runtime errors in the Python logic that consumes these configurations.
    *   **Automatic Documentation**: Pydantic models can be (and are, in this document) used to describe the expected configuration structure.

### 6.4. Abstraction of Core Components

*   **Choice**: Utilizing Abstract Base Classes (ABCs) like `Source` (in `domain.sources.base`) and `FeatureTransformer` (in `domain.features.transform`).
*   **Rationale**:
    *   **Clear Interfaces**: Defines a clear, common interface for different types of data sources and feature transformers.
    *   **Extensibility**: Allows new data source types (e.g., for different databases or file systems) or new feature transformation techniques to be added by subclassing these ABCs without altering the core job execution workflow.
    *   **Modularity**: Promotes modular design, as concrete implementations are interchangeable as long as they adhere to the defined interface.

### 6.5. Factory Pattern for Extensibility

*   **Choice**: Employing factory mechanisms for instantiating objects based on string identifiers from configuration files. This includes:
    *   The `get_transformer` function (from `domain.features.factory.py`) which uses `TRANSFORMER_REGISTRY`.
    *   The `SOURCE_REGISTRY` map in `runner/execute_batch_job.py` (a dictionary mapping source type strings like "databricks" to source classes like `DatabricksSparkSource`).
*   **Rationale**:
    *   **Decoupling**: Decouples the client code (e.g., the job runner) from concrete implementations of transformers and sources. The runner doesn't need to know about every possible class, only the string identifier.
    *   **Dynamic Instantiation**: Allows for dynamic instantiation of objects based on runtime configuration.
    *   **Ease of Registration**: Makes it straightforward to add new transformer types or source types by simply registering the new class with the respective registry/factory.

### 6.6. Centralized Spark Session Management

*   **Choice**: Implementing a `SparkSessionManager` (in `domain.core.spark`) to handle the creation, configuration, and lifecycle of `SparkSession` objects.
*   **Rationale**:
    *   **Consistency**: Ensures that Spark sessions are configured and managed consistently across all parts of the platform that require Spark (e.g., Spark-based sources, some transformers).
    *   **Simplified Usage**: Simplifies Spark session handling for component developers; they can just request a session from the manager.
    *   **Resource Management**: Provides a central point for managing Spark session termination.
    *   **Environment Abstraction**: Supports different Spark environments (local mode, Databricks Connect) through a single interface.

### 6.7. Modularity and Separation of Concerns

*   **Choice**: Structuring the codebase into distinct Python modules (`core`, `sources`, `features`, `jobs`, `runner`) with well-defined responsibilities.
*   **Rationale**:
    *   **Maintainability**: Makes the codebase easier to understand, maintain, and debug as components with specific functions are grouped together.
    *   **Testability**: Facilitates unit testing of individual modules and components in isolation.
    *   **Scalability of Development**: Allows different developers or teams to work on different parts of the platform concurrently with reduced chances of conflict.
    *   **Clarity**: Enhances the overall clarity and organization of the project.

## 7. Setup and Usage

This section provides a high-level overview of setting up and using the Feature Platform. **For detailed, step-by-step instructions, please refer to the main `README.md` file.**

### 7.1. Setup

1.  **Clone Repository**: Obtain the source code by cloning the repository.
2.  **Environment Setup**: Create and activate a Python virtual environment (e.g., using `venv` or `conda`).
3.  **Install Dependencies**: Install the package and its dependencies in development mode:
    ```bash
    pip install -e ".[dev]"
    ```
    This command installs all necessary packages, including those for core functionality and development tools.

### 7.2. Main Usage Pattern

1.  **Define Data Sources**:
    *   Data sources are defined as YAML files within the `source/` directory (Source Catalog), following the structure specified by the `SourceDefinition` Pydantic model (see `domain/core/source_definition.py`). Each source definition includes its name, version, type, connection configuration, schema (fields), and other metadata.
    *   Example path: `source/my_data_source/v1/my_data_source.yaml`.

2.  **Validate Source Catalog**:
    *   After defining or updating source YAML files, validate the entire catalog to ensure all definitions are correctly structured:
        ```bash
        python scripts/validate_source_catalog.py
        ```

3.  **Define Jobs**:
    *   Batch feature engineering jobs are defined as YAML files within the `configs/jobs/` directory.
    *   Each job configuration (conforming to `JobConfig` Pydantic model) specifies:
        *   `input_source`: References a source from the Source Catalog by its `name` and `version`. It can also include `load_params` for job-specific runtime arguments to customize data reading.
        *   `feature_transformers`: A list of feature transformations to apply, each specifying a registered transformer `name` and its `params`.
        *   `output_sink`: Defines where the final transformed data should be written.

4.  **Run Jobs**:
    *   Execute a defined job using the main runner script:
        ```bash
        python runner/execute_batch_job.py <path_to_your_job_config.yaml>
        ```
        For example:
        ```bash
        python runner/execute_batch_job.py configs/jobs/sample_financial_features_job.yaml
        ```

5.  **Environment Variables for Databricks**:
    *   When connecting to Databricks (either for `DatabricksSparkSource` with Databricks Connect, or `DatabricksSQLSource`), ensure necessary environment variables are set. These are used by `DatabricksConnectionConfig` and `SparkSessionManager`. Key variables include:
        *   `DATABRICKS_HOST`: Your Databricks workspace URL.
        *   `DATABRICKS_TOKEN`: A Databricks Personal Access Token (PAT).
        *   `DATABRICKS_HTTP_PATH`: The HTTP path for your Databricks SQL Warehouse (for `DatabricksSQLSource`).
        *   `DATABRICKS_CLUSTER_ID`: (For Spark Connect with `SparkSessionManager`) The ID of the cluster to connect to.
        *   `SPARK_REMOTE`: (For Spark Connect with `SparkSessionManager`) The Databricks Connect URL.

**Note**: This LLD section provides a high-level overview. For comprehensive, step-by-step setup and usage instructions, including detailed examples and troubleshooting, **please consult the main `README.md` file.**

## 8. Running as a Databricks Job

This section provides guidance for setting up and running the Feature Platform as a job on Databricks.

### 8.1. Overview

Running feature engineering pipelines as Databricks jobs offers several advantages:
*   **Leverages Cluster Resources**: Utilizes the scalable compute resources of a Databricks cluster.
*   **Avoids Local Issues**: Circumvents potential SSL/network configuration complexities often encountered with local Databricks Connect setups.
*   **Integration with Databricks Workflows**: Allows Feature Platform jobs to be easily scheduled and integrated into larger data processing workflows managed by Databricks.

The primary entry point for Databricks jobs is the `runner/databricks_job_main.py` script. This script is specifically designed to be run within the Databricks environment and correctly initializes the `SparkSessionManager` to use the existing Spark session provided by the Databricks job cluster.

### 8.2. Code Deployment / Library Installation

There are several ways to make the `feature-platform` code available to your Databricks job:

*   **Option A: Install from Git (Recommended via Databricks Repos)**
    *   **Databricks Repos**: The most straightforward method. Clone your `feature-platform` repository directly into your Databricks workspace using Databricks Repos.
        *   The Databricks job can then be configured to execute the `runner/databricks_job_main.py` script directly from this cloned repository path (e.g., `/Workspace/Repos/<user_or_shared_folder>/feature-platform/runner/databricks_job_main.py`).
        *   The Python import system within Databricks Repos typically handles the package's presence correctly if your scripts are run from the root of the cloned repo or if the repo root is added to `PYTHONPATH`.
    *   **Pip Install from Git**: Alternatively, you can install the package onto the job cluster via `pip` if the repository is accessible (e.g., a private repo with an access token or a public repo). This is done by adding a library to your cluster configuration or job task:
        ```
        pip install git+https://<your_git_provider.com>/<user_or_org>/feature-platform.git@<branch_or_tag>#egg=feature_platform
        ```
        Replace `<...>` placeholders with your specific Git repository URL and desired branch/tag.

*   **Option B: Build and Upload a Wheel**
    1.  **Build the Wheel Locally**:
        ```bash
        # Ensure 'build' package is installed: pip install build
        python -m build
        ```
        This command will generate a `.whl` file in the `dist/` directory (e.g., `dist/feature_platform-0.1.0-py3-none-any.whl`).
    2.  **Upload the Wheel**:
        *   Upload the generated `.whl` file to DBFS (e.g., `dbfs:/FileStore/libraries/feature_platform-0.1.0-py3-none-any.whl`).
        *   Install this wheel as a cluster library or as a job-dependent library.

*   **Option C: Syncing Code to DBFS (Less Common for Libraries)**
    *   While individual Python files or directories can be synced to DBFS (e.g., using `databricks fs cp ...`) and then added to the `PYTHONPATH` for a job, installing the code as a package (Option A or B) is generally preferred for better dependency management and cleaner execution.

### 8.3. Cluster Configuration

*   **Runtime**: Choose a recent Databricks Runtime ML version. These runtimes come pre-packaged with many common ML libraries, including Spark, Delta Lake, and compatible versions of PySpark. Ensure the chosen runtime's Spark version aligns with any specific Spark features used by the platform (e.g., Spark 3.3+ for certain Delta Lake capabilities if those are critical).
*   **Node Type**: Select appropriate node types based on expected workload.
    *   General purpose: Standard series (e.g., D-series on Azure, m-series on AWS).
    *   Memory-intensive: Memory-optimized instances if dealing with very large DataFrames that cause memory pressure (though Spark is designed to spill to disk).
*   **Libraries**:
    *   **`feature-platform` Package**: Ensure it's installed using one of the methods from "Code Deployment".
    *   **Other Dependencies**: The `feature-platform` package specifies its core dependencies (like `pyyaml`, `pydantic`, `delta-spark`, `databricks-sql-connector`) in its `pyproject.toml` (or `setup.py`).
        *   If installing via `pip` (from Git or wheel), these should be pulled in automatically.
        *   `pyspark` is provided by the Databricks runtime itself and should not be installed separately.
        *   `delta-spark` is typically included in Databricks runtimes.
        *   Double-check if `pyyaml`, `pydantic`, and `databricks-sql-connector` need to be added as explicit cluster libraries if they are not correctly installed as dependencies of the main package. This is usually only an issue if not building/installing the package correctly.

### 8.4. Configuration File Management

Your job configurations (`configs/jobs/*.yaml`) and Source Catalog definitions (`source/**/*.yaml`) need to be accessible by the Databricks job.

*   **Databricks Repos**: If you are using Databricks Repos to manage your `feature-platform` code, the simplest approach is to include your `configs/` and `source/` directories within the same repository. The paths used in the job parameters can then be relative to the repository root.
    *   Example: If your repo is cloned as `/Workspace/Repos/my_user/my_domain_project/`, and `databricks_job_main.py` is run from the `my_domain_project/` root, paths like `configs/jobs/my_job.yaml` and `source/` would work.
*   **DBFS**: Alternatively, upload these configuration directories to DBFS.
    *   Example:
        *   Source Catalog: `dbfs:/FileStore/domain_configs/source/`
        *   Job Configs: `dbfs:/FileStore/domain_configs/configs/jobs/`
    *   You would then provide these DBFS paths as parameters to the job.

### 8.5. Databricks Job Task Configuration

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
            "--source-catalog-path", "/dbfs/FileStore/domain_configs/source/"
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

### 8.6. Environment Variables (Optional)

*   **`FP_SOURCE_CATALOG_PATH`**: Instead of using the `--source-catalog-path` script parameter, you can configure this as a Spark environment variable at the cluster level (under "Advanced Options" -> "Spark" in the cluster configuration UI).
    *   Example: `FP_SOURCE_CATALOG_PATH=/dbfs/FileStore/domain_configs/source/`
*   **Databricks Connection Variables**: When running directly on a Databricks cluster (not using Databricks Connect from an external client), environment variables like `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID`, `SPARK_REMOTE` are generally **not** needed for the `SparkSessionManager`, as it will detect the Databricks environment and use the managed Spark session. These are primarily for client-side connections.
```

[end of DESIGN.md]
