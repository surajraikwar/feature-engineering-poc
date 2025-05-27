# Feature Platform

A feature platform for managing machine learning features with a focus on data quality, lineage, and reusability.

## Project Structure

```
feature-platform/
├── config/               # Configuration files (Not extensively used yet)
├── docs/                 # Documentation
├── feature_platform/     # Core package
│   ├── core/            # Core functionality
│   │   ├── config.py    # Centralized configuration classes (e.g., DatabricksConnectionConfig)
│   │   └── spark.py     # Spark session management (SparkSessionManager)
│   ├── features/        # Feature transformation logic
│   │   ├── __init__.py
│   │   ├── factory.py   # Factory for instantiating transformers
│   │   ├── financial_transformers.py # Domain-specific transformers (e.g., financial)
│   │   └── transform.py # Base FeatureTransformer and generic example implementations
│   ├── jobs/            # Job execution logic
│   │   ├── __init__.py
│   │   └── config_loader.py # Pydantic models and loader for job YAML configs
│   ├── sources/         # Data source implementations
│   │   ├── __init__.py
│   │   ├── base.py      # Base Source class
│   │   ├── databricks_spark.py # Source for Databricks using Spark (reads Delta tables)
│   │   ├── databricks_sql.py   # Source for Databricks using SQL Connector
│   │   └── spark_base.py     # Base SparkSource class
│   ├── storage/         # Storage backends (Placeholder)
│   └── utils/           # Utility functions (Placeholder)
├── registry/            # Entity and feature definitions (YAML based)
# ... (other registry subdirs)
│   │   ├── {entity_type}/  # One per entity type
│   │   └── shared/      # Shared types and utilities
# ... (other registry subdirs)
├── runner/              # Scripts for running jobs
│   ├── execute_batch_job.py          # Generic script to run YAML-defined batch jobs
│   ├── run_databricks_connect_job.py # Example using Databricks Connect (legacy/specific)
│   └── run_spark_job.py # Example Spark job demonstrating source and feature transformers (local mode, legacy/specific)
├── scripts/             # Utility scripts
# ... (other scripts)
# ... (other scripts)
├── source/              # Source configurations (YAML definitions for data sources)
│   └── {source_type}/   # Type of source (e.g., transaction)
│       └── v{version}/  # Versioned directory (e.g., source/transaction/v1/mm_transaction_source.yaml)
└── tests/               # Test suite
    ├── integration/     # Integration tests
    └── unit/            # Unit tests
```

## Core Components

### Spark Integration
*   **`SparkSessionManager` (`feature_platform/core/spark.py`):** Manages the lifecycle of a `SparkSession`, ensuring it's created when needed and properly stopped. It can be used as a context manager or by directly calling `get_session()` and `stop_session()`.
*   **`DatabricksConnectionConfig` (`feature_platform/core/config.py`):** A centralized dataclass to hold connection parameters for Databricks, sourced from environment variables (e.g., `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`). This config is intended to be used by various Databricks-related components.

### Data Sources
The platform provides several base and concrete classes for defining data sources:
*   **`Source` (`feature_platform/sources/base.py`):** An abstract base class for all data sources.
*   **`SparkSource` (`feature_platform/sources/spark_base.py`):** An abstract base class for sources that return Spark DataFrames. It requires a `SparkSessionManager` for its operation.
*   **`DatabricksSparkSource` (`feature_platform/sources/databricks_spark.py`):** A concrete implementation of `SparkSource` for reading data from Databricks, typically Delta tables. It uses the Spark session to read data specified by a `location` (e.g., table name or path).
*   **`DatabricksSQLSource` (`feature_platform/sources/databricks_sql.py`):** A source that connects to Databricks using the `databricks-sql-connector`. It reads data using SQL queries and returns pandas DataFrames. This is suitable for scenarios where a full Spark session isn't required or available for the client.

### Feature Transformers
*   **`FeatureTransformer` (`feature_platform/features/transform.py`):** An abstract base class for defining feature transformation logic.
    *   Generic examples like `SimpleAgeCalculator` and `WithGreeting` are in `transform.py`.
    *   Domain-specific transformers like `UserSpendAggregator`, `UserMonthlyTransactionCounter`, and `UserCategoricalSpendAggregator` are implemented in `feature_platform/features/financial_transformers.py`.
*   **Transformer Factory (`feature_platform/features/factory.py`):** Transformers are registered by name (e.g., "UserSpendAggregator") and instantiated by the `get_transformer` factory function. This allows job configurations to refer to transformers by name, making the system extensible.

### Job Configuration Loading
*   **`config_loader.py` (`feature_platform/jobs/config_loader.py`):** Contains Pydantic models for validating the structure of job YAML files and a `load_job_config` function to parse these files into Python objects.

## Getting Started

### Prerequisites

- Python 3.8+
- pip
- `pyspark` (automatically installed with base dependencies)
- (Optional) Graphviz for generating diagrams

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd feature-platform
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install the package in development mode (includes `pyspark` and other core dependencies):
   ```bash
   pip install -e ".[dev]"
   ```
   The `[dev]` option also installs development tools like `pytest`, `black`, `isort`, etc.

## Running an Example Spark Job

The `runner/run_spark_job.py` script provides an end-to-end example of using the feature platform components. It demonstrates:
1.  Initializing `SparkSessionManager`.
2.  Creating a dummy Delta table locally for demonstration purposes.
3.  Configuring and using `DatabricksSparkSource` to read this dummy table.
4.  Applying `FeatureTransformer` implementations (`SimpleAgeCalculator`, `WithGreeting`) to the DataFrame.

To run the example:
```bash
python runner/run_spark_job.py
```
This job runs entirely in local Spark mode (`local[*]`) and does not require a connection to an actual Databricks workspace. The output will show the transformations being applied to the DataFrame using a dummy local Delta table.
(Note: `run_spark_job.py` and `run_databricks_connect_job.py` are older, specific-purpose runners. The primary way to run jobs is now via `execute_batch_job.py` described below.)

## Running Batch Jobs (Configuration-Driven)

The primary way to execute feature engineering tasks is through the generic `execute_batch_job.py` script. This script takes a YAML configuration file that defines the entire job, including input source, feature transformations, and output sink.

**Command:**
```bash
python runner/execute_batch_job.py <path_to_job_config_yaml>
```
For example, to run the sample financial features job:
```bash
python runner/execute_batch_job.py configs/jobs/sample_financial_features_job.yaml
```

**Execution Environment:**
*   **Local Spark:** If `SPARK_REMOTE` environment variable is not set or is empty, the job will run using a local Spark session (`local[*]`). This is useful for development and testing with smaller datasets.
*   **Databricks Connect:** To run against a Databricks cluster, you need to configure Databricks Connect and set the following environment variables:
    *   `DATABRICKS_HOST`: Your Databricks workspace URL (e.g., `https://adb-xxxxxxxxxxxxxx.xx.azuredatabricks.net`).
    *   `DATABRICKS_TOKEN`: A Databricks Personal Access Token (PAT).
    *   `DATABRICKS_CLUSTER_ID`: The ID of the Databricks cluster.
    *   `SPARK_REMOTE`: The Databricks Connect URL (e.g., `sc://<your-workspace-host-without-https>:<port>`).
    The `SparkSessionManager` will use these variables to establish a connection.

## Job Configuration (YAML)

Batch jobs are defined in YAML files. The structure is validated by Pydantic models in `feature_platform/jobs/config_loader.py`. Here's a breakdown using `configs/jobs/sample_financial_features_job.yaml` as an example:

```yaml
job_name: "financial_features_extraction_v1"
description: "Computes key financial features for users from transaction data."

input_source:
  source_type: "databricks_spark" # Identifies the source class from a registry
  config: # Parameters for the source class
    location: "jupiter.mm.fact_mm_transaction"
    format: "delta"
    # options: { ... } # Optional Spark read options

feature_transformers:
  - name: "UserSpendAggregator" # Name of the transformer class from a registry
    params: # Parameters for the transformer's __init__ method
      user_id_col: "user_id"
      timestamp_col: "timestamp"
      amount_col: "amount"
      window_days: 30
  - name: "UserMonthlyTransactionCounter"
    params: { ... }
  # ... more transformers

output_sink:
  sink_type: "display" # Type of sink (e.g., display, delta_table, parquet_files)
  config: # Parameters for the sink
    num_rows: 20
    truncate: false
```

**Key Sections:**

*   **`job_name`, `description`**: Metadata for the job.
*   **`input_source`**: Defines where to read data from.
    *   `source_type`: A string key (e.g., `"databricks_spark"`) that maps to a source class in the `SOURCE_REGISTRY` within `runner/execute_batch_job.py`.
    *   `config`: A block of parameters passed to the source class during initialization. The structure of this block depends on the `source_type`. For `"databricks_spark"`, it includes `location`, `format`, etc.
*   **`feature_transformers`**: A list of transformations to apply sequentially.
    *   Each item in the list defines one transformer.
    *   `name`: A string key (e.g., `"UserSpendAggregator"`) that maps to a `FeatureTransformer` class in the `TRANSFORMER_REGISTRY` (defined in `feature_platform/features/factory.py`).
    *   `params`: A dictionary of parameters passed to the transformer's `__init__` method.
*   **`output_sink`**: Defines what to do with the final transformed DataFrame.
    *   `sink_type`: A string key identifying the sink type. Supported types include:
        *   `"display"`: Shows a sample of the DataFrame to standard output.
            *   `num_rows`: Number of rows to show.
            *   `truncate`: Whether to truncate long string fields.
        *   `"delta_table"` (or `"overwrite_delta"`, `"append_delta"`): Writes the DataFrame to a Delta table.
            *   `path`: The path to the Delta table (e.g., `"your_catalog.your_schema.output_table"`).
            *   `mode`: Spark write mode (e.g., `"overwrite"`, `"append"`).
            *   `options`: Optional dictionary for Spark write options.
        *   `"parquet_files"` (or `"overwrite_parquet"`, `"append_parquet"`): Writes the DataFrame to Parquet files.
            *   `path`: The directory path for the Parquet files.
            *   `mode`: Spark write mode.
            *   `partition_by`: Optional list of columns to partition by.
            *   `options`: Optional dictionary for Spark write options.
    *   `config`: A block of parameters specific to the chosen `sink_type`.

*(Note: The old `YAML Configuration for Sources (source/**/*.yaml)` section can be removed or significantly condensed as job configs now define sources directly.)*

## Usage

### Working with Entities

Entity definitions are stored in the `registry/entity` directory. Each entity type has its own directory with a YAML file.

Example entity definition (`registry/entity/customer/customer.yaml`):

```yaml
name: customer
description: Represents a customer in the system
version: 1.0.0
is_leaf: false
primary_key: user_id
fields:
  - name: user_id
    type: string
    required: true
    description: Unique identifier for the customer
  # Additional fields...
relations:
  - to: transaction
    type: 1:many
    description: A customer can have multiple transactions
```

### Validating the Registry

To validate the entity definitions:

```bash
python scripts/validate_registry.py
```

### Generating Entity Diagrams

To generate an entity relationship diagram (requires Graphviz):

```bash
python scripts/generate_entity_diagram.py
# Output will be saved to docs/diagrams/entity_relationships.png
```

## Development

### Running Tests

Run all tests:

```bash
pytest
```

Run unit tests only:

```bash
pytest tests/unit/
```

Run integration tests:

```bash
pytest tests/integration/
```

### Code Quality

Format code with Black:

```bash
black .
```

Check for style issues with Flake8:

```bash
flake8
```

### Pre-commit Hooks

Pre-commit hooks are set up to run automatically before each commit. They include:

- Registry validation
- Code formatting with Black
- Linting with Flake8

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

*(Consider removing or refactoring the old `YAML Configuration for Sources (source/**/*.yaml)` section if it's now superseded by the job configuration approach.)*

### Running Tests (Alternative command, consistent with pyproject.toml)

```bash
pytest # Uses settings from pyproject.toml, including coverage
```

### Code Style

This project uses:
- Black for code formatting
- isort for import sorting
- mypy for static type checking
- pytest for testing

Run `black .` and `isort .` before committing code.
The pre-commit hooks should also handle this.
```
