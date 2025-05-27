# Feature Platform

A feature platform for managing machine learning features with a focus on data quality, lineage, and reusability.

## Project Structure

```
feature-platform/
├── config/               # Configuration files (Not extensively used yet)
├── configs/              # Job configurations
│   └── jobs/
│       └── sample_financial_features_job.yaml # Example job definition
├── docs/                 # Documentation (including entity diagrams)
├── feature_platform/     # Core package
│   ├── core/             # Core functionality
│   │   ├── __init__.py
│   │   ├── config.py     # Centralized configuration (e.g., DatabricksConnectionConfig)
│   │   ├── entity.py     # Entity and Relation definitions (Pydantic models for registry)
│   │   ├── registry.py   # EntityRegistry for loading and managing entity definitions
│   │   ├── source_definition.py # Pydantic models for source YAML definitions
│   │   ├── source_registry.py   # Loads and manages source definitions from the catalog
│   │   └── spark.py      # Spark session management (SparkSessionManager)
│   ├── features/         # Feature transformation logic
│   │   ├── __init__.py
│   │   ├── factory.py    # Factory for instantiating transformers
│   │   ├── financial_transformers.py # Domain-specific transformers
│   │   └── transform.py  # Base FeatureTransformer and generic implementations
│   ├── jobs/             # Job execution logic
│   │   ├── __init__.py
│   │   └── config_loader.py # Pydantic models and loader for job YAML configs
│   ├── sources/          # Data source implementations
│   │   ├── __init__.py
│   │   ├── base.py       # Base Source class
│   │   ├── databricks_spark.py # Source for Databricks using Spark (reads Delta tables)
│   │   ├── databricks_sql.py   # Source for Databricks using SQL Connector
│   │   └── spark_base.py # Base SparkSource class
│   └── __init__.py
├── registry/             # Entity definitions (YAML based) - See "Entity Registry" section
│   └── entity/
│       └── customer/
│           └── customer.yaml # Example entity definition
├── runner/               # Main script for running jobs
│   └── execute_batch_job.py # Generic script to run YAML-defined batch jobs
├── scripts/              # Utility and operational scripts
│   ├── validate_registry.py        # Validates entity definitions in the registry
│   ├── generate_entity_diagram.py  # Generates entity relationship diagrams
│   ├── validate_source_catalog.py  # Validates source definitions in the source/ catalog
│   └── archived_runners/           # Legacy runner scripts
│       ├── run_databricks_connect_job.py
│       └── run_spark_job.py
├── source/               # Source Catalog: Reusable YAML definitions for data sources
│   ├── README.md         # Describes the structure of source definition YAML files
│   └── {source_name}/
│       └── v{version}/
│           └── {source_name}.yaml # e.g., source/transaction/v1/mm_transaction_source.yaml
└── tests/                # Test suite
    ├── integration/      # Integration tests
    └── unit/             # Unit tests
```

## Core Components

### Spark Integration
*   **`SparkSessionManager` (`feature_platform/core/spark.py`):** Manages the lifecycle of a `SparkSession`.
*   **`DatabricksConnectionConfig` (`feature_platform/core/config.py`):** Centralized dataclass for Databricks connection parameters (hostname, token, HTTP path, catalog, schema), primarily sourced from environment variables.

### Source Catalog
The `source/` directory serves as a **catalog of reusable, versioned data source definitions**. Each data source is defined in a YAML file.
*   **Structure:** Typically `source/{source_name}/v{version}/{source_name}.yaml`.
*   **Content:** Each YAML file defines a single source, specifying its `name`, `version`, `type` (e.g., "databricks", "snowflake"), `entity` it relates to, connection/access `config` (like table names, paths, queries, database details), `fields` schema, and optional `quality_checks`. For detailed structure, see `source/README.md`.
*   **`SourceDefinition` (`feature_platform/core/source_definition.py`):** Pydantic models that define the expected structure of these source YAML files, enabling validation.
*   **`SourceRegistry` (`feature_platform/core/source_registry.py`):** A class responsible for loading all source definitions from the `source/` directory, validating them against `SourceDefinition` models, and making them retrievable by `name` and `version`.

### Data Source Implementations
These are Python classes that know how to read data based on a `SourceDefinition` (retrieved from the catalog).
*   **`Source` (`feature_platform/sources/base.py`):** Abstract base class for all data sources.
*   **`SparkSource` (`feature_platform/sources/spark_base.py`):** Abstract base class for sources returning Spark DataFrames.
*   **`DatabricksSparkSource` (`feature_platform/sources/databricks_spark.py`):** Reads data from Databricks (typically Delta tables) using Spark. Its configuration (table name, format, etc.) is now primarily derived from a `SourceDefinition` loaded from the Source Catalog.
*   **`DatabricksSQLSource` (`feature_platform/sources/databricks_sql.py`):** Reads data from Databricks using the SQL Connector (returns pandas DataFrames). Similarly, its configuration is derived from the Source Catalog.

### Feature Transformers
*   **`FeatureTransformer` (`feature_platform/features/transform.py`):** Abstract base class for feature transformation logic.
*   **Transformer Factory (`feature_platform/features/factory.py`):** Transformers are registered and instantiated by the `get_transformer` factory function, allowing job configurations to refer to them by name.

### Job Configuration & Loading
*   **`config_loader.py` (`feature_platform/jobs/config_loader.py`):** Contains Pydantic models for job YAML structure (see "Job Configuration (YAML)" section) and the `load_job_config` function.

### Entity Registry
*   **Location:** `registry/entity/`
*   **`Entity` & `Relation` (`feature_platform/core/entity.py`):** Pydantic models for entity and relationship definitions.
*   **`EntityRegistry` (`feature_platform/core/registry.py`):** Loads and manages entity definitions from YAML files.

## Getting Started

### Prerequisites

- Python 3.8+
- pip
- (Optional) Graphviz for `scripts/generate_entity_diagram.py`

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

3. Install the package in development mode:
   ```bash
   pip install -e ".[dev]"
   ```
   This installs the package itself, along with core dependencies (`pyspark`, `pyyaml`, `pydantic`) and development tools (`pytest`, `black`, `isort`, etc.).

## Running Batch Jobs (Configuration-Driven)

The primary way to execute feature engineering tasks is through the `runner/execute_batch_job.py` script. This script takes a YAML configuration file that defines the job.

**Command:**
```bash
python runner/execute_batch_job.py <path_to_job_config_yaml>
```
For example, to run the sample financial features job:
```bash
python runner/execute_batch_job.py configs/jobs/sample_financial_features_job.yaml
```

**Functionality:**
The `execute_batch_job.py` script:
1.  Loads the job configuration YAML.
2.  Uses the `SourceRegistry` (initialized from `source/`) to look up and instantiate the data source specified in the job's `input_source` section. This means the details of the source (like table names, paths, connection info) are fetched from the YAML files in the `source/` catalog.
3.  Applies the sequence of feature transformers defined in the job.
4.  Directs the output to the specified sink (e.g., display, Delta table).

**Execution Environment:**
*   **Local Spark:** If `SPARK_REMOTE` environment variable is not set, runs using a local Spark session (`local[*]`).
*   **Databricks Connect:** Set `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CLUSTER_ID`, and `SPARK_REMOTE` to run against a Databricks cluster. The `SparkSessionManager` uses these.

## Job Configuration (YAML)

Batch jobs are defined in YAML files (e.g., `configs/jobs/sample_financial_features_job.yaml`). The structure is validated by Pydantic models in `feature_platform/jobs/config_loader.py`.

```yaml
job_name: "financial_features_extraction_v1"
description: "Computes key financial features for users from transaction data."

input_source:
  name: "mm_transaction_source"  # Name of the source in the source/ catalog
  version: "v1.0.0"             # Optional: specific version from the catalog
  load_params:                  # Optional: job-specific runtime parameters for the source
    # These parameters are passed to the source's read method or used as options.
    # Example for a source that supports date filtering:
    # filter_start_date: "2023-01-01"
    # filter_end_date: "2023-01-31"

feature_transformers:
  - name: "UserSpendAggregator" # Name of the transformer class from a registry
    params: # Parameters for the transformer's __init__ method
      user_id_col: "user_id"
      timestamp_col: "timestamp"
      amount_col: "amount"
      window_days: 30
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
    *   `name`: References a data source defined in the `source/` catalog (e.g., "mm_transaction_source").
    *   `version` (Optional): Specifies a version of the source from the catalog. If omitted, the `SourceRegistry` might fetch the latest or a default version (current behavior: raises error if multiple versions exist and none specified, unless a "latest" logic is implemented in `SourceRegistry`).
    *   `load_params` (Optional): A dictionary of job-specific runtime parameters passed to the source's read operation (e.g., date filters, query parameters). These can override or supplement options defined in the source catalog YAML.
    *   The detailed configuration (like table name, path, format, connection details) is now pulled from the corresponding YAML file in the `source/` catalog by the runner using the `name` and `version`.
*   **`feature_transformers`**: A list of transformations to apply sequentially.
    *   `name`: Maps to a `FeatureTransformer` class in the `TRANSFORMER_REGISTRY`.
    *   `params`: Parameters for the transformer's `__init__` method.
*   **`output_sink`**: Defines what to do with the final transformed DataFrame (e.g., `display`, `delta_table`, `parquet_files`).

(Note: The old `YAML Configuration for Sources (source/**/*.yaml)` section is now effectively replaced by the "Source Catalog" section and how `input_source` in jobs refers to it.)

## Scripts

The `scripts/` directory contains various utility and operational scripts:

*   **`validate_registry.py`**: Validates all entity definitions in the `registry/entity/` directory against the Pydantic models.
*   **`generate_entity_diagram.py`**: Generates an entity relationship diagram from the definitions in `registry/entity/` (requires Graphviz). Output is typically saved to `docs/diagrams/`.
*   **`validate_source_catalog.py`**: Validates all data source definition YAML files within the `source/` catalog against the `SourceDefinition` Pydantic models. This helps ensure that source definitions are correctly structured before they are used in jobs.
*   **`archived_runners/`**: Contains legacy runner scripts (`run_databricks_connect_job.py`, `run_spark_job.py`) that are kept for reference but are superseded by `runner/execute_batch_job.py`.

## Development

### Running Tests

Run all tests:
```bash
pytest
```
This command uses settings from `pyproject.toml`, which may include coverage configuration.

Run unit tests only:
```bash
pytest tests/unit/
```

Run integration tests:
```bash
pytest tests/integration/
```

### Code Quality & Style

This project uses:
- **Black** for code formatting.
- **isort** for import sorting.
- **Flake8** for linting (style and complexity checks).
- **Mypy** for static type checking (configured in `pyproject.toml`).

To format code:
```bash
black .
isort .
```

To check for style issues and type errors:
```bash
flake8 .
mypy . # Ensure mypy is configured and run from the project root
```

### Pre-commit Hooks

Pre-commit hooks are set up to run checks automatically before each commit. They typically include:
- Registry validation (`scripts/validate_registry.py`)
- Source catalog validation (`scripts/validate_source_catalog.py`)
- Code formatting (Black, isort)
- Linting (Flake8)
- Type checking (Mypy)

This helps ensure code quality and consistency.

## Contributing

1. Fork the repository.
2. Create a feature branch (`git checkout -b feature/your-amazing-feature`).
3. Commit your changes (`git commit -m 'Add your amazing feature'`).
4. Push to the branch (`git push origin feature/your-amazing-feature`).
5. Open a Pull Request.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.
