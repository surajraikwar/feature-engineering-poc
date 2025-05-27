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
│   │   └── transform.py # Base FeatureTransformer and example implementations
│   ├── sources/         # Data source implementations
│   │   ├── __init__.py
│   │   ├── base.py      # Base Source class
│   │   ├── databricks_spark.py # Source for Databricks using Spark (reads Delta tables)
│   │   ├── databricks_sql.py   # Source for Databricks using SQL Connector
│   │   └── spark_base.py     # Base SparkSource class
│   ├── storage/         # Storage backends (Placeholder)
│   └── utils/           # Utility functions (Placeholder)
├── registry/            # Entity and feature definitions (YAML based)
│   ├── entity/          # Entity definitions
│   │   ├── {entity_type}/  # One per entity type
│   │   └── shared/      # Shared types and utilities
│   ├── feature/         # Feature definitions (Placeholder)
│   └── schema/          # Shared schemas and types (Placeholder)
├── runner/              # Scripts for running example jobs
│   └── run_spark_job.py # Example Spark job demonstrating source and feature transformers
├── scripts/             # Utility scripts
│   ├── generate_entity_diagram.py  # Generate ERD
│   ├── migrate_sources.py          # Migrate source configs (Placeholder)
│   ├── validate_registry.py        # Validate registry
│   └── validate_sources.py         # Validate sources (Placeholder)
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
*   **`FeatureTransformer` (`feature_platform/features/transform.py`):** An abstract base class for defining feature transformation logic. Concrete implementations (e.g., `SimpleAgeCalculator`, `WithGreeting` also in `transform.py`) take a Spark DataFrame as input, apply transformations, and return a new DataFrame with computed features.

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
This job runs entirely in local Spark mode (`local[*]`) and does not require a connection to an actual Databricks workspace. The output will show the transformations being applied to the DataFrame.

## YAML Configuration for Sources (`source/**/*.yaml`)

Data sources can be defined via YAML files (e.g., `source/transaction/v1/mm_transaction_source.yaml`). The Python classes `DatabricksSparkSourceConfig` (for `DatabricksSparkSource`) and `DeltaSQLSourceConfig` (for `DatabricksSQLSource`) are designed to be populated from these YAML files.

**Current State of YAML Processing:**

*   **Directly Used Fields:**
    *   `name`: Name of the source.
    *   `entity`: The entity this source relates to.
    *   `type`: The type of source (e.g., "delta", "sql_table"). Used by `SourceConfig`.
    *   `location`: The path to the data (e.g., "catalog.schema.table" for Databricks tables, or a file path for local delta tables in the example runner).
    *   `format`: The format for Spark reads (e.g., "delta", "parquet"). Used by `SparkSourceConfig`.
    *   `options`: A dictionary of options for Spark reads (e.g., `{"header": "true"}` for CSVs). Used by `SparkSourceConfig`.
    *   `fields`: A list of field names expected in the source.
    *   `timestamp_column`: Used by `DeltaSQLSourceConfig` for time-based filtering.
    *   `partition_filters`: Used by `DeltaSQLSourceConfig` to add filters to SQL queries.
    *   `databricks_connection`: Can nest `DatabricksConnectionConfig` parameters if needed by `DeltaSQLSourceConfig`, though typically these are picked from environment variables.

*   **Recognized but Not Yet Implemented in Execution Logic:**
    The current YAML parsing logic (within `SourceConfig` and its derivatives) might recognize additional keys if they are defined within a nested `config:` block in the YAML (e.g., as seen in `source/transaction/v1/mm_transaction_source.yaml` which has `incremental`, `schedule`, `quality_checks`, `notifications`).
    However, while these keys are loaded into the configuration objects if present, the current Python source classes (`DatabricksSparkSource`, `DatabricksSQLSource`) **do not yet implement functionalities based on these specific keys.** For example:
    *   `schedule` information is not used to run jobs automatically.
    *   `quality_checks` are not automatically enforced during data reading.
    *   `incremental` load logic is not implemented based on these flags.
    This means that such configurations are parsed but their implied features (like actual job scheduling or data quality enforcement) are future enhancements and not currently active in the execution flow.

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
