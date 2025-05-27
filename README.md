# Feature Platform

A feature platform for managing machine learning features with a focus on data quality, lineage, and reusability.

## Project Structure

```
feature-platform/
├── config/               # Configuration files
├── docs/                 # Documentation
├── feature_platform/     # Core package
│   ├── core/            # Core functionality
│   ├── sources/         # Data source implementations
│   ├── storage/         # Storage backends
│   └── utils/           # Utility functions
├── registry/            # Entity and feature definitions
│   ├── entity/          # Entity definitions
│   │   ├── {entity_type}/  # One per entity type
│   │   └── shared/      # Shared types and utilities
│   ├── feature/         # Feature definitions
│   └── schema/          # Shared schemas and types
├── runner/              # Scripts for running the platform
├── scripts/             # Utility scripts
│   ├── generate_entity_diagram.py  # Generate ERD
│   ├── migrate_sources.py          # Migrate source configs
│   ├── validate_registry.py        # Validate registry
│   └── validate_sources.py         # Validate sources
├── source/              # Source configurations
│   └── {source_type}/   # Type of source (e.g., transaction)
│       └── v{version}/  # Versioned directory
└── tests/               # Test suite
    ├── integration/     # Integration tests
    └── unit/            # Unit tests
```

## Getting Started

### Prerequisites

- Python 3.8+
- pip
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

3. Install the package in development mode:
   ```bash
   pip install -e ".[dev]"
   ```

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

### Running Tests

```bash
pytest --cov=domain --cov-report=term-missing
```

### Code Style

This project uses:
- Black for code formatting
- isort for import sorting
- mypy for static type checking
- pytest for testing

Run `black .` and `isort .` before committing code.
