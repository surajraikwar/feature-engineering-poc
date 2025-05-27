# Source Configurations

This directory contains configuration files for various data sources used in the feature platform.

## Directory Structure

```
source/
├── {source_type}/         # Type of source (e.g., transaction, email, sms)
│   └── v{version}/        # Versioned directory (e.g., v1, v2)
│       └── *.yaml       # Source configuration files
```

## Configuration Format

Each source configuration file should follow this schema:

```yaml
# Required fields
name: string              # Unique name for this source
description: string       # Human-readable description
version: string           # Configuration version (e.g., "1.0.0")
type: string             # Source type (e.g., "databricks", "snowflake")
entity: string           # Target entity this source populates
location: string         # Source location (format depends on source type)

# Optional fields
fields:                  # List of fields to include
  - name: string         # Field name
    type: string         # Field type
    description: string  # Field description
    required: boolean    # Whether the field is required
    default: any         # Default value (optional)

# Source-specific configuration
config:
  # Databricks specific
  catalog: string        # Databricks catalog name
  schema: string         # Databricks schema name
  table: string          # Table name
  
  # Query configuration
  query: string          # Optional: raw SQL query
  incremental: boolean   # Whether this is an incremental load
  
  # Scheduling
  schedule: string       # Cron-style schedule (e.g., "0 0 * * *" for daily)
  
  # Data quality checks
  quality_checks:
    - type: string      # Type of check (e.g., "not_null", "unique")
      field: string     # Field to check
      condition: string  # Condition for the check

# Metadata
created_at: timestamp    # When this config was created
created_by: string       # Who created this config
updated_at: timestamp    # When this config was last updated
```

## Versioning

- Each major change to a source configuration should create a new version directory
- Version directories follow semantic versioning (v1, v2, etc.)
- The latest version should be symlinked to a `latest` directory

## Example

See `transaction/v1/` for an example configuration.
