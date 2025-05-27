# Feature Platform Registry

This directory contains all entity and feature definitions for the feature platform.

## Directory Structure

```
registry/
├── entity/              # Entity definitions
│   ├── {entity_type}/   # One directory per entity type
│   │   ├── {entity_name}.yaml  # Main entity definition
│   │   └── relations/          # Relationship definitions
│   └── shared/           # Shared types and utilities
├── feature/             # Feature definitions
└── schema/              # Shared schemas and types
```

## Entity Definitions

Each entity should be defined in its own YAML file following this schema:

```yaml
# registry/entity/{entity_type}/{entity_name}.yaml
name: string           # Unique entity name (snake_case)
description: string    # Human-readable description
version: string        # Semantic version (e.g., "1.0.0")
is_leaf: boolean       # Whether this is a leaf entity
primary_key: string    # Primary key field name
fields:                # List of fields
  - name: string       # Field name (snake_case)
    type: string      # Field type (string, integer, float, boolean, datetime, etc.)
    required: boolean # Whether the field is required
    description: string # Field description
relations:             # Relationships to other entities
  - to: string         # Target entity name
    type: string       # Relationship type (1:1, 1:many, many:1, many:many)
    foreign_key: string # Foreign key field name (if applicable)
```

## Versioning

- Use semantic versioning (MAJOR.MINOR.PATCH)
- Create a new version when making breaking changes
- Document changes in the CHANGELOG.md

## Validation

Run the validation script to check your definitions:

```bash
python scripts/validate_registry.py
```

## Best Practices

1. Keep entity definitions small and focused
2. Use consistent naming conventions (snake_case for fields)
3. Document all fields and relationships
4. Add examples for complex types
5. Keep related entities together
