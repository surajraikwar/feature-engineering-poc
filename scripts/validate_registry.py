"""Script to validate the registry structure and content."""
import os
import yaml
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Define the expected schema for entity definitions
ENTITY_SCHEMA = {
    "name": str,
    "description": str,
    "version": str,
    "is_leaf": bool,
    "primary_key": str,
    "fields": list,
    "relations": list,
}

class RegistryValidationError(Exception):
    """Custom exception for registry validation errors."""
    pass

def validate_entity_definition(entity_def: Dict[str, Any]) -> List[str]:
    """Validate a single entity definition against the schema."""
    errors = []
    
    # Check required fields
    for field, field_type in ENTITY_SCHEMA.items():
        if field not in entity_def:
            errors.append(f"Missing required field: {field}")
        elif not isinstance(entity_def[field], field_type):
            errors.append(f"Field '{field}' should be of type {field_type.__name__}")
    
    # Validate fields
    if "fields" in entity_def:
        for i, field in enumerate(entity_def["fields"]):
            if not all(k in field for k in ["name", "type"]):
                errors.append(f"Field at index {i} is missing required properties (name, type)")
    
    # Validate relations
    if "relations" in entity_def:
        for i, rel in enumerate(entity_def["relations"]):
            if not all(k in rel for k in ["to", "type"]):
                errors.append(f"Relation at index {i} is missing required properties (to, type)")
    
    return errors

def validate_registry_structure(registry_path: Path) -> List[str]:
    """Validate the registry directory structure."""
    errors = []
    
    # Check required directories
    required_dirs = ["entity", "feature", "schema"]
    for dir_name in required_dirs:
        if not (registry_path / dir_name).exists():
            errors.append(f"Missing required directory: {dir_name}")
    
    return errors

def validate_entity_files(entities_path: Path) -> List[str]:
    """Validate all entity definition files."""
    errors = []
    # Get all YAML files, excluding common_types.yaml
    entity_files = (
        list(entities_path.glob("**/*.yaml")) + 
        list(entities_path.glob("**/*.yml"))
    )
    # Filter out common_types.yaml
    entity_files = [f for f in entity_files if f.name != "common_types.yaml"]
    
    # Check for empty entity directories
    entity_dirs = [d for d in entities_path.iterdir() if d.is_dir() and d.name != "shared"]
    for entity_dir in entity_dirs:
        yaml_files = list(entity_dir.glob("*.yaml")) + list(entity_dir.glob("*.yml"))
        if not yaml_files:
            errors.append(f"No YAML files found in entity directory: {entity_dir.name}")
    
    # Validate each entity file
    for file_path in entity_files:
        try:
            with open(file_path, 'r') as f:
                try:
                    content = yaml.safe_load(f)
                    if content is None:
                        errors.append(f"Empty YAML file: {file_path}")
                        continue
                        
                    # Handle both single-document and multi-document YAML
                    entities = content if isinstance(content, list) else [content]
                    for i, entity in enumerate(entities):
                        if not entity:
                            errors.append(f"Empty entity definition in {file_path}")
                            continue
                            
                        entity_errors = validate_entity_definition(entity)
                        for err in entity_errors:
                            errors.append(f"{file_path} (entity {i + 1}): {err}")
                            
                except yaml.YAMLError as e:
                    errors.append(f"Invalid YAML in {file_path}: {str(e)}")
        except Exception as e:
            errors.append(f"Error reading {file_path}: {str(e)}")
    
    return errors

def main():
    """Main validation function."""
    registry_path = Path(__file__).parent.parent / "registry"
    
    # Validate structure
    structure_errors = validate_registry_structure(registry_path)
    
    # Validate entities
    entity_errors = validate_entity_files(registry_path / "entity")
    
    # Combine and report errors
    all_errors = structure_errors + entity_errors
    
    if all_errors:
        print("\nValidation failed with the following errors:", file=sys.stderr)
        for error in all_errors:
            print(f"- {error}", file=sys.stderr)
        sys.exit(1)
    else:
        print("Registry validation passed!")
        sys.exit(0)

if __name__ == "__main__":
    main()
