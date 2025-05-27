"""Script to validate source configurations."""
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys

# Define the expected schema for source configurations
SOURCE_SCHEMA = {
    "name": str,
    "description": str,
    "version": str,
    "type": str,
    "entity": str,
    "location": str,
    "fields": list,
    "config": dict,
}

class SourceValidationError(Exception):
    """Custom exception for source validation errors."""
    pass

def validate_source_config(config: Dict[str, Any]) -> List[str]:
    """Validate a single source configuration."""
    errors = []
    
    # Check required fields
    for field, field_type in SOURCE_SCHEMA.items():
        if field not in config:
            errors.append(f"Missing required field: {field}")
        elif not isinstance(config[field], field_type):
            errors.append(f"Field '{field}' should be of type {field_type.__name__}")
    
    # Validate fields
    if "fields" in config:
        for i, field in enumerate(config["fields"]):
            if not all(k in field for k in ["name", "type"]):
                errors.append(f"Field at index {i} is missing required properties (name, type)")
    
    # Validate config section
    if "config" in config:
        config_section = config["config"]
        if not isinstance(config_section, dict):
            errors.append("Config section must be a dictionary")
    
    # Validate version format (semver)
    if "version" in config:
        version = config["version"]
        if not (isinstance(version, str) and version.startswith("v")):
            errors.append("Version should start with 'v' (e.g., 'v1.0.0')")
    
    return errors

def validate_source_files(sources_path: Path) -> List[str]:
    """Validate all source configuration files."""
    errors = []
    source_files = list(sources_path.glob("**/*.yaml")) + list(sources_path.glob("**/*.yml"))
    
    for file_path in source_files:
        try:
            with open(file_path, 'r') as f:
                try:
                    content = yaml.safe_load(f)
                    if content is None:
                        errors.append(f"Empty YAML file: {file_path}")
                        continue
                        
                    # Handle both single-document and multi-document YAML
                    sources = content if isinstance(content, list) else [content]
                    for i, source in enumerate(sources):
                        if not source:
                            errors.append(f"Empty source definition in {file_path}")
                            continue
                            
                        source_errors = validate_source_config(source)
                        for err in source_errors:
                            errors.append(f"{file_path} (source {i + 1}): {err}")
                            
                except yaml.YAMLError as e:
                    errors.append(f"Invalid YAML in {file_path}: {str(e)}")
        except Exception as e:
            errors.append(f"Error reading {file_path}: {str(e)}")
    
    return errors

def main():
    """Main validation function."""
    sources_path = Path(__file__).parent.parent / "source"
    
    if not sources_path.exists():
        print(f"Source directory not found: {sources_path}")
        sys.exit(1)
    
    # Validate sources
    errors = validate_source_files(sources_path)
    
    if errors:
        print("\nValidation failed with the following errors:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        sys.exit(1)
    else:
        print("Source validation passed!")
        sys.exit(0)

if __name__ == "__main__":
    main()
