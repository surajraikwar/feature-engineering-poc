"""
Provides the SourceRegistry for loading and managing data source definitions
(SourceDefinition objects) from the YAML-based source catalog. It allows
for discovery and retrieval of source configurations.
"""
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml
from pydantic import ValidationError

from domain.core.source_definition import SourceDefinition

logger = logging.getLogger(__name__)

class SourceRegistry:
    def __init__(self):
        self._source_definitions: Dict[Tuple[str, str], SourceDefinition] = {}

    def add_source_definition(self, source_def: SourceDefinition):
        key = (source_def.name, source_def.version)
        if key in self._source_definitions:
            raise ValueError(
                f"Source definition with name '{source_def.name}' and version '{source_def.version}' already exists."
            )
        self._source_definitions[key] = source_def
        logger.info(f"Added source definition: {source_def.name} v{source_def.version}")

    def get_source_definition(self, name: str, version: Optional[str] = None) -> Optional[SourceDefinition]:
        if version:
            return self._source_definitions.get((name, version))
        else:
            # Find all versions for the given name
            matching_versions = []
            for (src_name, src_version), src_def in self._source_definitions.items():
                if src_name == name:
                    matching_versions.append(src_def)
            
            if not matching_versions:
                return None
            if len(matching_versions) == 1:
                return matching_versions[0]
            else:
                # For now, if multiple versions exist and no specific version is requested,
                # we raise an error. Later, we might implement "latest" version logic.
                raise ValueError(
                    f"Multiple versions found for source '{name}'. Please specify a version. "
                    f"Available versions: {[v.version for v in matching_versions]}"
                )

    def get_all_source_definitions(self) -> List[SourceDefinition]:
        return list(self._source_definitions.values())

    @classmethod
    def from_yaml_dir(cls, dir_path: Path) -> "SourceRegistry":
        registry = cls()
        if not dir_path.is_dir():
            logger.error(f"Provided path '{dir_path}' is not a directory or does not exist.")
            return registry

        for yaml_file in dir_path.glob("**/*.yaml"):
            logger.info(f"Processing YAML file: {yaml_file}")
            try:
                with open(yaml_file, "r") as f:
                    yaml_data = yaml.safe_load(f)

                if not yaml_data:
                    logger.warning(f"YAML file is empty or contains no data: {yaml_file}")
                    continue
                
                if isinstance(yaml_data, list):
                    # File contains a list of source definitions
                    for item_data in yaml_data:
                        try:
                            source_def = SourceDefinition(**item_data)
                            registry.add_source_definition(source_def)
                        except ValidationError as e:
                            logger.error(f"Validation error parsing item in {yaml_file}: {e}")
                        except Exception as e:
                            logger.error(f"Error processing item in {yaml_file}: {e}")
                elif isinstance(yaml_data, dict):
                    # File contains a single source definition
                    try:
                        source_def = SourceDefinition(**yaml_data)
                        registry.add_source_definition(source_def)
                    except ValidationError as e:
                        logger.error(f"Validation error parsing {yaml_file}: {e}")
                    except Exception as e:
                        logger.error(f"Error processing {yaml_file}: {e}")
                else:
                    logger.warning(f"Unexpected YAML structure in {yaml_file}. Expected dict or list.")

            except yaml.YAMLError as e:
                logger.error(f"Error parsing YAML file {yaml_file}: {e}")
            except IOError as e:
                logger.error(f"Error reading file {yaml_file}: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while processing {yaml_file}: {e}")
        
        return registry
