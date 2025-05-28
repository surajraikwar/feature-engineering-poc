"""Entity registry for managing entities and their relationships."""

from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import logging

from .entity import Entity, Relation

logger = logging.getLogger(__name__)

class EntityRegistry:
    """Registry for managing entities and their relationships."""
    
    def __init__(self):
        """Initialize a new EntityRegistry."""
        self._entities: Dict[str, Entity] = {}
    
    def add_entity(self, entity: Entity) -> None:
        """Add an entity to the registry.
        
        Args:
            entity: The entity to add
            
        Raises:
            ValueError: If an entity with the same name already exists
        """
        if entity.name in self._entities:
            raise ValueError(f"Entity '{entity.name}' already exists in registry")
        self._entities[entity.name] = entity
        logger.debug(f"Added entity: {entity.name}")
    
    def get_entity(self, name: str) -> Optional[Entity]:
        """Get an entity by name.
        
        Args:
            name: Name of the entity to retrieve
            
        Returns:
            The entity if found, None otherwise
        """
        return self._entities.get(name)
    
    def get_all_entities(self) -> List[Entity]:
        """Get all registered entities.
        
        Returns:
            List of all registered entities
        """
        return list(self._entities.values())
    
    def validate_relationships(self) -> List[str]:
        """Validate all relationships in the registry.
        
        Returns:
            List of error messages for any invalid relationships
        """
        errors = []
        for entity in self._entities.values():
            for relation in entity.relations:
                if relation.to_entity not in self._entities:
                    errors.append(
                        f"Entity '{entity.name}' has relation to unknown entity "
                        f"'{relation.to_entity}'"
                    )
        
        # Check for circular dependencies
        if not errors:
            cycle = self._find_cycle()
            if cycle:
                errors.append(f"Circular dependency detected: {' -> '.join(cycle)}")
        
        return errors
    
    def _find_cycle(self) -> List[str]:
        """Find cycles in the entity graph using depth-first search.
        
        Returns:
            List of entity names forming a cycle if found, empty list otherwise
        """
        visited = set()
        path = []
        
        def visit(entity_name: str) -> Optional[List[str]]:
            if entity_name in path:
                idx = path.index(entity_name)
                return path[idx:]
                
            if entity_name in visited:
                return None
                
            visited.add(entity_name)
            path.append(entity_name)
            
            entity = self._entities[entity_name]
            for rel in entity.relations:
                if cycle := visit(rel.to_entity):
                    return cycle
                    
            path.pop()
            return None
        
        for entity_name in self._entities:
            if cycle := visit(entity_name):
                return cycle
                
        return []
    
    @classmethod
    def from_yaml_dir(cls, dir_path: Path) -> 'EntityRegistry':
        """Load entities from YAML files in a directory.
        
        Args:
            dir_path: Path to directory containing entity YAML files
            
        Returns:
            A new EntityRegistry instance with the loaded entities
        """
        registry = cls()
        
        if not dir_path.exists():
            logger.warning(f"Directory does not exist: {dir_path}")
            return registry
            
        for yaml_file in dir_path.glob("**/*.yaml"):
            try:
                entity = Entity.from_yaml(yaml_file)
                registry.add_entity(entity)
                logger.info(f"Loaded entity from {yaml_file}")
            except Exception as e:
                logger.error(f"Error loading entity from {yaml_file}: {str(e)}")
                raise
        
        return registry
    
    def to_yaml_dir(self, dir_path: Path) -> None:
        """Save all entities to YAML files in a directory.
        
        Args:
            dir_path: Path to directory where YAML files will be saved
        """
        dir_path.mkdir(parents=True, exist_ok=True)
        
        for entity in self._entities.values():
            file_path = dir_path / f"{entity.name.lower()}.yaml"
            entity.to_yaml(file_path)
            logger.debug(f"Saved entity to {file_path}")
    
    def __contains__(self, name: str) -> bool:
        """Check if an entity exists in the registry."""
        return name in self._entities
    
    def __len__(self) -> int:
        """Get the number of entities in the registry."""
        return len(self._entities)
    
    def __str__(self) -> str:
        return f"EntityRegistry(entities={len(self)})"
