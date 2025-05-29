"""Entity and relation definitions for the feature platform."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from pathlib import Path
import yaml


@dataclass
class FieldDefinition:
    """Represents the definition of a field within an entity.

    Attributes:
        name: Name of the field.
        type: Data type of the field.
        required: Whether the field is required. Defaults to False.
        description: Optional description of the field.
        is_primary_key: Whether this field is the primary key. Defaults to False.
    """
    name: str
    type: str
    required: bool = False
    description: Optional[str] = None
    is_primary_key: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert field definition to dictionary."""
        return {
            'name': self.name,
            'type': self.type,
            'required': self.required,
            'description': self.description,
            'is_primary_key': self.is_primary_key,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FieldDefinition':
        """Create a FieldDefinition from a dictionary."""
        return cls(
            name=data['name'],
            type=data['type'],
            required=data.get('required', False),
            description=data.get('description'),
            is_primary_key=data.get('is_primary_key', False), # PK flag can be in dict
        )


@dataclass
class Relation:
    """Represents a relationship between entities.
    
    Attributes:
        to_entity: Name of the target entity
        relation_type: Type of relationship ('one_to_many', 'many_to_one', 'one_to_one')
        foreign_key: Optional name of the foreign key column
    """
    to_entity: str
    relation_type: str
    foreign_key: Optional[str] = None
    
    def __str__(self) -> str:
        return f"{self.relation_type} → {self.to_entity}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert relation to dictionary."""
        return {
            'to_entity': self.to_entity,
            'relation_type': self.relation_type,
            'foreign_key': self.foreign_key
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Relation':
        """Create a Relation from a dictionary."""
        return cls(
            to_entity=data['to_entity'],
            relation_type=data['relation_type'],
            foreign_key=data.get('foreign_key')
        )


@dataclass
class Entity:
    """Represents an entity in the feature platform.
    
    Attributes:
        name: Name of the entity
        version: Version of the entity definition
        primary_key: Name of the primary key field
        fields: List of field definitions for the entity
        is_leaf: Whether this is a leaf entity (no children)
        description: Optional description of the entity
        relations: List of relationships to other entities
        metadata: Additional metadata for the entity
    """
    name: str
    version: str # New attribute
    primary_key: str
    fields: List[FieldDefinition] = field(default_factory=list) # New attribute
    is_leaf: bool = False
    description: str = ""
    relations: List[Relation] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_relation(self, relation: Relation) -> None:
        """Add a relationship to another entity."""
        self.relations.append(relation)

    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary."""
        return {
            'name': self.name,
            'version': self.version, # New
            'primary_key': self.primary_key,
            'fields': [f.to_dict() for f in self.fields], # New
            'is_leaf': self.is_leaf,
            'description': self.description,
            'relations': [rel.to_dict() for rel in self.relations],
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Entity':
        """Create an Entity from a dictionary."""
        relations = [
            Relation.from_dict(rel_data)
            for rel_data in data.get('relations', [])
        ]
        
        # Deserialize fields and identify primary key
        field_definitions = []
        primary_key_name = data['primary_key']
        for field_data in data.get('fields', []):
            field_def = FieldDefinition.from_dict(field_data)
            if field_def.name == primary_key_name:
                field_def.is_primary_key = True
            field_definitions.append(field_def)

        return cls(
            name=data['name'],
            version=data['version'], # New
            primary_key=primary_key_name,
            fields=field_definitions, # New
            is_leaf=data.get('is_leaf', False),
            description=data.get('description', ''),
            relations=relations,
            metadata=data.get('metadata', {})
        )

    @classmethod
    def from_yaml(cls, yaml_path: Path) -> 'Entity':
        """Load an entity from a YAML file."""
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)
    
    def to_yaml(self, yaml_path: Path) -> None:
        """Save the entity to a YAML file."""
        with open(yaml_path, 'w') as f:
            yaml.safe_dump(self.to_dict(), f, sort_keys=False)

    def get_relation(self, target_entity_name: str) -> Optional[Relation]:
        """
        Finds and returns a relation to the specified target entity.

        Args:
            target_entity_name: The name of the target entity to find a relation for.

        Returns:
            The Relation object if found, otherwise None.
        """
        for relation in self.relations:
            if relation.to_entity == target_entity_name:
                return relation
        return None
    
    def __str__(self) -> str:
        return f"Entity(name='{self.name}', primary_key='{self.primary_key}')"
