"""Tests for the entity module."""

import pytest
from pathlib import Path
import tempfile
import yaml

from domain.core.entity import Entity, Relation


def test_entity_creation():
    """Test creating an entity with relations."""
    # Create a relation
    relation = Relation(
        to_entity="transaction",
        relation_type="one_to_many",
        foreign_key="customer_id"
    )
    
    # Create an entity
    entity = Entity(
        name="customer",
        primary_key="id",
        description="Customer information",
        relations=[relation],
        metadata={"owner": "analytics"}
    )
    
    # Assert basic properties
    assert entity.name == "customer"
    assert entity.primary_key == "id"
    assert entity.description == "Customer information"
    assert len(entity.relations) == 1
    assert entity.metadata["owner"] == "analytics"
    
    # Assert relation
    assert entity.relations[0].to_entity == "transaction"
    assert entity.relations[0].relation_type == "one_to_many"
    assert entity.relations[0].foreign_key == "customer_id"


def test_entity_to_from_dict():
    """Test converting entity to/from dictionary."""
    # Create entity with relation
    entity = Entity(
        name="customer",
        primary_key="id",
        relations=[
            Relation("transaction", "one_to_many", "customer_id")
        ]
    )
    
    # Convert to dict and back
    entity_dict = entity.to_dict()
    new_entity = Entity.from_dict(entity_dict)
    
    # Assert equality
    assert new_entity.name == entity.name
    assert new_entity.primary_key == entity.primary_key
    assert len(new_entity.relations) == len(entity.relations)
    assert new_entity.relations[0].to_entity == "transaction"


def test_entity_yaml_serialization():
    """Test serializing/deserializing entity to/from YAML."""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "customer.yaml"
        
        # Create and save entity
        entity = Entity(
            name="customer",
            primary_key="id",
            description="Test customer entity"
        )
        entity.to_yaml(temp_path)
        
        # Load and verify
        loaded_entity = Entity.from_yaml(temp_path)
        assert loaded_entity.name == "customer"
        assert loaded_entity.primary_key == "id"
        assert loaded_entity.description == "Test customer entity"


def test_relation_equality():
    """Test relation equality comparison."""
    rel1 = Relation("transaction", "one_to_many", "customer_id")
    rel2 = Relation("transaction", "one_to_many", "customer_id")
    rel3 = Relation("order", "one_to_many", "customer_id")
    
    assert rel1 == rel2
    assert rel1 != rel3


def test_entity_str_representation():
    """Test string representation of entity."""
    entity = Entity("customer", "id")
    assert str(entity) == "Entity(name='customer', primary_key='id')"
