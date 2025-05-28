"""Tests for the entity module."""

import pytest
from pathlib import Path
import tempfile
import yaml

from domain.core.entity import Entity, Relation, FieldDefinition


def test_entity_creation_and_field_definitions():
    """Test creating an entity with relations, version, and field definitions."""
    # Field definitions
    field_id = FieldDefinition(name="id", type="string", required=True, description="Primary key")
    field_name = FieldDefinition(name="name", type="string", required=True, description="Customer name")
    field_age = FieldDefinition(name="age", type="integer", description="Customer age")

    # Create a relation
    relation = Relation(
        to_entity="transaction",
        relation_type="one_to_many",
        foreign_key="customer_id"
    )
    
    # Create an entity
    entity = Entity(
        name="customer",
        version="1.0.0",
        primary_key="id",
        fields=[field_id, field_name, field_age],
        description="Customer information",
        relations=[relation],
        metadata={"owner": "analytics"}
    )
    
    # Assert basic properties
    assert entity.name == "customer"
    assert entity.version == "1.0.0"
    assert entity.primary_key == "id"
    assert entity.description == "Customer information"
    assert len(entity.relations) == 1
    assert entity.metadata["owner"] == "analytics"
    
    # Assert relation
    assert entity.relations[0].to_entity == "transaction"
    assert entity.relations[0].relation_type == "one_to_many"
    assert entity.relations[0].foreign_key == "customer_id"

    # Assert fields
    assert len(entity.fields) == 3
    assert entity.fields[0].name == "id"
    assert entity.fields[0].type == "string"
    assert entity.fields[0].is_primary_key is False # is_primary_key is set by from_dict or manual
    
    # Test to_dict and from_dict which handles is_primary_key logic
    entity_dict = entity.to_dict()
    # Manually set is_primary_key in the dict as to_dict() would have it if fields were processed by from_dict
    for f_dict in entity_dict["fields"]:
        if f_dict["name"] == entity.primary_key:
            f_dict["is_primary_key"] = True # Simulate what from_dict would achieve based on primary_key
            
    hydrated_entity = Entity.from_dict(entity_dict)
    
    assert hydrated_entity.fields[0].name == "id"
    assert hydrated_entity.fields[0].is_primary_key is True
    assert hydrated_entity.fields[1].name == "name"
    assert hydrated_entity.fields[1].is_primary_key is False
    assert hydrated_entity.fields[2].name == "age"
    assert hydrated_entity.fields[2].is_primary_key is False


def test_entity_to_from_dict_with_version_and_fields():
    """Test converting entity with version and fields to/from dictionary."""
    # Define fields as dictionaries for from_dict consumption
    fields_data_for_from_dict = [
        {"name": "user_id", "type": "string", "required": True, "description": "Primary key"},
        {"name": "username", "type": "string", "required": True, "description": "User's name"},
        {"name": "email", "type": "string", "required": False}
    ]

    entity_data = {
        "name": "user",
        "version": "1.2.3",
        "primary_key": "user_id",
        "fields": fields_data_for_from_dict,
        "relations": [
            {"to_entity": "order", "relation_type": "one_to_many", "foreign_key": "user_id"}
        ],
        "description": "User entity for testing",
        "is_leaf": False,
        "metadata": {"source": "test_suite"}
    }
    
    # Create entity using from_dict
    entity = Entity.from_dict(entity_data)
    
    # Assert basic properties
    assert entity.name == "user"
    assert entity.version == "1.2.3"
    assert entity.primary_key == "user_id"
    assert entity.description == "User entity for testing"
    assert entity.is_leaf is False
    assert entity.metadata["source"] == "test_suite"

    # Assert fields
    assert len(entity.fields) == 3
    
    id_field = next(f for f in entity.fields if f.name == "user_id")
    username_field = next(f for f in entity.fields if f.name == "username")
    email_field = next(f for f in entity.fields if f.name == "email")

    assert id_field.type == "string"
    assert id_field.required is True
    assert id_field.description == "Primary key"
    assert id_field.is_primary_key is True

    assert username_field.type == "string"
    assert username_field.required is True
    assert username_field.description == "User's name"
    assert username_field.is_primary_key is False

    assert email_field.type == "string"
    assert email_field.required is False
    assert email_field.is_primary_key is False

    # Assert relations
    assert len(entity.relations) == 1
    assert entity.relations[0].to_entity == "order"
    assert entity.relations[0].relation_type == "one_to_many"

    # Convert back to dict
    entity_dict_again = entity.to_dict()

    # Assert primary key flag is correctly set in the output dict
    id_field_dict = next(f for f in entity_dict_again["fields"] if f["name"] == "user_id")
    assert id_field_dict["is_primary_key"] is True
    other_field_dict = next(f for f in entity_dict_again["fields"] if f["name"] == "username")
    assert other_field_dict["is_primary_key"] is False

    # Create a new entity from the generated dict
    new_entity = Entity.from_dict(entity_dict_again)
    assert new_entity.version == entity.version
    assert len(new_entity.fields) == len(entity.fields)
    new_id_field = next(f for f in new_entity.fields if f.name == "user_id")
    assert new_id_field.is_primary_key is True


def test_entity_yaml_serialization_with_version_and_fields():
    """Test serializing/deserializing entity with version and fields to/from YAML."""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / "customer_advanced.yaml"
        
        # Define field definitions for the entity
        field_definitions = [
            FieldDefinition(name="id", type="string", required=True, description="Primary key for customer"),
            FieldDefinition(name="full_name", type="string", required=True),
            FieldDefinition(name="loyalty_points", type="integer", required=False, description="Customer loyalty points")
        ]
        
        # Create entity with version and fields
        entity = Entity(
            name="customer_adv",
            version="2.1.0",
            primary_key="id",
            fields=field_definitions,
            description="Test customer entity with version and fields",
            relations=[Relation("purchase", "one_to_many", "customer_id")]
        )
        entity.to_yaml(temp_path)
        
        # Load and verify
        loaded_entity = Entity.from_yaml(temp_path)
        assert loaded_entity.name == "customer_adv"
        assert loaded_entity.version == "2.1.0"
        assert loaded_entity.primary_key == "id"
        assert loaded_entity.description == "Test customer entity with version and fields"
        
        # Verify fields
        assert len(loaded_entity.fields) == 3
        
        loaded_id_field = next(f for f in loaded_entity.fields if f.name == "id")
        loaded_name_field = next(f for f in loaded_entity.fields if f.name == "full_name")
        loaded_points_field = next(f for f in loaded_entity.fields if f.name == "loyalty_points")

        assert loaded_id_field.type == "string"
        assert loaded_id_field.required is True
        assert loaded_id_field.description == "Primary key for customer"
        assert loaded_id_field.is_primary_key is True # Set by from_dict called within from_yaml

        assert loaded_name_field.type == "string"
        assert loaded_name_field.is_primary_key is False

        assert loaded_points_field.type == "integer"
        assert loaded_points_field.description == "Customer loyalty points"
        assert loaded_points_field.is_primary_key is False
        
        # Verify relations
        assert len(loaded_entity.relations) == 1
        assert loaded_entity.relations[0].to_entity == "purchase"

def test_relation_equality():
    """Test relation equality comparison."""
    rel1 = Relation("transaction", "one_to_many", "customer_id")
    rel2 = Relation("transaction", "one_to_many", "customer_id")
    rel3 = Relation("order", "one_to_many", "customer_id")
    
    assert rel1 == rel2 # Relation is a dataclass, so equality is value-based
    assert rel1 != rel3


def test_entity_str_representation():
    """Test string representation of entity."""
    # Entity creation requires version and primary_key
    entity = Entity(name="customer", version="1.0", primary_key="id", fields=[FieldDefinition("id","string")])
    assert str(entity) == "Entity(name='customer', primary_key='id')"


def test_entity_get_relation():
    """Test the get_relation method of the Entity class."""
    relation1 = Relation(to_entity="transaction", relation_type="1:many", foreign_key="customer_id")
    relation2 = Relation(to_entity="address", relation_type="1:1", foreign_key="customer_id")
    
    # Define fields for the entity, including the primary key
    fields = [
        FieldDefinition(name="id", type="string", required=True, description="Primary key"),
        FieldDefinition(name="name", type="string")
    ]

    entity = Entity(
        name="customer",
        version="1.0",
        primary_key="id",
        fields=fields,
        relations=[relation1, relation2]
    )

    # Test getting existing relations
    assert entity.get_relation("transaction") == relation1
    assert entity.get_relation("address") == relation2

    # Test getting a non-existent relation
    assert entity.get_relation("non_existent_entity") is None

    # Test with an entity with no relations
    entity_no_relations = Entity(
        name="product", 
        version="1.0", 
        primary_key="sku",
        fields=[FieldDefinition(name="sku", type="string")]
    )
    assert entity_no_relations.get_relation("any_target") is None
