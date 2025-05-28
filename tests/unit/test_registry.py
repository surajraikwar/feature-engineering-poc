"""Tests for the registry module."""

import pytest
from pathlib import Path
import tempfile
import yaml

from domain.core.entity import Entity, Relation
from domain.core.registry import EntityRegistry


def test_add_and_get_entity():
    """Test adding and retrieving entities from the registry."""
    registry = EntityRegistry()
    entity = Entity("customer", "id")
    
    # Add entity
    registry.add_entity(entity)
    
    # Retrieve entity
    retrieved = registry.get_entity("customer")
    assert retrieved is not None
    assert retrieved.name == "customer"
    assert retrieved.primary_key == "id"
    
    # Test non-existent entity
    assert registry.get_entity("nonexistent") is None


def test_duplicate_entity():
    """Test that adding a duplicate entity raises an error."""
    registry = EntityRegistry()
    entity = Entity("customer", "id")
    
    # First add should succeed
    registry.add_entity(entity)
    
    # Second add should fail
    with pytest.raises(ValueError, match="already exists in registry"):
        registry.add_entity(entity)


def test_validate_relationships(sample_registry):
    """Test relationship validation in the registry."""
    # Should be valid - all relations point to existing entities
    errors = sample_registry.validate_relationships()
    assert not errors


def test_circular_dependency_detection():
    """Test detection of circular dependencies between entities."""
    registry = EntityRegistry()
    
    # Create entities with circular dependency
    customer = Entity("customer", "id")
    order = Entity("order", "id")
    
    # Add relations
    customer.relations = [
        Relation("order", "one_to_many", "customer_id")
    ]
    order.relations = [
        Relation("customer", "many_to_one", "customer_id")
    ]
    
    # Add to registry
    registry.add_entity(customer)
    registry.add_entity(order)
    
    # Should detect circular dependency
    errors = registry.validate_relationships()
    assert len(errors) == 1
    assert "Circular dependency" in errors[0]


def test_yaml_serialization():
    """Test saving and loading registry to/from YAML files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create a registry with test data
        registry = EntityRegistry()
        registry.add_entity(Entity("customer", "id", description="Customer data"))
        registry.add_entity(Entity("order", "id", description="Order data"))
        
        # Save to YAML
        registry.to_yaml_dir(temp_path)
        
        # Should create two YAML files
        yaml_files = list(temp_path.glob("*.yaml"))
        assert len(yaml_files) == 2
        
        # Load back and verify
        loaded_registry = EntityRegistry.from_yaml_dir(temp_path)
        assert len(loaded_registry) == 2
        assert loaded_registry.get_entity("customer").description == "Customer data"
        assert loaded_registry.get_entity("order").description == "Order data"


def test_registry_contains():
    """Test the __contains__ method of the registry."""
    registry = EntityRegistry()
    registry.add_entity(Entity("customer", "id"))
    
    assert "customer" in registry
    assert "nonexistent" not in registry


def test_registry_length():
    """Test the __len__ method of the registry."""
    registry = EntityRegistry()
    assert len(registry) == 0
    
    registry.add_entity(Entity("customer", "id"))
    assert len(registry) == 1
    
    registry.add_entity(Entity("order", "id"))
    assert len(registry) == 2
