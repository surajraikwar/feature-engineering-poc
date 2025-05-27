import os
import pytest
from pathlib import Path

from domain.registry.Registry import Registry, load_registry
from domain.Entity import Entity, Relation


def test_registry_initialization():
    """Test that registry initializes correctly."""
    registry = Registry()
    assert len(registry.entities) == 0


def test_add_entity():
    """Test adding entities to the registry."""
    registry = Registry()
    entity = Entity(name="test", primary_key="id", is_leaf=True)
    registry.add_entity(entity)
    
    assert "test" in registry.entities
    assert registry.get_entity("test") == entity


def test_duplicate_entity():
    """Test that adding a duplicate entity raises an error."""
    registry = Registry()
    entity = Entity(name="test", primary_key="id", is_leaf=True)
    registry.add_entity(entity)
    
    with pytest.raises(ValueError):
        registry.add_entity(entity)


def test_validate_relationships():
    """Test relationship validation."""
    registry = Registry()
    
    # Create entities with relationships
    customer = Entity(
        name="customer",
        primary_key="user_id",
        is_leaf=False,
        relations=[{"to": "transaction", "type": "1:many"}]
    )
    
    transaction = Entity(
        name="transaction",
        primary_key="transaction_id",
        is_leaf=True,
        relations=[]
    )
    
    registry.add_entity(customer)
    registry.add_entity(transaction)
    
    # Should not raise any errors
    errors = registry.validate_relationships()
    assert not errors


def test_circular_dependency():
    """Test detection of circular dependencies."""
    registry = Registry()
    
    # Create entities with circular relationship
    parent = Entity(
        name="parent",
        primary_key="id",
        is_leaf=False,
        relations=[{"to": "child", "type": "1:many"}]
    )
    
    child = Entity(
        name="child",
        primary_key="id",
        is_leaf=False,
        relations=[{"to": "parent", "type": "many:1"}]
    )
    
    registry.add_entity(parent)
    registry.add_entity(child)
    
    # Should detect circular dependency
    errors = registry.validate_relationships()
    assert any("Circular dependency" in error for error in errors)


def test_load_entities_from_dir(tmp_path):
    """Test loading entities from a directory."""
    # Create a temporary YAML file
    entity_yaml = """
    - name: test_entity
      primary_key: id
      is_leaf: true
    """
    
    # Create a temporary directory and write the YAML file
    test_dir = tmp_path / "entities"
    test_dir.mkdir()
    entity_file = test_dir / "test_entity.yaml"
    entity_file.write_text(entity_yaml)
    
    # Load the registry
    registry = Registry()
    registry.load_entities_from_dir(str(test_dir))
    
    # Verify the entity was loaded
    assert "test_entity" in registry.entities
    assert registry.get_entity("test_entity").primary_key == "id"


def test_get_downstream_entities():
    """Test getting downstream dependencies."""
    registry = Registry()
    
    # Create entities with relationships: a -> b -> c
    a = Entity("a", "id", False, [{"to": "b", "type": "1:many"}])
    b = Entity("b", "id", False, [{"to": "c", "type": "1:many"}])
    c = Entity("c", "id", True, [])
    
    for entity in [a, b, c]:
        registry.add_entity(entity)
    
    # Validate relationships to build the graph
    registry.validate_relationships()
    
    # Test direct downstream dependencies
    assert registry.get_downstream_entities("c", direct_only=True) == {"b"}
    assert registry.get_downstream_entities("b", direct_only=True) == {"a"}
    assert registry.get_downstream_entities("a", direct_only=True) == set()
    
    # Test all downstream dependencies (direct and indirect)
    assert registry.get_downstream_entities("c") == {"a", "b"}
    assert registry.get_downstream_entities("b") == {"a"}
    assert registry.get_downstream_entities("a") == set()
    
    # Test with non-existent entity
    assert registry.get_downstream_entities("nonexistent") == set()
