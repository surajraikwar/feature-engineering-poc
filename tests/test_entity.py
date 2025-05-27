import pytest
from pathlib import Path
import tempfile
import yaml

from domain.Entity import Entity, Relation, load_entities_from_dir


def test_entity_creation():
    """Test basic entity creation."""
    entity = Entity(name="test", primary_key="id", is_leaf=True)
    assert entity.name == "test"
    assert entity.primary_key == "id"
    assert entity.is_leaf is True
    assert len(entity.relations) == 0


def test_entity_with_relations():
    """Test entity with relationships."""
    entity = Entity(
        name="customer",
        primary_key="user_id",
        is_leaf=False,
        relations=[{"to": "transaction", "type": "1:many"}]
    )
    assert len(entity.relations) == 1
    assert entity.relations[0].to_entity == "transaction"
    assert entity.relations[0].relation_type == "1:many"


def test_entity_equality():
    """Test entity equality comparison."""
    entity1 = Entity("test", "id", True)
    entity2 = Entity("test", "id", True)
    entity3 = Entity("different", "id", True)
    
    assert entity1 == entity2
    assert entity1 != entity3
    assert entity1 != "not_an_entity"


def test_relation_equality():
    """Test relation equality comparison."""
    rel1 = Relation("target", "1:many")
    rel2 = Relation("target", "1:many")
    rel3 = Relation("different", "1:1")
    
    assert rel1 == rel2
    assert rel1 != rel3
    assert rel1 != "not_a_relation"


def test_entity_describe():
    """Test entity description."""
    entity = Entity(
        name="test",
        primary_key="id",
        is_leaf=True,
        relations=[{"to": "other", "type": "1:1"}]
    )
    description = entity.describe()
    
    assert description["Entity"] == "test"
    assert description["Primary Key"] == "id"
    assert description["Is Leaf"] is True
    assert "1:1 → other" in description["Relations"]


def test_load_entities_from_dir(tmp_path):
    """Test loading entities from a directory."""
    # Create a temporary YAML file
    entity_yaml = """
    - name: test_entity
      primary_key: id
      is_leaf: true
      relations:
        - to: other_entity
          type: 1:many
    """
    
    # Create a temporary directory and write the YAML file
    test_dir = tmp_path / "entities"
    test_dir.mkdir()
    entity_file = test_dir / "test_entity.yaml"
    entity_file.write_text(entity_yaml)
    
    # Load entities
    entities = load_entities_from_dir(str(test_dir))
    
    # Verify the entity was loaded correctly
    assert len(entities) == 1
    assert entities[0].name == "test_entity"
    assert entities[0].primary_key == "id"
    assert entities[0].is_leaf is True
    assert len(entities[0].relations) == 1
    assert entities[0].relations[0].to_entity == "other_entity"
    assert entities[0].relations[0].relation_type == "1:many"


def test_load_entities_from_nonexistent_dir():
    """Test loading entities from a non-existent directory."""
    with pytest.raises(ValueError, match="Directory not found"):
        load_entities_from_dir("/nonexistent/directory")


def test_load_entities_from_file(tmp_path):
    """Test loading a single entity from a file."""
    # Create a temporary YAML file with a single entity (not a list)
    entity_yaml = """
    name: single_entity
    primary_key: id
    is_leaf: true
    """
    
    test_dir = tmp_path / "entities"
    test_dir.mkdir()
    entity_file = test_dir / "single_entity.yaml"
    entity_file.write_text(entity_yaml)
    
    # Load entities
    entities = load_entities_from_dir(str(test_dir))
    
    # Verify the entity was loaded correctly
    assert len(entities) == 1
    assert entities[0].name == "single_entity"
    assert entities[0].primary_key == "id"
    assert entities[0].is_leaf is True
    assert len(entities[0].relations) == 0
