"""
The domain.core module contains fundamental components such as
configuration management, entity and relation definitions, source and entity
registries, and Spark session management utilities.
"""
from .entity import Entity, Relation
from .registry import EntityRegistry

__all__ = ["Entity", "Relation", "EntityRegistry"]
