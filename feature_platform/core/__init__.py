"""Core functionality for the feature platform."""

from .entity import Entity, Relation
from .registry import EntityRegistry

__all__ = ["Entity", "Relation", "EntityRegistry"]
