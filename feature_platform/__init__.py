"""Feature Platform - A platform for managing and serving machine learning features."""

__version__ = "0.1.0"

from .core.entity import Entity, Relation
from .core.registry import EntityRegistry
from .sources.base import Source
from .sources.databricks import DatabricksSource
