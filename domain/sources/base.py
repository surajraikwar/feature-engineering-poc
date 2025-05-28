"""Base classes for data sources."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Generic, List, Optional, TypeVar, Type
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T')

class SourceType(str, Enum):
    """Supported source types."""
    DELTA = "delta"
    SNAPSHOT = "snapshot"
    STREAM = "stream"


@dataclass
class SourceConfig:
    """Base configuration for a data source."""
    name: str
    entity: str
    type: SourceType
    location: str
    fields: List[str] = field(default_factory=list)
    description: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SourceConfig':
        """Create a SourceConfig from a dictionary."""
        return cls(**data)


class Source(ABC, Generic[T]):
    """
    Abstract base class for all data sources.
    
    A source represents a connection to a data store that can provide
    data for one or more entities in the feature platform.
    """
    
    def __init__(self, config: SourceConfig):
        """
        Initialize the source with its configuration.
        
        Args:
            config: Configuration for this source
        """
        self.config = config
        self._initialized = False
    
    @abstractmethod
    def read(self, *args, **kwargs) -> T:
        """
        Read data from the source.
        
        Returns:
            The data read from the source
        """
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Validate the source configuration and connectivity.
        
        Returns:
            True if the source is valid, False otherwise
        """
        pass
    
    def initialize(self) -> None:
        """Initialize the source if not already initialized."""
        if not self._initialized:
            if not self.validate():
                raise ValueError(f"Source validation failed for {self.config.name}")
            self._initialized = True
    
    def close(self) -> None:
        """
        Close the source and release any resources.
        
        This method should be idempotent.
        """
        self._initialized = False
    
    def __enter__(self):
        """Context manager entry."""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.config.name}')"
