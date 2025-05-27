from abc import abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict

from feature_platform.sources.base import Source, SourceConfig
from feature_platform.core.spark import SparkSessionManager # Ensure this import works

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame # For type hinting

@dataclass
class SparkSourceConfig(SourceConfig):
    """Base configuration for a Spark-based data source."""
    # Add any common Spark-specific config options here if needed in the future
    # For now, it's structurally similar to SourceConfig
    format: str = "delta" # Default to delta, can be overridden (e.g. "parquet", "csv")
    options: Dict[str, Any] = field(default_factory=dict) # Spark read options

class SparkSource(Source["SparkDataFrame"]):
    """
    Abstract base class for Spark-based data sources.
    These sources are expected to return Spark DataFrames.
    """

    def __init__(self, config: SparkSourceConfig, spark_manager: SparkSessionManager):
        super().__init__(config) # config here is SparkSourceConfig
        self.spark_manager = spark_manager
        # Ensure config is of type SparkSourceConfig for type checkers if needed
        if not isinstance(config, SparkSourceConfig):
            raise ValueError("config must be an instance of SparkSourceConfig")
        self.config: SparkSourceConfig = config # Narrowing type for self.config


    @abstractmethod
    def read(self, *args, **kwargs) -> "SparkDataFrame":
        """
        Read data from the source as a Spark DataFrame.
        """
        pass

    # initialize() and close() from base Source class can be used as is,
    # or overridden if Spark-specific initialization/cleanup is needed beyond session management.
    # validate() also needs to be implemented by concrete classes.
