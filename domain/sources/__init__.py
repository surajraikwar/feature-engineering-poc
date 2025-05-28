"""
The domain.sources module offers abstractions and concrete
implementations for various data sources, such as Databricks Spark tables,
Databricks SQL endpoints, and other potential data backends.
"""
from .base import Source, SourceConfig
from .spark_base import SparkSource, SparkSourceConfig
from .databricks_spark import DatabricksSparkSource, DatabricksSparkSourceConfig
from .databricks_sql import DatabricksSQLSource, DeltaSQLSourceConfig # Updated line

__all__ = [
    "Source", 
    "SourceConfig", 
    "SparkSource", 
    "SparkSourceConfig",
    "DatabricksSparkSource", 
    "DatabricksSparkSourceConfig",
    "DatabricksSQLSource",
    "DeltaSQLSourceConfig"
]
