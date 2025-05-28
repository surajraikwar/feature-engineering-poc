"""
The domain package provides core infrastructure for defining,
managing, and executing feature engineering pipelines.
"""
__version__ = "0.1.0"

from .core.entity import Entity, Relation
from .core.registry import EntityRegistry
from .sources.base import Source
# Note: The 'from .sources.databricks import DatabricksSource' was present in the original
# content provided by read_files. However, 'domain/sources/databricks.py'
# was deleted in a previous subtask (Subtask 6).
# I will omit this line to prevent an ImportError if this file is actually executed.
# If this import is essential and points to a different, existing 'databricks.py',
# then this decision should be revisited. Assuming it referred to the deleted file.
# from .sources.databricks import DatabricksSource
