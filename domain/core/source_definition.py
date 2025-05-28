"""
Defines Pydantic models for parsing and validating data source configurations
from YAML files in the source catalog. These models specify the expected
structure for source metadata, connection details, field definitions,
quality checks, etc.
"""
from typing import List, Optional, Union

from pydantic import BaseModel, Field

class FieldDefinition(BaseModel):
    name: str
    type: str
    description: Optional[str] = None
    required: Optional[bool] = False
    default: Optional[str] = None

class QualityCheckDefinition(BaseModel):
    type: str
    field: Optional[str] = None
    condition: Optional[str] = None

class MetadataDefinition(BaseModel):
    created_at: Optional[str] = None
    created_by: Optional[str] = None
    updated_at: Optional[str] = None
    updated_by: Optional[str] = None
    tags: Optional[List[str]] = None

class DatabricksSourceDetailConfig(BaseModel):
    catalog: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema") # "schema" is a BaseModel attribute
    table: Optional[str] = None
    query: Optional[str] = None
    incremental: Optional[bool] = False

# In the future, we will add more source types here
SourceTypeSpecificConfig = Union[DatabricksSourceDetailConfig]

class SourceDefinition(BaseModel):
    name: str
    description: Optional[str] = None
    version: str
    type: str 
    entity: str
    location: Optional[str] = None # For sources like databricks, location is not needed
    fields: Optional[List[FieldDefinition]] = None
    config: SourceTypeSpecificConfig
    quality_checks: Optional[List[QualityCheckDefinition]] = None
    metadata: Optional[MetadataDefinition] = None
