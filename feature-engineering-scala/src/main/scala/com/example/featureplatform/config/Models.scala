package com.example.featureplatform.config

import io.circe.{Decoder, Encoder, Json} // Added Json import
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}

// --- Core Source Definition Models (from domain/core/source_definition.py) ---

/** Defines a field within a data source. */
case class FieldDefinition(
  name: String,
  `type`: String, // Escaped because 'type' is a keyword in Scala
  description: Option[String] = None,
  required: Option[Boolean] = Some(false), // Pydantic default is False
  default: Option[String] = None
)
object FieldDefinition {
  implicit val decoder: Decoder[FieldDefinition] = deriveDecoder[FieldDefinition]
  implicit val encoder: Encoder[FieldDefinition] = deriveEncoder[FieldDefinition]
}

/** Defines a data quality check to be performed on a source. */
case class QualityCheckDefinition(
  `type`: String,
  field: Option[String] = None,
  condition: Option[String] = None
)
object QualityCheckDefinition {
  implicit val decoder: Decoder[QualityCheckDefinition] = deriveDecoder[QualityCheckDefinition]
  implicit val encoder: Encoder[QualityCheckDefinition] = deriveEncoder[QualityCheckDefinition]
}

/** Defines metadata for configurations like creation/update timestamps and tags. */
case class MetadataDefinition(
  created_at: Option[String] = None,
  created_by: Option[String] = None,
  updated_at: Option[String] = None,
  updated_by: Option[String] = None,
  tags: Option[List[String]] = None
)
object MetadataDefinition {
  implicit val decoder: Decoder[MetadataDefinition] = deriveDecoder[MetadataDefinition]
  implicit val encoder: Encoder[MetadataDefinition] = deriveEncoder[MetadataDefinition]
}

/** Configuration specific to Databricks sources (table, query). */
case class DatabricksSourceDetailConfig(
  catalog: Option[String] = None,
  schema: Option[String] = None, // Corresponds to schema_name in Pydantic, aliased as "schema"
  table: Option[String] = None,
  query: Option[String] = None,
  incremental: Option[Boolean] = Some(false) // Pydantic default is False
)
object DatabricksSourceDetailConfig {
  implicit val decoder: Decoder[DatabricksSourceDetailConfig] = deriveDecoder[DatabricksSourceDetailConfig]
  implicit val encoder: Encoder[DatabricksSourceDetailConfig] = deriveEncoder[DatabricksSourceDetailConfig]
}

// SourceTypeSpecificConfig is a Union in Python.
// For now, we directly use DatabricksSourceDetailConfig as it's the only member.

/**
 * Defines a data source, including its type, location, schema, and specific configuration.
 */
case class SourceDefinition(
  name: String,
  description: Option[String] = None,
  version: String,
  `type`: String,
  entity: String,
  location: Option[String] = None,
  fields: Option[List[FieldDefinition]] = None,
  config: DatabricksSourceDetailConfig, // Directly using DatabricksSourceDetailConfig
  quality_checks: Option[List[QualityCheckDefinition]] = None,
  metadata: Option[MetadataDefinition] = None
)
object SourceDefinition {
  implicit val decoder: Decoder[SourceDefinition] = deriveDecoder[SourceDefinition]
  implicit val encoder: Encoder[SourceDefinition] = deriveEncoder[SourceDefinition]
}

// --- Job Configuration Models (from domain/jobs/config_loader.py) ---

// BaseConfigModel with extra = Extra.forbid is generally the default behavior for case classes
// when using libraries like Circe for decoding (i.e., unknown fields cause errors).
// No direct Scala equivalent for BaseConfigModel itself is needed unless it has fields/methods.

/** Configuration for a job's input source. */
case class JobInputSourceConfig(
  name: String,
  version: Option[String] = None,
  load_params: Option[Map[String, Json]] = None // Changed Any to Json
)
object JobInputSourceConfig {
  implicit val decoder: Decoder[JobInputSourceConfig] = deriveDecoder[JobInputSourceConfig]
  implicit val encoder: Encoder[JobInputSourceConfig] = deriveEncoder[JobInputSourceConfig] // For completeness
}

// FeatureTransformerParams had extra = Extra.allow.
// We'll represent params directly as Map[String, Any] in FeatureTransformerConfig.
// If FeatureTransformerParams had its own well-defined fields, it would be a case class.
// case class FeatureTransformerParams(params: Map[String, Any] = Map.empty)

/** Configuration for a single feature transformer. */
case class FeatureTransformerConfig(
  name: String,
  params: Map[String, Json] // Changed Any to Json
)
object FeatureTransformerConfig {
  implicit val decoder: Decoder[FeatureTransformerConfig] = deriveDecoder[FeatureTransformerConfig]
  implicit val encoder: Encoder[FeatureTransformerConfig] = deriveEncoder[FeatureTransformerConfig]
}

// OutputSinkParams had extra = Extra.allow but also specific fields.
/** Parameters for an output sink. */
case class OutputSinkParams(
  num_rows: Option[Int] = Some(20),
  truncate: Option[Boolean] = Some(false), // Note: Pydantic default was False
  path: Option[String] = None,
  mode: Option[String] = Some("overwrite"),
  partition_by: Option[List[String]] = None,
  options: Option[Map[String, Json]] = None // Changed Any to Json
)
object OutputSinkParams {
  implicit val decoder: Decoder[OutputSinkParams] = deriveDecoder[OutputSinkParams]
  implicit val encoder: Encoder[OutputSinkParams] = deriveEncoder[OutputSinkParams]
}

/** Configuration for the output sink of a job. */
case class OutputSinkConfig(
  sink_type: String = "display",
  config: OutputSinkParams = OutputSinkParams() // Default factory behavior
)
object OutputSinkConfig {
  implicit val decoder: Decoder[OutputSinkConfig] = deriveDecoder[OutputSinkConfig]
  implicit val encoder: Encoder[OutputSinkConfig] = deriveEncoder[OutputSinkConfig]
}

/**
 * Defines the overall configuration for a feature engineering job.
 */
case class JobConfig(
  job_name: Option[String] = Some("Untitled Job"),
  description: Option[String] = Some(""),
  input_source: JobInputSourceConfig,
  feature_transformers: List[FeatureTransformerConfig] = List.empty, // Default factory behavior
  output_sink: OutputSinkConfig = OutputSinkConfig() // Default factory behavior
)
object JobConfig {
  implicit val decoder: Decoder[JobConfig] = deriveDecoder[JobConfig]
  implicit val encoder: Encoder[JobConfig] = deriveEncoder[JobConfig]
}
