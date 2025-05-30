package com.example.featureplatform.sources

import com.example.featureplatform.config.{DatabricksSourceDetailConfig, SourceDefinition}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
 * A [[DataSourceReader]] for reading data from Databricks sources like tables, queries, or file paths (e.g., Delta Lake).
 * It uses the configuration provided in [[DatabricksSourceDetailConfig]] within the [[SourceDefinition]].
 */
class DatabricksSparkSource extends DataSourceReader {

  /**
   * Reads data from a Databricks source.
   * The method determines whether to read from a table, a SQL query, or a file location
   * based on the populated fields in `sourceDefinition.config` and `sourceDefinition.location`.
   * It also performs basic schema validation if fields are defined in the `sourceDefinition`.
   *
   * @param spark The active SparkSession.
   * @param sourceDefinition The definition of the Databricks source.
   * @return Either a Throwable on error (e.g., configuration issue, read error, schema validation failure),
   *         or a DataFrame containing the loaded data.
   */
  override def read(spark: SparkSession, sourceDefinition: SourceDefinition): Either[Throwable, DataFrame] = {
    Try {
      val databricksConfig: DatabricksSourceDetailConfig = sourceDefinition.config

      val df: DataFrame = if (databricksConfig.query.isDefined && databricksConfig.query.get.trim.nonEmpty) {
        spark.sql(databricksConfig.query.get)
      } else if (databricksConfig.table.isDefined && databricksConfig.table.get.trim.nonEmpty) {
        // Assumes table name might be fully qualified (catalog.schema.table) or schema.table, or just table
        // Defaults for catalog/schema can be tricky if not specified and relying on session defaults.
        // For robust behavior, expecting well-defined table names or explicit catalog/schema in config.
        val tableName = databricksConfig.table.get
        val fullTableName = (
          databricksConfig.catalog.filter(_.trim.nonEmpty), 
          databricksConfig.schema.filter(_.trim.nonEmpty)
        ) match {
          case (Some(cat), Some(sch)) if !tableName.contains('.') => s"$cat.$sch.$tableName"
          case (None, Some(sch)) if !tableName.contains('.')      => s"$sch.$tableName"
          // If tableName already contains '.', it's assumed to be schema.table or catalog.schema.table.
          // If catalog or schema are not provided, or if tableName is already qualified,
          // Spark will use the current session's default database or the qualified name.
          case _ => tableName 
        }
        spark.table(fullTableName)
      } else if (sourceDefinition.location.isDefined && sourceDefinition.location.get.trim.nonEmpty) {
        // Assuming location implies a path-based source, typically Delta for this project context
        // The 'type' field in SourceDefinition could be used to confirm format e.g. "delta", "parquet", etc.
        // For now, defaulting to delta if location is used without table/query.
        val format = sourceDefinition.`type` match {
          case "delta" => "delta"
          case "parquet" => "parquet"
          case "csv" => "csv" // Example, add more as needed
          case "json" => "json" // Example
          case _ if sourceDefinition.location.get.endsWith(".delta") => "delta" // Heuristic
          case _ => "delta" // Default to delta for path-based if type is generic like "databricks_path"
        }
        spark.read.format(format).load(sourceDefinition.location.get)
      } else {
        throw new IllegalArgumentException("Databricks source configuration is incomplete. Either 'query', 'table', or 'location' must be specified.")
      }

      // Schema Validation
      sourceDefinition.fields match {
        case Some(definedFields) if definedFields.nonEmpty =>
          val dfColumns = df.columns.map(_.toLowerCase).toSet
          val definedFieldNames = definedFields.map(_.name.toLowerCase).toSet
          
          // Check for missing fields that are defined in the SourceDefinition
          val missingFields = definedFieldNames -- dfColumns
          if (missingFields.nonEmpty) {
            // Optional: Check for required fields only
            // val requiredFieldsFromDefinition = definedFields.filter(_.required.getOrElse(false)).map(_.name.toLowerCase).toSet
            // val missingRequiredFields = requiredFieldsFromDefinition -- dfColumns
            // if (missingRequiredFields.nonEmpty) {
            //   throw new IllegalArgumentException(s"Schema validation failed: Missing required field(s): ${missingRequiredFields.mkString(", ")}")
            // }
            // For now, considers all defined fields as implicitly required for this basic check
            throw new IllegalArgumentException(s"Schema validation failed: DataFrame is missing field(s) defined in SourceDefinition: ${missingFields.mkString(", ")}")
          }
          
          // Optional: Check for extra fields in DataFrame not in SourceDefinition (strict validation)
          // val extraFields = dfColumns -- definedFieldNames
          // if (extraFields.nonEmpty) {
          //   throw new IllegalArgumentException(s"Schema validation failed: DataFrame has extra field(s) not in SourceDefinition: ${extraFields.mkString(", ")}")
          // }

          // Optional: Type validation (more complex)
          // definedFields.foreach { fieldDef =>
          //   val dfSchemaField = df.schema(df.schema.fieldIndex(fieldDef.name)) // Case sensitive lookup
          //   if (dfSchemaField.dataType.simpleString != fieldDef.`type`) { // This is a naive comparison
          //     throw new IllegalArgumentException(s"Schema validation failed for field '${fieldDef.name}': Expected type ${fieldDef.`type`}, found ${dfSchemaField.dataType.simpleString}")
          //   }
          // }
          df // Return df if basic validation (presence of defined fields) passes
        case _ =>
          df // No fields defined in SourceDefinition, so no schema validation to perform
      }
    }.toEither
  }
}
