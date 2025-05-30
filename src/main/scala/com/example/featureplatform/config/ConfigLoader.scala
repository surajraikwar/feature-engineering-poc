package com.example.featureplatform.config

import java.io.File
import scala.io.Source
import scala.util.{Either, Left, Right, Try}
import io.circe.yaml.parser
import io.circe.syntax._ // Required for the .as[T] syntax on Json objects

/**
 * Utility object for loading job configurations from YAML files.
 */
object JobConfigLoader {

  /**
   * Loads a [[JobConfig]] from the specified YAML file path.
   * @param filePath Path to the YAML job configuration file.
   * @return Either a Throwable on error, or the successfully parsed JobConfig.
   */
  def loadJobConfig(filePath: String): Either[Throwable, JobConfig] = {
    Try {
      val yamlString = Source.fromFile(filePath).mkString
      parser.parse(yamlString) match {
        case Left(parsingError) => Left(parsingError)
        case Right(json) =>
          json.as[JobConfig] match {
            case Left(decodingError) => Left(decodingError)
            case Right(jobConfig) => Right(jobConfig)
          }
      }
    }.toEither.flatMap(Predef.identity) // Flattens Try[Either[Throwable, JobConfig]] to Either[Throwable, JobConfig]
  }
}

/**
 * Holds a collection of [[SourceDefinition]]s, keyed by (name, version).
 * @param sources A map containing source definitions.
 */
class SourceRegistry(private val sources: Map[(String, String), SourceDefinition]) {
  /** Retrieves a specific source definition by name and version. */
  def getSourceDefinition(name: String, version: String): Option[SourceDefinition] = sources.get((name, version))

  /**
   * Retrieves a source definition by name. If multiple versions exist for the same name,
   * this basic implementation returns the first one encountered.
   */
  def getSourceDefinition(name: String): Option[SourceDefinition] =
    sources.collectFirst { case ((n, _), sd) if n == name => sd }

  /** Returns all loaded source definitions as a list. */
  def getAllSourceDefinitions(): List[SourceDefinition] = sources.values.toList
}

/**
 * Companion object for [[SourceRegistry]], providing methods to load definitions from a directory.
 */
object SourceRegistry {
  /**
   * Loads all source definitions from `*.yaml` or `*.yml` files within a given directory.
   * @param directoryPath Path to the directory containing source definition files.
   * @return Either a Throwable on error, or a SourceRegistry instance populated with the definitions.
   */
  def loadFromDirectory(directoryPath: String): Either[Throwable, SourceRegistry] = {
    Try {
      val dir = new File(directoryPath)
      if (dir.exists && dir.isDirectory) {
        // Using java.io.File#listFiles for potentially simpler directory listing
        val yamlFileObjects = Option(dir.listFiles()) 
          .map(_.toList)
          .getOrElse(List.empty)
          .filter(file => file.isFile && (file.getName.endsWith(".yaml") || file.getName.endsWith(".yml")))
        
        val yamlFilePaths = yamlFileObjects.map(_.toPath) // Convert to Path for consistency if needed later, or use File directly

        val parsedDefinitions = yamlFilePaths.map { filePath => // Changed from yamlFiles to yamlFilePaths
          Try {
            val yamlString = Source.fromFile(filePath.toFile).mkString
            parser.parse(yamlString) match {
              case Left(parsingError) => Left(parsingError)
              case Right(json) => json.as[SourceDefinition]
            }
            }.toEither.flatMap(Predef.identity) // Similar flattening
        }

        // Collect all successful parses, or return the first error
        val successfulDefinitions = parsedDefinitions.collect { case Right(sd) => sd }
        val firstError = parsedDefinitions.collectFirst { case Left(err) => err }

        firstError match {
          case Some(err) => Left(err)
          case None =>
            val sourceMap = successfulDefinitions.map(sd => (sd.name, sd.version) -> sd).toMap
            Right(new SourceRegistry(sourceMap))
        }
      } else {
        Left(new java.io.FileNotFoundException(s"Directory not found or not a directory: $directoryPath"))
      }
    }.toEither.flatMap(Predef.identity)
  }
}
