package com.example.featureplatform.config

import java.io.File
import scala.io.Source
import scala.util.{Either, Left, Right, Try}
import io.circe.parser // JSON parser
import io.circe.syntax._ // Required for the .as[T] syntax on Json objects

/**
 * Utility object for loading job configurations from JSON files.
 */
object JobConfigLoader {

  /**
   * Loads a [[JobConfig]] from the specified JSON file path.
   * @param filePath Path to the JSON job configuration file.
   * @return Either a Throwable on error, or the successfully parsed JobConfig.
   */
  def loadJobConfig(filePath: String): Either[Throwable, JobConfig] = {
    Try {
      val jsonString = Source.fromFile(filePath).mkString
      parser.parse(jsonString) match { // Uses JSON parser
        case Left(parsingError) => Left(parsingError)
        case Right(json) =>
          json.as[JobConfig] match { // Decodes from JSON model
            case Left(decodingError) => Left(decodingError)
            case Right(jobConfig) => Right(jobConfig)
          }
      }
    }.toEither.flatMap(Predef.identity)
  }
}

/**
 * Holds a collection of [[SourceDefinition]]s, keyed by (name, version).
 * @param sources A map containing source definitions.
 */
class SourceRegistry(private val sources: Map[(String, String), SourceDefinition]) {
  def getSourceDefinition(name: String, version: String): Option[SourceDefinition] = sources.get((name, version))
  def getSourceDefinition(name: String): Option[SourceDefinition] =
    sources.collectFirst { case ((n, _), sd) if n == name => sd }
  def getAllSourceDefinitions(): List[SourceDefinition] = sources.values.toList
}

/**
 * Companion object for [[SourceRegistry]], providing methods to load definitions from a directory.
 */
object SourceRegistry {
  /**
   * Loads all source definitions from `*.json` files within a given directory.
   * @param directoryPath Path to the directory containing source definition files.
   * @return Either a Throwable on error, or a SourceRegistry instance populated with the definitions.
   */
  def loadFromDirectory(directoryPath: String): Either[Throwable, SourceRegistry] = {
    Try {
      val dir = new File(directoryPath)
      if (dir.exists && dir.isDirectory) {
        val jsonFileObjects = Option(dir.listFiles())
          .map(_.toList)
          .getOrElse(List.empty)
          .filter(file => file.isFile && file.getName.endsWith(".json")) // Filters for .json

        val jsonFilePaths = jsonFileObjects.map(_.toPath)

        val parsedDefinitions = jsonFilePaths.map { filePath =>
          Try {
            val jsonString = Source.fromFile(filePath.toFile).mkString
            parser.parse(jsonString) match { // Uses JSON parser
              case Left(parsingError) => Left(parsingError)
              case Right(json) => json.as[SourceDefinition] // Decodes from JSON model
            }
          }.toEither.flatMap(Predef.identity)
        }

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
