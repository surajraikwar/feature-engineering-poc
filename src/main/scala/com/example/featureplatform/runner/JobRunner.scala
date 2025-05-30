package com.example.featureplatform.runner

import com.example.featureplatform.config.{AppConfig, JobConfigLoader, SourceRegistry}
import com.example.featureplatform.features.TransformerFactory
import com.example.featureplatform.sources.DatabricksSparkSource
import com.example.featureplatform.spark.SparkSessionManager
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import io.circe.Json // Import for Json type

import scala.util.Try

/**
 * Main entry point for running feature engineering jobs.
 * This object orchestrates the loading of configurations, reading data,
 * applying feature transformations, and writing output, based on a job configuration file.
 */
object JobRunner {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * The main method for the JobRunner.
   *
   * @param args Command-line arguments:
   *             - args(0): Path to the job configuration YAML file.
   *             - args(1): Path to the root directory of the source catalog (containing source definitions).
   *             - args(2) (Optional): Spark master URL (e.g., "local[*]").
   */
  /**
   * Main entry point for the job
   * @param args Command line arguments:
   *             - args(0): Path to job config YAML
   *             - args(1): Path to source catalog directory
   *             - args(2): (Optional) Spark master URL (for local testing)
   */
  def main(args: Array[String]): Unit = {
    // Validate arguments
    if (args.length < 2) {
      logger.error(""""|Invalid arguments. Usage:
                     |  JobRunner <jobConfigPath> <sourceCatalogPath> [sparkMaster]
                     |
                     |  jobConfigPath    Path to job configuration YAML file
                     |  sourceCatalogPath Path to directory containing source definitions
                     |  sparkMaster      (Optional) Spark master URL (e.g., 'local[*]' for local testing)
                     |""".stripMargin)
      System.exit(1)
    }

    val jobConfigPath = args(0)
    val sourceCatalogPath = args(1)
    val sparkMaster = if (args.length > 2) Some(args(2)) else None

    logger.info(s"Starting job with Job Config: $jobConfigPath, Source Catalog: $sourceCatalogPath")
    if (sparkMaster.isDefined) logger.info(s"Spark master explicitly set to: ${sparkMaster.get}")

    // Initialize Spark session with proper error handling
    val spark = try {
      val session = SparkSessionManager.getSession(
        appName = "FeaturePlatformJobRunner",
        master = sparkMaster
      )
      logger.info("Successfully initialized Spark session")
      session
    } catch {
      case e: Exception =>
        logger.error("Failed to initialize Spark session", e)
        System.exit(1)
        throw e // This line will never be reached due to System.exit
    }

    // Load job configuration
    val result = for {
      // Validate Databricks configuration
      _ <- AppConfig.fromEnv() match {
        case Right(config) =>
          logger.info(s"Using Databricks workspace: ${config.databricks.serverHostname}")
          logger.info(s"Catalog: ${config.databricks.catalog}, Schema: ${config.databricks.schema}")
          Right(())
        case Left(error) =>
          Left(new Exception(s"Invalid Databricks configuration: $error"))
      }

      // Load job configuration
      jobConfig <- JobConfigLoader.loadJobConfig(jobConfigPath).left.map { e =>
        new Exception(s"Failed to load job config from $jobConfigPath: ${e.getMessage}", e)
      }
      _ = logger.info(s"Successfully loaded job config: ${jobConfig.job_name.getOrElse("Untitled Job")}")

      // Load source registry
      sourceRegistry <- SourceRegistry.loadFromDirectory(sourceCatalogPath).left.map { e =>
        new Exception(s"Failed to load source registry from $sourceCatalogPath: ${e.getMessage}", e)
      }
      _ = logger.info(s"Successfully loaded ${sourceRegistry.getAllSourceDefinitions().size} source definitions.")

      sourceName = jobConfig.input_source.name
      sourceVersionOpt = jobConfig.input_source.version

      // Handle source version: if None, try to get any version; if Some, get that specific version.
      sourceDef <- (sourceVersionOpt match {
        case Some(version) => sourceRegistry.getSourceDefinition(sourceName, version)
        case None => sourceRegistry.getSourceDefinition(sourceName) // Gets first available if multiple, or specific if only one
      }).toRight {
        val versionMsg = sourceVersionOpt.map(v => s"version $v").getOrElse("any version")
        new NoSuchElementException(s"Source '$sourceName' $versionMsg not found in registry.")
      }
      _ = logger.info(s"Found source definition: ${sourceDef.name} v${sourceDef.version}")
      
      // TODO: Instantiate the data source reader based on sourceDef.type (e.g. using a SourceReaderFactory)
      // For now, hardcoding DatabricksSparkSource as it's the primary one implemented.
      dataSourceReader = new DatabricksSparkSource() 
      
      initialDf <- dataSourceReader.read(spark, sourceDef).left.map { e =>
        new Exception(s"Failed to read source ${sourceDef.name}: ${e.getMessage}", e)
      }
      _ = logger.info(s"Successfully read initial DataFrame from source: ${sourceDef.name}. Schema:")
      _ = initialDf.printSchema() // Log schema

      // Apply feature transformers sequentially
      transformedDf <- jobConfig.feature_transformers.foldLeft(Right(initialDf): Either[Throwable, DataFrame]) { (dfEither, transformerConfig) =>
        dfEither.flatMap { df =>
          logger.info(s"Attempting to apply transformer: ${transformerConfig.name}")
          TransformerFactory.getTransformer(transformerConfig).flatMap { transformer =>
            Try(transformer.apply(df)).toEither.left.map { e =>
              new Exception(s"Error applying transformer ${transformerConfig.name}: ${e.getMessage}", e)
            }
          }.map{ transformed => logger.info(s"Successfully applied transformer: ${transformerConfig.name}"); transformed }
        }
      }
      _ = logger.info("All transformers applied successfully. Transformed DataFrame schema:")
      _ = transformedDf.printSchema()

    } yield {
      // Output Sink Logic
      logger.info(s"Processing output sink: ${jobConfig.output_sink.sink_type}")
      jobConfig.output_sink.sink_type.toLowerCase match {
        case "delta_table" | "delta" => // Allow "delta" as alias
          val sinkConfig = jobConfig.output_sink.config
          val path = sinkConfig.path.getOrElse(throw new IllegalArgumentException("Delta table path ('path') is required for output_sink type 'delta_table'."))
          val mode = sinkConfig.mode.getOrElse("overwrite")
          
          var writer = transformedDf.write.format("delta").mode(mode)
          
          sinkConfig.options.foreach { optMap =>
            val sparkOptions = optMap.map { case (k, vJson) =>
              k -> (vJson.asString.getOrElse { // Try as string first
                vJson.asBoolean.map(_.toString).getOrElse { // Then as boolean
                  vJson.asNumber.map(_.toString).getOrElse { // Then as number
                    logger.warn(s"Option $k value $vJson could not be converted to String/Boolean/Number, skipping.")
                    "" // Should ideally filter out these options
                  }
                }
              })
            }.filterNot(_._2.isEmpty)
            if(sparkOptions.nonEmpty) {
                logger.info(s"Applying sink options: $sparkOptions")
                writer = writer.options(sparkOptions)
            }
          }
          
          sinkConfig.partition_by.filter(_.nonEmpty).foreach { partitionCols =>
            logger.info(s"Partitioning output by: ${partitionCols.mkString(", ")}")
            writer = writer.partitionBy(partitionCols: _*)
          }
          
          writer.save(path)
          logger.info(s"Output written to Delta table: $path in mode $mode")

        case "display" =>
          val sinkConfig = jobConfig.output_sink.config
          val numRows = sinkConfig.num_rows.getOrElse(20)
          val truncate = sinkConfig.truncate.getOrElse(true) // Default true for Spark show
          logger.info(s"Displaying DataFrame (numRows=$numRows, truncate=$truncate):")
          transformedDf.show(numRows, truncate)

        case other =>
          logger.warn(s"Unsupported sink_type: '$other'. Output will not be written.")
          // Optionally: throw new IllegalArgumentException(s"Unsupported sink_type: $other")
      }
      logger.info(s"Successfully processed output for job: ${jobConfig.job_name.getOrElse("Untitled Job")}")
    }
    
    // Process the result
    result match {
      case Right(_) =>
        logger.info("""
          |========================================
          |  JOB COMPLETED SUCCESSFULLY
          |========================================""".stripMargin)

      case Left(e) =>
        val errorMsg = s"""
          |========================================
          |  JOB FAILED
          |========================================
          |${e.getMessage}
          |========================================""".stripMargin
        logger.error(errorMsg)
        logger.error("Stack trace:", e)
        System.exit(1)
    }

    // Clean up resources
    try {
      logger.info("Stopping Spark session...")
      spark.stop()
      logger.info("Spark session stopped")
    } catch {
      case e: Exception =>
        logger.warn("Error while stopping Spark session", e)
    }
  }
}
