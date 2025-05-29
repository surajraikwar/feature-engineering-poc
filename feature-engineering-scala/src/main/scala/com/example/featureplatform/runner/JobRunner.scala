package com.example.featureplatform.runner

import com.example.featureplatform.config.{JobConfigLoader, SourceRegistry}
import com.example.featureplatform.spark.SparkSessionManager
import com.example.featureplatform.sources.DatabricksSparkSource // Assuming this is the primary reader for now
import com.example.featureplatform.features.TransformerFactory
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory
import io.circe.Json
import scala.util.Try // Added import for Try

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
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      logger.error("Usage: JobRunner <jobConfigPath> <sourceCatalogPath> [sparkMaster]")
      System.exit(1)
    }

    val jobConfigPath = args(0)
    val sourceCatalogPath = args(1)
    val sparkMaster = if (args.length > 2) Some(args(2)) else None

    logger.info(s"Starting job with Job Config: $jobConfigPath, Source Catalog: $sourceCatalogPath")
    if(sparkMaster.isDefined) logger.info(s"Spark master explicitly set to: ${sparkMaster.get}")

    val spark = SparkSessionManager.getSession(appName = "FeaturePlatformJobRunner", master = sparkMaster)
    spark.sparkContext.setLogLevel("WARN") // Reduce verbosity for job run

    val result: Either[Throwable, Unit] = for {
      jobConfig <- JobConfigLoader.loadJobConfig(jobConfigPath).left.map { e =>
        new Exception(s"Failed to load job config from $jobConfigPath: ${e.getMessage}", e)
      }
      _ = logger.info(s"Successfully loaded job config for: ${jobConfig.job_name.getOrElse("Untitled Job")}")

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
    
    // Handle overall result
    result match {
      case Right(_) => 
        logger.info("Job completed successfully.")
      case Left(e) => 
        logger.error(s"Job failed: ${e.getMessage}", e)
        // e.printStackTrace() // Uncomment for full stack trace if needed for debugging
        System.exit(1) // Exit with error code
    }
    
    logger.info("Stopping Spark session.")
    spark.stop()
  }
}
