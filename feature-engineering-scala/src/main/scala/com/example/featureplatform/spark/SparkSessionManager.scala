package com.example.featureplatform.spark

import com.example.featureplatform.config.AppConfig
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

/**
 * Manages SparkSession creation and retrieval.
 * Configures the session with Delta Lake extensions and Databricks-specific settings.
 */
object SparkSessionManager {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Retrieves an active SparkSession or creates a new one if none exists.
   * Configures the session with Delta Lake extensions and Databricks settings.
   *
   * @param appName The name for the Spark application.
   * @param master Optional Spark master URL. For Databricks, this is typically None.
   * @return An active SparkSession.
   */
  def getSession(appName: String = "FeaturePlatformApp", master: Option[String] = None): SparkSession = {
    SparkSession.getActiveSession.getOrElse {
      logger.info(s"Creating new SparkSession for application: $appName")
      
      // Initialize builder with common configurations
      val builder = SparkSession.builder()
        .appName(appName)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200") // Adjust based on your cluster size
        .enableHiveSupport()

      // Set master if provided (for local testing)
      master.foreach { m =>
        logger.info(s"Setting master to: $m")
        builder.master(m)
        
        // Additional local development settings
        builder.config("spark.driver.memory", "4g")
        builder.config("spark.executor.memory", "4g")
        builder.config("spark.driver.maxResultSize", "2g")
      }

      // Get Databricks configuration from environment
      AppConfig.fromEnv() match {
        case Right(appConfig) =>
          val dbConfig = appConfig.databricks
          logger.info(s"Configuring Databricks connection to workspace: ${dbConfig.serverHostname}")
          
          // Set Databricks connection properties
          builder
            .config("spark.databricks.service.address", s"${dbConfig.serverHostname}:443")
            .config("spark.databricks.service.token", dbConfig.token)
            .config("spark.databricks.service.clusterId", dbConfig.clusterId)
            .config("spark.databricks.service.server.enabled", "true")
            .config("spark.databricks.delta.preview.enabled", "true")
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
          
          // Set catalog and schema if specified
          if (dbConfig.catalog.nonEmpty) {
            logger.info(s"Setting catalog to: ${dbConfig.catalog}")
            builder.config("spark.sql.catalog.databricks_catalog.catalog.name", dbConfig.catalog)
          }
          if (dbConfig.schema.nonEmpty) {
            logger.info(s"Setting schema to: ${dbConfig.schema}")
            builder.config("spark.sql.catalog.databricks_catalog.schema.name", dbConfig.schema)
          }
          
        case Left(error) =>
          logger.warn(s"Failed to load Databricks configuration: $error")
          logger.warn("Running in local mode without Databricks integration")
      }

      // Create the session
      val session = builder.getOrCreate()
      
      // Configure Delta Lake
      try {
        // Import Delta Lake implicits
        import io.delta.implicits._
        import io.delta.tables._
        
        // Set log level to WARN to reduce verbosity
        session.sparkContext.setLogLevel("WARN")
        
        // Log Spark configuration
        logger.info("Spark session created with configuration:")
        Seq(
          "spark.app.name",
          "spark.master",
          "spark.databricks.service.address",
          "spark.sql.catalog.spark_catalog"
        ).foreach { key =>
          logger.info(s"  $key = ${session.conf.getOption(key).getOrElse("<not set>")}")
        }
        
        session
      } catch {
        case e: Exception =>
          logger.error("Failed to initialize Delta Lake extensions. Make sure Delta Lake dependencies are included.", e)
          // Continue with the session even if Delta init fails
          session
      }
    }
  }
}
