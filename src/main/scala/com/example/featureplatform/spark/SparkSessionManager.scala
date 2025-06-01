package com.example.featureplatform.spark

import com.example.featureplatform.config.AppConfig
import org.apache.spark.SparkConf // Import SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

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
      val sparkConf = new SparkConf()

      // Master Configuration
      if (master.isDefined) {
        master.foreach { m =>
          logger.info(s"Spark master explicitly set by argument to: $m")
          sparkConf.set("spark.master", m)
          // Add local development SparkConf settings if master is provided
          sparkConf.set("spark.driver.memory", "4g")
          sparkConf.set("spark.executor.memory", "4g")
          sparkConf.set("spark.driver.maxResultSize", "2g")
        }
      } else {
        logger.info("Spark master not set by argument, SPARK_REMOTE may override or defaulting to 'local[*]'.")
        sparkConf.set("spark.master", "local[*]") // Default/fallback if not overridden by SPARK_REMOTE
      }
      
      // Databricks service configurations are now expected to be handled by SPARK_REMOTE.
      // Logging appConfig for informational purposes if available.
      AppConfig.fromEnv() match {
        case Right(appConfig) =>
          val dbConfig = appConfig.databricks
          logger.info(s"Databricks configuration loaded (serverHostname: ${dbConfig.serverHostname}, catalog: ${dbConfig.catalog}, schema: ${dbConfig.schema}). SparkSessionManager will not set service.* properties.")
        case Left(error) =>
          logger.warn(s"Failed to load Databricks configuration: $error. Proceeding with local/default Spark setup.")
      }

      // Initialize builder with SparkConf and common configurations
      val builder = SparkSession.builder()
        .config(sparkConf) // Apply spark.master (and other SparkConf settings if any)
        .appName(appName)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.sql.legacy.createHiveTableByDefault", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200") // Adjust based on your cluster size
        .enableHiveSupport()
      
      // Apply Databricks specific Delta and catalog/schema configurations to the builder
      // These are not core connection parameters and are fine to set on the builder
      AppConfig.fromEnv() match {
        case Right(appConfig) =>
          val dbConfig = appConfig.databricks
          builder
            .config("spark.databricks.delta.preview.enabled", "true")
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")

          // Catalog and schema builder configurations removed here, will be set by USE CATALOG/SCHEMA post-session creation
        case Left(_) => // Already logged warning, no further action needed here for these configs
      }

      // Create the session
      val session = builder.getOrCreate()
      
      // Configure Delta Lake (This part seems to be mostly logging and setting log level)
      // USE CATALOG/SCHEMA logic removed, relying on SPARK_REMOTE and shell script --conf for context.
      try {
        // Programmatically set current catalog and schema
        AppConfig.fromEnv() match {
          case Right(loadedConfig) =>
            val effectiveDbConfig = loadedConfig.databricks
            if (effectiveDbConfig.catalog.nonEmpty) {
              logger.info(s"Programmatically setting current catalog for session to: ${effectiveDbConfig.catalog}")
              try {
                session.catalog.setCurrentCatalog(effectiveDbConfig.catalog)
                // Only attempt to set schema if catalog was successfully set and schema is defined
                if (effectiveDbConfig.schema.nonEmpty) {
                  logger.info(s"Programmatically setting current database (schema) for session to: ${effectiveDbConfig.schema}")
                  session.catalog.setCurrentDatabase(effectiveDbConfig.schema)
                }
              } catch {
                case e: Exception =>
                  logger.warn(s"Failed to programmatically set catalog/schema: ${e.getMessage}", e)
                  // Decide if this should be a fatal error or just a warning
              }
            }
          case Left(error) =>
            logger.warn(s"Cannot set catalog/schema programmatically as AppConfig failed to load: $error")
        }

        // Import Delta Lake implicits
        
        // Set log level to WARN to reduce verbosity
        session.sparkContext.setLogLevel("WARN")
        
        // Log Spark configuration
        logger.info("Spark session created with configuration:")
        Seq(
          "spark.app.name",
          "spark.master",
          // "spark.databricks.service.host", // No longer set by SparkSessionManager
          // "spark.databricks.service.clusterId", // No longer set by SparkSessionManager
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
