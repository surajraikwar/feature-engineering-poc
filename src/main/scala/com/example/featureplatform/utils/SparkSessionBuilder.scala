package com.example.featureplatform.utils

import com.example.featureplatform.config.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Utility for building a SparkSession with Databricks configuration
 */
object SparkSessionBuilder {

  /**
   * Create a SparkSession configured for Databricks
   */
  def createDatabricksSession(appName: String, config: AppConfig): SparkSession = {
    val sparkConf = new SparkConf()
      .setAppName(appName)
      .setMaster(config.spark.master)
      .set("spark.databricks.service.client.enabled", "true")
      .set("spark.databricks.service.port", "8787")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.databricks.delta.preview.enabled", "true")
      .set("spark.sql.warehouse.dir", "/databricks/spark/warehouse")
      .set("spark.sql.catalog.implementation", "hive")
      .set("spark.sql.legacy.createHiveTableByDefault", "false")

    // Add any additional Spark configs
    config.spark.config.foreach { case (k, v) =>
      sparkConf.set(k, v)
    }

    // Initialize SparkSession
    val builder = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .config("spark.sql.catalog.databricks_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.catalog.databricks_catalog.warehouse", "/databricks/spark/warehouse")
      .config("spark.sql.catalog.databricks_catalog.type", "hive")
      .config("spark.sql.catalog.databricks_catalog.provider", "hive")
      .config("spark.databricks.delta.catalog.databricks_catalog", "io.delta.sql.DeltaCatalog")

    // Set Databricks specific configurations if available
    if (config.databricks.serverHostname.nonEmpty) {
      builder
        .config("spark.databricks.service.address", s"${config.databricks.serverHostname}:443")
        .config("spark.databricks.service.token", config.databricks.token)
        .config("spark.databricks.service.clusterId", config.databricks.clusterId)
        .config("spark.databricks.service.server.enabled", "true")
        .config("spark.databricks.service.port", "8787")
    }

    // Set catalog and schema if specified
    if (config.databricks.catalog.nonEmpty) {
      builder.config("spark.sql.catalog.databricks_catalog.catalog.name", config.databricks.catalog)
    }
    if (config.databricks.schema.nonEmpty) {
      builder.config("spark.sql.catalog.databricks_catalog.schema.name", config.databricks.schema)
    }

    // Build and return the session
    builder.getOrCreate()
  }

  /**
   * Create a local SparkSession for testing/development
   */
  def createLocalSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }
}
