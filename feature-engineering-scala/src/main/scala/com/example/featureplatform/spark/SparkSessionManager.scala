package com.example.featureplatform.spark

import org.apache.spark.sql.SparkSession

/**
 * Manages SparkSession creation and retrieval.
 * Ensures Delta Lake support is configured for the session.
 */
object SparkSessionManager {

  /**
   * Retrieves an active SparkSession or creates a new one if none exists.
   * Configures the session with Delta Lake extensions.
   *
   * @param appName The name for the Spark application.
   * @param master Optional Spark master URL (e.g., "local[*]" for local testing).
   *               If None, relies on the environment (e.g., Databricks cluster).
   * @return An active SparkSession.
   */
  def getSession(appName: String = "FeaturePlatformApp", master: Option[String] = None): SparkSession = {
    SparkSession.getActiveSession.getOrElse {
      val builder = SparkSession.builder()
        .appName(appName)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

      master.foreach(m => builder.master(m))

      builder.getOrCreate()
    }
  }
}
