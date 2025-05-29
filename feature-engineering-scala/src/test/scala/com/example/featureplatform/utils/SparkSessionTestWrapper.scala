package com.example.featureplatform.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Trait to provide a SparkSession for ScalaTest suites.
 * Manages the lifecycle of the SparkSession, creating it before all tests
 * in a suite and stopping it after all tests have run.
 */
trait SparkSessionTestWrapper extends BeforeAndAfterAll { this: Suite =>

  @transient protected lazy val spark: SparkSession = {
    // Using println for simple debug output visible in sbt test logs if initialization occurs.
    // Consider a proper logger if more advanced logging is needed here.
    System.out.println("Attempting to initialize SparkSession for test suite...")
    val session = SparkSession.builder()
      .appName(s"FeatureTransformerTests-${this.getClass.getSimpleName}")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.ui.enabled", "false") // Disable Spark UI for tests
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    System.out.println(s"SparkSession initialized: ${session.version}, master: ${session.sparkContext.master}")
    session
  }

  override def afterAll(): Unit = {
    try {
      // Access spark val to ensure it's stopped if it was initialized.
      // If it was never accessed (e.g. no tests run or suite aborted early), it might not be initialized.
      // A direct check on a private var or a different flag might be more robust if needed.
      // For now, this relies on spark being accessed if any test runs.
      if (SparkSession.getActiveSession.isDefined || SparkSession.getDefaultSession.isDefined) {
         // Referring to 'spark' lazy val here will initialize it if not already, then stop.
         // This is to ensure cleanup even if tests didn't explicitly use the 'spark' val.
         // However, if it was never used, stopping a newly created one is wasteful.
         // A better approach might be to track initialization status.
         // For simplicity, let's assume if afterAll is called, spark might have been used.
        System.out.println(s"Attempting to stop SparkSession for test suite ${this.getClass.getSimpleName}...")
        spark.stop() // Stop the lazy val session
        System.out.println("SparkSession stopped.")
      }
    } finally {
      super.afterAll()
      // Clean up the global SparkSession to avoid interference between test suites
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}
