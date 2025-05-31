package com.example.featureplatform.utils

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

/**
 * Trait to provide a SparkSession for ScalaTest suites.
 * Manages the lifecycle of the SparkSession, creating it before all tests
 * in a suite and stopping it after all tests have run.
 */
trait SparkSessionTestWrapper extends BeforeAndAfterAll { this: Suite =>

  @transient protected lazy val spark: SparkSession = {
    System.out.println("Attempting to initialize SparkSession for test suite...")
    
    // Set system properties for Spark to work with newer JDK
    System.setProperty("java.security.manager", "allow")
    
    val builder = SparkSession.builder()
      .appName(s"FeatureTransformerTests-${this.getClass.getSimpleName}")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .config("spark.driver.extraJavaOptions", 
        "-Dio.netty.tryReflectionSetAccessible=true " +
        "--add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
        "--add-opens=java.base/java.io=ALL-UNNAMED " +
        "--add-opens=java.base/java.net=ALL-UNNAMED " +
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--add-opens=java.base/java.util=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
      )

    val session = builder.getOrCreate()
    
    // Set log level to WARN to reduce test output
    session.sparkContext.setLogLevel("WARN")
    
    System.out.println(s"SparkSession initialized: ${session.version}, master: ${session.sparkContext.master}")
    session
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Force initialization of the SparkSession before tests run
    spark
  }

  override def afterAll(): Unit = {
    try {
      if (SparkSession.getActiveSession.isDefined || SparkSession.getDefaultSession.isDefined) {
        System.out.println(s"Attempting to stop SparkSession for test suite ${this.getClass.getSimpleName}...")
        spark.stop()
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
