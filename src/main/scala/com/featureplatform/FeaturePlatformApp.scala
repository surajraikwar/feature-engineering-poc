package com.featureplatform

import cats.effect._
import cats.implicits._
import com.featureplatform.config._
import com.featureplatform.repository.FeatureRepository
import com.featureplatform.service.FeatureService
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

object FeaturePlatformApp extends IOApp {
  
  override def run(args: List[String]): IO[ExitCode] = {
    // Create a blocking execution context for IO operations
    val blockingEC = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
    
    // Load configuration
    val config = ConfigSource.default.loadOrThrow[AppConfig]
    
    // Create Spark session
    val spark = SparkSession.builder()
      .appName(config.spark.appName)
      .master(config.spark.master)
      .config("spark.sql.shuffle.partitions", config.spark.shufflePartitions.toString)
      // Delta Lake configurations
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .config("spark.databricks.delta.properties.defaults.logRetentionDuration", config.spark.delta.logRetentionDuration)
      .config("spark.databricks.delta.properties.defaults.checkpointRetentionDuration", config.spark.delta.checkpointRetentionDuration)
      .config("spark.databricks.delta.optimizeWrite.enabled", config.spark.delta.optimizeWrite.toString)
      .config("spark.databricks.delta.autoCompact.enabled", config.spark.delta.autoCompact.toString)
      .getOrCreate()
    
    // Create resources that need to be cleaned up
    val resources = for {
      _ <- Resource.eval(IO.delay(println("Starting Feature Platform...")))
      _ <- Resource.make(IO.unit)(_ => IO.delay(spark.stop()).void)
    } yield ()
    
    // Main program logic
    val program = resources.use { _ =>
      for {
        // Initialize repositories and services
        featureRepo = FeatureRepository[IO](spark, config.featureStore)
        featureService = FeatureService[IO](featureRepo, spark)
        
        // Example: Process transactions and compute features
        _ <- IO.delay(println("Processing transactions..."))
        
        // In a real application, you would read transactions from your data source
        // For example: val transactions = spark.read.format("delta").load("path/to/transactions").as[Transaction]
        // For now, we'll use an empty dataset
        transactions = spark.emptyDataset[Transaction]
        
        // Compute features
        features <- featureService.computeTransactionFeatures(transactions)
        _ <- IO.delay(println(s"Computed ${features.count()} features"))
        
        // Save features
        _ <- featureService.saveFeatures(features)
        _ <- IO.delay(println("Features saved successfully"))
        
      } yield ExitCode.Success
    }
    
    // Handle any errors
    program.handleErrorWith { error =>
      IO.delay(error.printStackTrace()) *> 
      IO(ExitCode.Error)
    }
  }
}
