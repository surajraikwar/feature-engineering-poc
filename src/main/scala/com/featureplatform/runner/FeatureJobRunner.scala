package com.featureplatform.runner

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import scalaj.http.{Http, HttpRequest, HttpResponse}
import play.api.libs.json._
import com.typesafe.scalalogging.Logger

import java.io.File
import scala.util.{Failure, Success, Try}

object FeatureJobRunner {
  private val logger = Logger(getClass.getName)
  
  // Case classes for JSON serialization/deserialization
  case class JobConfig(
      job_name: String,
      input_source: InputSource,
      feature_transformers: List[FeatureTransformer],
      output_sink: OutputSink
  )
  
  case class InputSource(name: String, version: String)
  case class FeatureTransformer(name: String, params: Map[String, String] = Map.empty)
  case class OutputSink(sink_type: String, config: Map[String, String])
  
  // Implicit JSON formatters
  implicit val inputSourceFormat: Format[InputSource] = Json.format[InputSource]
  implicit val featureTransformerFormat: Format[FeatureTransformer] = Json.format[FeatureTransformer]
  implicit val outputSinkFormat: Format[OutputSink] = Json.format[OutputSink]
  implicit val jobConfigFormat: Format[JobConfig] = Json.format[JobConfig]
  
  def main(args: Array[String]): Unit = {
    // Set log level
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    if (args.length < 1) {
      logger.error("Usage: FeatureJobRunner <path-to-job-config>")
      System.exit(1)
    }
    
    val configPath = args(0)
    logger.info(s"Loading job configuration from: $configPath")
    
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName("FeaturePlatformJob")
      .getOrCreate()
    
    try {
      // Read and parse the job configuration
      val jobConfig = readJobConfig(spark, configPath)
      logger.info(s"Running job: ${jobConfig.job_name}")
      
      // Execute the feature transformations
      runJob(spark, jobConfig)
      
      logger.info("Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Job failed: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }
  
  private def readJobConfig(spark: SparkSession, configPath: String): JobConfig = {
    val configContent = spark.sparkContext.wholeTextFiles(configPath).first()._2
    Json.parse(configContent).as[JobConfig]
  }
  
  private def runJob(spark: SparkSession, jobConfig: JobConfig): Unit = {
    logger.info(s"Processing input source: ${jobConfig.input_source.name}")
    
    // Read the input source
    val inputDf = spark.table(s"${jobConfig.input_source.name}")
    
    // Apply feature transformations
    val transformedDf = jobConfig.feature_transformers.foldLeft(inputDf) { (df, transformer) =>
      logger.info(s"Applying transformer: ${transformer.name}")
      applyTransformer(spark, df, transformer)
    }
    
    // Write the output
    writeOutput(transformedDf, jobConfig.output_sink)
  }
  
  private def applyTransformer(spark: SparkSession, df: org.apache.spark.sql.DataFrame, 
                             transformer: FeatureTransformer): org.apache.spark.sql.DataFrame = {
    transformer.name match {
      case "TransactionStatusDeriver" => 
        // Implement your transformer logic here
        logger.info("Applying TransactionStatusDeriver")
        df
        
      case "TransactionChannelDeriver" =>
        // Implement your transformer logic here
        logger.info("Applying TransactionChannelDeriver")
        df
        
      // Add more transformers as needed
      
      case _ =>
        logger.warn(s"Unknown transformer: ${transformer.name}")
        df
    }
  }
  
  private def writeOutput(df: org.apache.spark.sql.DataFrame, outputSink: OutputSink): Unit = {
    outputSink.sink_type match {
      case "delta" =>
        val outputPath = outputSink.config.getOrElse("path", 
          throw new IllegalArgumentException("Output path not specified"))
        logger.info(s"Writing output to Delta table: $outputPath")
        
        val mode = outputSink.config.getOrElse("mode", "overwrite")
        df.write
          .format("delta")
          .mode(mode)
          .save(outputPath)
          
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported output sink type: ${outputSink.sink_type}")
    }
  }
}
