package com.featureplatform.runner

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import play.api.libs.json._
import com.typesafe.scalalogging.Logger

import java.io.File
import scala.util.{Try, Success, Failure}

object FeatureJobRunner {
  private val logger = Logger(getClass.getName)
  
  // Case classes for JSON serialization/deserialization
  case class JobConfig(
      job_name: String,
      description: Option[String],
      input_source: InputSource,
      feature_transformers: List[FeatureTransformer],
      output_sink: OutputSink
  )
  
  case class InputSource(
      name: String, 
      version: String,
      path: Option[String] = None,
      format: Option[String] = Some("parquet"),
      options: Option[Map[String, String]] = None
  )
  
  case class FeatureTransformer(
      name: String, 
      params: Map[String, String] = Map.empty
  )
  
  case class OutputSink(
      sink_type: String, 
      config: Map[String, String],
      format: Option[String] = None,
      options: Option[Map[String, String]] = None
  )
  
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
    
    // Initialize Spark session with Hive support disabled by default
    val spark = SparkSession.builder()
      .appName("FeaturePlatformJob")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // For datetime parsing
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.speculation", "false")
      .enableHiveSupport()
      .getOrCreate()
    
    try {
      // Read and parse the job configuration
      val jobConfig = readJobConfig(spark, configPath)
      jobConfig.description.foreach(desc => logger.info(s"Description: $desc"))
      logger.info(s"Running job: ${jobConfig.job_name}")
      
      // Execute the feature transformations
      runJob(spark, jobConfig)
      
      logger.info("Job completed successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Job failed: ${e.getMessage}", e)
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
  
  private def readJobConfig(spark: SparkSession, configPath: String): JobConfig = {
    logger.info(s"Reading configuration from: $configPath")
    val configContent = scala.io.Source.fromFile(configPath).mkString
    Json.parse(configContent).as[JobConfig]
  }
  
  private def runJob(spark: SparkSession, jobConfig: JobConfig): Unit = {
    logger.info(s"Processing input source: ${jobConfig.input_source.name}")
    
    // Read the input source
    val inputDf = readInputSource(spark, jobConfig.input_source)
    
    // Show sample data and schema
    logger.info("Input data schema:")
    inputDf.printSchema()
    logger.info("Sample data (first 5 rows):")
    inputDf.show(5, truncate = false)
    
    // Apply feature transformations
    val transformedDf = jobConfig.feature_transformers.foldLeft(inputDf) { (df, transformer) =>
      logger.info(s"Applying transformer: ${transformer.name}")
      val resultDf = applyTransformer(spark, df, transformer)
      logger.info(s"Schema after ${transformer.name}:")
      resultDf.printSchema()
      resultDf
    }
    
    // Write the output
    writeOutput(transformedDf, jobConfig.output_sink)
  }
  
  private def readInputSource(spark: SparkSession, inputSource: InputSource): DataFrame = {
    inputSource.path match {
      case Some(path) =>
        // Read from file path
        val format = inputSource.format.getOrElse("parquet")
        logger.info(s"Reading from $format file: $path")
        
        val reader = spark.read.format(format)
        
        // Add any additional options
        inputSource.options.getOrElse(Map.empty).foreach { case (k, v) =>
          reader.option(k, v)
        }
        
        reader.load(path)
        
      case None =>
        // Fall back to table name
        logger.info(s"Reading from table: ${inputSource.name}")
        spark.table(inputSource.name)
    }
  }
  
  private def applyTransformer(spark: SparkSession, df: org.apache.spark.sql.DataFrame, 
                             transformer: FeatureTransformer): org.apache.spark.sql.DataFrame = {
    import com.featureplatform.transformers._
    
    transformer.name match {
      case "TransactionStatusDeriver" => 
        logger.info("Applying TransactionStatusDeriver")
        TransactionTransformers.deriveTransactionStatus(df)
        
      case "TransactionChannelDeriver" =>
        logger.info("Applying TransactionChannelDeriver")
        TransactionTransformers.deriveTransactionChannel(df)
        
      case "TransactionValueDeriver" =>
        logger.info("Applying TransactionValueDeriver")
        val threshold = transformer.params.getOrElse("high_value_threshold", "1000.0").toDouble
        val amountCol = transformer.params.getOrElse("input_col", "transactionamount")
        TransactionTransformers.deriveTransactionValue(df, amountCol, threshold)
        
      case "TransactionDatetimeDeriver" =>
        logger.info("Applying TransactionDatetimeDeriver")
        val timestampCol = transformer.params.getOrElse("timestamp_col", "transactiontimestamp")
        TransactionTransformers.deriveDatetimeFeatures(df, timestampCol)
        
      case "TransactionModeDeriver" =>
        logger.info("Applying TransactionModeDeriver")
        TransactionTransformers.deriveTransactionMode(df)
        
      case "TransactionCategoryDeriver" =>
        logger.info("Applying TransactionCategoryDeriver")
        TransactionTransformers.deriveMerchantCategories(df)
        
      case "TransactionIndicatorDeriver" =>
        logger.info("Applying TransactionIndicatorDeriver")
        // This is a composite transformer that applies all transformations
        val transformer = new TransactionFeatureTransformer(spark)
        transformer.transform(df)
        
      case _ =>
        logger.warn(s"Unknown transformer: ${transformer.name}")
        df
    }
  }
  
  private def writeOutput(df: org.apache.spark.sql.DataFrame, outputSink: OutputSink): Unit = {
    val outputPath = outputSink.config.getOrElse("path", 
      throw new IllegalArgumentException("Output path not specified"))
    
    val mode = outputSink.config.getOrElse("mode", "overwrite")
    val format = outputSink.format.getOrElse(
      outputSink.sink_type match {
        case "delta" => "delta"
        case "parquet" => "parquet"
        case "csv" => "csv"
        case "json" => "json"
        case "table" => ""
        case _ => throw new UnsupportedOperationException(s"Unsupported output format: ${outputSink.sink_type}")
      }
    )
    
    logger.info(s"Writing output to $format: $outputPath (mode: $mode)")
    
    // Create writer with common options
    val writer = df.write.mode(mode)
    
    // Add any additional options
    outputSink.options.getOrElse(Map.empty).foreach { case (k, v) =>
      writer.option(k, v)
    }
    
    // Write the data based on the sink type
    outputSink.sink_type.toLowerCase match {
      case "delta" =>
        writer.format("delta").save(outputPath)
        
      case "parquet" | "csv" | "json" | "orc" =>
        writer.format(format).save(outputPath)
        
      case "table" =>
        writer.saveAsTable(outputPath)
        
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported output sink type: ${outputSink.sink_type}")
    }
    
    logger.info(s"Successfully wrote output to $outputPath")
  }
}
