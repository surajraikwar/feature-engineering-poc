package com.example.featureplatform.runner

import com.example.featureplatform.utils.SparkSessionTestWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.io.PrintWriter
import java.nio.file.{Files, Path}
import java.sql.Timestamp
// scala.util.Using is not available in Scala 2.12. Will use try/finally.

object JobRunnerSpecTestData {
  val transactionsSchema: StructType = StructType(Seq(
    StructField("transaction_id", StringType, true),
    StructField("user_id", StringType, true),
    StructField("transactiondatetime", TimestampType, true),
    StructField("transactionamount", DoubleType, true),
    StructField("creditdebitindicator", StringType, true),
    StructField("transactionchannel", StringType, true),
    StructField("jupiterfinegraincategory", StringType, true)
  ))

  val transactionsData: Seq[Row] = Seq(
    Row("txn1", "user1", Timestamp.valueOf("2023-01-15 10:30:00"), 1200.0, "DEBIT", "ATM", "Dining"),
    Row("txn2", "user2", Timestamp.valueOf("2023-01-15 11:00:00"), 300.0, "CREDIT", "MOBILE_BANKING", "Groceries"),
    Row("txn3", "user1", Timestamp.valueOf("2023-01-16 14:00:00"), 450.0, "DEBIT", "POS", "Shopping"),
    Row("txn4", "user3", Timestamp.valueOf("2023-01-16 18:00:00"), 600.0, "DEBIT", "ATM", "Cash Withdrawal"),
    Row("txn5", "user2", Timestamp.valueOf("2023-01-17 09:00:00"), 20.0, "DEBIT", "UPI_OTHER", "Travel")
  )
}

class JobRunnerSpec extends AnyWordSpec with Matchers with SparkSessionTestWrapper with EitherValues {

  import JobRunnerSpecTestData._

  var tempTestDir: Path = _
  var inputParquetPath: Path = _
  var jobConfigPath: Path = _
  var sourceCatalogDir: Path = _
  var outputDeltaPath: Path = _

  override def beforeAll(): Unit = {
    super.beforeAll() // Initializes spark session

    tempTestDir = Files.createTempDirectory("job_runner_spec_")
    inputParquetPath = tempTestDir.resolve("input_data/transactions.parquet")
    jobConfigPath = tempTestDir.resolve("job_config.yaml")
    sourceCatalogDir = tempTestDir.resolve("source_catalog")
    outputDeltaPath = tempTestDir.resolve("output_data.delta")

    Files.createDirectories(inputParquetPath.getParent)
    Files.createDirectories(sourceCatalogDir)

    // Create sample input Parquet file
    val df = spark.createDataFrame(spark.sparkContext.parallelize(transactionsData), transactionsSchema)
    df.write.mode("overwrite").parquet(inputParquetPath.toString)

    // Create source definition YAML pointing to the generated Parquet file
    val sourceDefContent =
      s"""
        |name: "integration_test_transactions"
        |version: "v1"
        |type: "parquet"
        |entity: "transaction"
        |location: "${inputParquetPath.toAbsolutePath.toString}" # Absolute path for Spark
        |fields:
        |  - {name: "transaction_id", type: "string"}
        |  - {name: "user_id", type: "string"}
        |  - {name: "transactiondatetime", type: "timestamp"}
        |  - {name: "transactionamount", type: "double"}
        |  - {name: "creditdebitindicator", type: "string"}
        |  - {name: "transactionchannel", type: "string"}
        |  - {name: "jupiterfinegraincategory", type: "string"}
        |config:
        |  format: "parquet"
        |metadata:
        |  created_by: "integration_test_setup"
      """.stripMargin
    val sourceDefFile = sourceCatalogDir.resolve("transaction_source.yaml").toFile
    val sourcePrintWriter = new PrintWriter(sourceDefFile)
    try {
      sourcePrintWriter.println(sourceDefContent)
    } finally {
      sourcePrintWriter.close()
    }

    // Create job configuration YAML
    val jobConfigContent =
      s"""
        |job_name: "Integration Test Job"
        |input_source:
        |  name: "integration_test_transactions"
        |  version: "v1"
        |feature_transformers:
        |  - name: "TransactionIndicatorDeriver"
        |    params: {}
        |  - name: "TransactionValueDeriver"
        |    params:
        |      input_col: "transactionamount"
        |      output_col: "is_high_value"
        |      high_value_threshold: 500.0
        |  - name: "TransactionDatetimeDeriver"
        |    params:
        |      transaction_timestamp_col: "transactiondatetime"
        |      output_col_hour: "tx_hour"
        |      output_col_day_of_week: "tx_day_of_week"
        |output_sink:
        |  sink_type: "delta"
        |  config:
        |    path: "${outputDeltaPath.toAbsolutePath.toString}" # Absolute path for Spark
        |    mode: "overwrite"
        |    options:
        |      "optionA": "valueA"
        |      "optionB": true
        |      "optionC": 123
      """.stripMargin
    val jobConfigFile = jobConfigPath.toFile
    val jobPrintWriter = new PrintWriter(jobConfigFile)
    try {
      jobPrintWriter.println(jobConfigContent)
    } finally {
      jobPrintWriter.close()
    }
  }

  override def afterAll(): Unit = {
    // Clean up temporary directory
    def deleteDirectory(path: Path): Unit = {
      if (Files.exists(path)) {
        Files.walk(path)
          .sorted(java.util.Comparator.reverseOrder())
          .map[java.io.File](_.toFile)
          .forEach(f => {
            // Add a print for debugging deletion issues if any
            // System.out.println(s"Deleting: ${f.getAbsolutePath} -> ${f.delete()}")
            f.delete()
          })
      }
    }
    if (tempTestDir != null) {
      deleteDirectory(tempTestDir)
    }
    super.afterAll() // Stops spark session
  }

  "JobRunner" should {
    "successfully execute an end-to-end feature engineering job" in {
      val sparkSession = spark // Local stable val

      val args = Array(
        jobConfigPath.toString,
        sourceCatalogDir.toString
        // sparkMaster is not passed, so JobRunner will use local[*] from SparkSessionTestWrapper default
      )

      // Capture System.exit calls to prevent test JVM from exiting
      // This is a bit advanced; for now, we'll assume JobRunner won't call System.exit(0) on success
      // or rely on the fact that an exception would make the test fail.
      // A more robust way would be to refactor JobRunner.main to return Either[Throwable, Unit]
      // and call that method instead of the one with System.exit.
      // For this test, we'll check for no exceptions and verify output.
      
      var jobError: Option[Throwable] = None
      try {
        JobRunner.main(args) // This will run the job and write to outputDeltaPath
      } catch {
        case e: Throwable => jobError = Some(e)
      }
      
      jobError shouldBe None // Check that JobRunner completed without throwing unhandled exceptions to main

      // Verify output using a new SparkSession to avoid issues with stopped contexts by JobRunner
      val verificationSpark = SparkSession.builder()
        .appName("JobRunnerSpecVerification")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.ui.enabled", "false") // Keep UI quiet for this verification session
        .getOrCreate()
      
      try {
        val outputDf = verificationSpark.read.format("delta").load(outputDeltaPath.toString)
        outputDf.count() shouldBe 5

        // Check for columns added by transformers
        val outputColumns = outputDf.columns.map(_.toLowerCase)
        outputColumns should contain allOf (
          "transaction_id", "user_id", "transactiondatetime", "transactionamount", 
          "creditdebitindicator", "transactionchannel", "jupiterfinegraincategory",
          "is_credit", "is_debit", // From TransactionIndicatorDeriver
          "is_high_value",        // From TransactionValueDeriver
          "tx_hour", "tx_day_of_week" // From TransactionDatetimeDeriver
        )

        // Verify some transformed values
        // Must import implicits from verificationSpark
        val implicits = verificationSpark.implicits
        import implicits._

        // Example: TransactionIndicatorDeriver for txn2 ("CREDIT")
        val txn2Indicator = outputDf.filter($"transaction_id" === "txn2").select("is_credit", "is_debit").first()
        txn2Indicator.getBoolean(0) shouldBe true  // is_credit
        txn2Indicator.getBoolean(1) shouldBe false // is_debit

        // Example: TransactionValueDeriver for txn1 (1200.0 > 500.0 threshold)
        val txn1Value = outputDf.filter($"transaction_id" === "txn1").select("is_high_value").first()
        txn1Value.getBoolean(0) shouldBe true

        // Example: TransactionValueDeriver for txn2 (300.0 < 500.0 threshold)
        val txn2Value = outputDf.filter($"transaction_id" === "txn2").select("is_high_value").first()
        txn2Value.getBoolean(0) shouldBe false
        
        // Example: TransactionDatetimeDeriver for txn1 (2023-01-15 10:30:00 -> Sunday is 1, Hour is 10)
        val txn1Datetime = outputDf.filter($"transaction_id" === "txn1").select("tx_hour", "tx_day_of_week").first()
        txn1Datetime.getInt(0) shouldBe 10 // tx_hour
        txn1Datetime.getInt(1) shouldBe 1  // tx_day_of_week (Sunday)
      } finally {
        verificationSpark.stop()
      }
    }
  }
}
