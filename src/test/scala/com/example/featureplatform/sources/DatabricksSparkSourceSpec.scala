package com.example.featureplatform.sources

import com.example.featureplatform.config.{DatabricksSourceDetailConfig, FieldDefinition, SourceDefinition}
import com.example.featureplatform.utils.SparkSessionTestWrapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Date

object DatabricksSparkSourceSpec {
  // Define schema and path for consistent use
  val sampleDataSchema: StructType = StructType(Seq(
    StructField("id", StringType, true),
    StructField("name", StringType, true),
    StructField("value", DoubleType, true),
    StructField("event_date", DateType, true)
  ))

  val sampleData: Seq[Row] = Seq(
    Row("1", "alpha", 10.5, Date.valueOf("2023-01-15")),
    Row("2", "beta", 20.0, Date.valueOf("2023-01-16")),
    Row("3", "gamma", 30.75, Date.valueOf("2023-01-17"))
  )
  
  val sampleParquetPath: String = "target/test_data/sample_source_table.parquet" // Use target for generated files
  val sampleDeltaPath: String = "target/test_data/sample_source_table.delta"
}

class DatabricksSparkSourceSpec extends AnyWordSpec with Matchers with SparkSessionTestWrapper with EitherValues {

  import DatabricksSparkSourceSpec._ // Import constants

  override def beforeAll(): Unit = {
    super.beforeAll() // This initializes spark session via lazy val access or explicit call
    
    // Create the base directory for test data if it doesn't exist
    Files.createDirectories(Paths.get(sampleParquetPath).getParent)
    Files.createDirectories(Paths.get(sampleDeltaPath).getParent)

    // Create sample Parquet file
    val df = spark.createDataFrame(spark.sparkContext.parallelize(sampleData), sampleDataSchema)
    df.write.mode(SaveMode.Overwrite).parquet(sampleParquetPath)

    // Create sample Delta file
    df.write.mode(SaveMode.Overwrite).format("delta").save(sampleDeltaPath)
  }
  
  override def afterAll(): Unit = {
    super.afterAll() // This stops the spark session
    // Clean up generated test data
    def deleteDirectory(path: String): Unit = {
      val dir = new File(path)
      if (dir.exists()) {
        Option(dir.listFiles()).foreach(_.foreach { file =>
          if (file.isDirectory) deleteDirectory(file.getAbsolutePath) else file.delete()
        })
        dir.delete()
      }
    }
    deleteDirectory(sampleParquetPath)
    deleteDirectory(sampleDeltaPath)
    // Delete parent if empty, be careful
    // Files.deleteIfExists(Paths.get("target/test_data")) 
  }

  val reader = new DatabricksSparkSource()

  "DatabricksSparkSource.read" should {

    "successfully read a Parquet file from location" in {
      val sparkSession = spark // Local stable val for SparkSession

      val sourceDef = SourceDefinition(
        name = "test_parquet", version = "1.0", `type` = "parquet", entity = "test",
        location = Some(sampleParquetPath),
        config = DatabricksSourceDetailConfig() // Empty, as location and type are primary
      )
      val result = reader.read(spark, sourceDef)
      result.isRight should be (true)
      result.value.count() shouldBe 3
      result.value.schema shouldEqual sampleDataSchema
      // Collect and compare data if needed, ensure order or use set comparison
      val collectedData = result.value.collect().map(r => Row(r.getString(0), r.getString(1), r.getDouble(2), r.getDate(3)))
      collectedData should contain theSameElementsAs sampleData
    }

    "successfully read a Delta file from location" in {
      val sparkSession = spark

      val sourceDef = SourceDefinition(
        name = "test_delta", version = "1.0", `type` = "delta", entity = "test",
        location = Some(sampleDeltaPath),
        config = DatabricksSourceDetailConfig()
      )
      val result = reader.read(spark, sourceDef)
      result.isRight should be (true)
      result.value.count() shouldBe 3
      result.value.schema shouldEqual sampleDataSchema
      val collectedData = result.value.collect().map(r => Row(r.getString(0), r.getString(1), r.getDouble(2), r.getDate(3)))
      collectedData should contain theSameElementsAs sampleData
    }

    "perform successful schema validation if fields match" in {
      val sparkSession = spark
      val fields = List(
        FieldDefinition("id", "StringType"), // Type matching is not strictly checked by current impl, only presence
        FieldDefinition("value", "DoubleType")
      )
      val sourceDef = SourceDefinition(
        name = "test_schema_val_ok", version = "1.0", `type` = "parquet", entity = "test",
        location = Some(sampleParquetPath),
        fields = Some(fields),
        config = DatabricksSourceDetailConfig()
      )
      val result = reader.read(spark, sourceDef)
      result.isRight should be (true)
      result.value.count() shouldBe 3
    }

    "return Left with IllegalArgumentException if schema validation fails (missing field)" in {
      val sparkSession = spark
      val fields = List(
        FieldDefinition("id", "StringType"),
        FieldDefinition("non_existent_field", "StringType")
      )
      val sourceDef = SourceDefinition(
        name = "test_schema_val_fail", version = "1.0", `type` = "parquet", entity = "test",
        location = Some(sampleParquetPath),
        fields = Some(fields),
        config = DatabricksSourceDetailConfig()
      )
      val result = reader.read(spark, sourceDef)
      result.isLeft should be (true)
      result.left.value shouldBe a [IllegalArgumentException]
      result.left.value.getMessage should include ("DataFrame is missing field(s) defined in SourceDefinition: non_existent_field")
    }
    
    "successfully read if SourceDefinition fields are a subset of DataFrame columns (no strict validation for extra columns)" in {
      val sparkSession = spark
       val fields = List(
        FieldDefinition("id", "StringType") // Only 'id' is defined
      )
      val sourceDef = SourceDefinition(
        name = "test_schema_subset", version = "1.0", `type` = "parquet", entity = "test",
        location = Some(sampleParquetPath),
        fields = Some(fields),
        config = DatabricksSourceDetailConfig()
      )
      val result = reader.read(spark, sourceDef)
      result.isRight should be (true) // Should pass as 'id' is present
      result.value.columns should contain ("id")
      result.value.columns.length should be > 1 // Original DF has more columns
    }

    "successfully read from a SQL query" in {
      val sparkSession = spark
      // Create a temp view to query against
      val tempDf = spark.createDataFrame(spark.sparkContext.parallelize(sampleData), sampleDataSchema)
      tempDf.createOrReplaceTempView("query_test_source")

      val sourceDef = SourceDefinition(
        name = "test_query", version = "1.0", `type` = "sql_query", entity = "test",
        config = DatabricksSourceDetailConfig(query = Some("SELECT id, name FROM query_test_source WHERE value > 15.0"))
      )
      val result = reader.read(spark, sourceDef)
      result.isRight should be (true)
      result.value.count() shouldBe 2 // beta (20.0), gamma (30.75)
      result.value.schema.fieldNames should contain theSameElementsAs Seq("id", "name")
      
      spark.catalog.dropTempView("query_test_source")
    }

    "successfully read from a table name (using a temp view)" in {
      val sparkSession = spark
      val tempDf = spark.createDataFrame(spark.sparkContext.parallelize(sampleData), sampleDataSchema)
      tempDf.createOrReplaceTempView("my_test_table_view")

      val sourceDef = SourceDefinition(
        name = "test_table_view", version = "1.0", `type` = "databricks_table", entity = "test",
        config = DatabricksSourceDetailConfig(table = Some("my_test_table_view")) // No catalog/schema, uses default
      )
      val result = reader.read(spark, sourceDef)
      result.isRight should be (true)
      result.value.count() shouldBe 3
      result.value.schema shouldEqual sampleDataSchema
      
      spark.catalog.dropTempView("my_test_table_view")
    }
    
    "return Left with IllegalArgumentException for invalid configuration (no query, table, or location)" in {
      val sparkSession = spark
      val sourceDef = SourceDefinition(
        name = "test_invalid_config", version = "1.0", `type` = "unknown", entity = "test",
        config = DatabricksSourceDetailConfig() // All relevant fields are None
      )
      val result = reader.read(spark, sourceDef)
      result.isLeft should be (true)
      result.left.value shouldBe a [IllegalArgumentException]
      result.left.value.getMessage should include ("Databricks source configuration is incomplete")
    }

    "return Left for a non-existent file path" in {
      val sparkSession = spark
      val sourceDef = SourceDefinition(
        name = "test_non_existent_file", version = "1.0", `type` = "parquet", entity = "test",
        location = Some("target/test_data/non_existent_file.parquet"),
        config = DatabricksSourceDetailConfig()
      )
      val result = reader.read(spark, sourceDef)
      result.isLeft should be (true)
      // Spark throws AnalysisException for file not found, which is caught by Try
      // The exact exception type might vary based on Spark version and specific error.
      // Checking for a general Exception or a SparkException superclass might be more robust.
      result.left.value shouldBe a [org.apache.spark.sql.AnalysisException]
    }
  }
}
