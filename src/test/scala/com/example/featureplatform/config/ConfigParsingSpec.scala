package com.example.featureplatform.config

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues
import java.io.{File, PrintWriter}
import scala.util.Try

class ConfigParsingSpec extends AnyWordSpec with Matchers with EitherValues {

  // Helper to get resource path. For test resources
  def getResourcePath(relativePath: String): String = {
    s"src/test/resources/$relativePath"
  }

  "JobConfigLoader" should {
    "successfully parse the 'sample_job_config.json' from test resources" in {
      val jobConfigPath = getResourcePath("sample_job_config.json")
      val result = JobConfigLoader.loadJobConfig(jobConfigPath)

      result.isRight should be (true)
      val jobConfig = result.value

      jobConfig.job_name shouldBe Some("Sample Feature Generation Job")
      jobConfig.input_source.name shouldBe "customer_transactions"
      jobConfig.input_source.version shouldBe Some("1.0")
      jobConfig.feature_transformers.size shouldBe 2
      jobConfig.feature_transformers.head.name shouldBe "transaction_amount_log"
      jobConfig.feature_transformers.head.params.get("input_col").flatMap(_.asString) shouldBe Some("transaction_amount")
      
      jobConfig.output_sink.sink_type shouldBe "delta_table"
      jobConfig.output_sink.config.path shouldBe Some("/mnt/processed/feature_store/sample_output")
      jobConfig.output_sink.config.options.isDefined shouldBe true
      jobConfig.output_sink.config.options.get.get("mergeSchema").flatMap(_.asString) shouldBe Some("true")
    }
  }

  "SourceRegistry" should {
    "successfully parse source definition from the test resources" in {
      val sourceCatalogPath = getResourcePath("sample_source_definitions")
      val result = SourceRegistry.loadFromDirectory(sourceCatalogPath)

      result.isRight should be (true)
      val registry = result.value
      
      // Should find at least one source definition
      val allSources = registry.getAllSourceDefinitions()
      allSources.size should be >= 1

      // Check the customer_transactions source definition
      val sourceDefOpt = registry.getSourceDefinition("customer_transactions", "1.0")
      sourceDefOpt.isDefined should be (true)
      val sourceDef = sourceDefOpt.get

      sourceDef.name shouldBe "customer_transactions"
      sourceDef.version shouldBe "1.0"
      sourceDef.location shouldBe Some("src/test/resources/data/dummy_transactions.parquet")
      sourceDef.`type` shouldBe "parquet"
      sourceDef.fields.isDefined shouldBe true
      sourceDef.fields.get.length should be >= 2
      sourceDef.fields.get.head.name shouldBe "transaction_id"
      
      sourceDef.metadata.isDefined shouldBe true
      sourceDef.metadata.get.created_by shouldBe Some("test_user")
      sourceDef.metadata.get.tags shouldBe Some(List("test_data", "parquet"))
    }
  }
}
