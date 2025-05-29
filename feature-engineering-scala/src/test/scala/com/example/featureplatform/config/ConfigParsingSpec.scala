package com.example.featureplatform.config

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues

class ConfigParsingSpec extends AnyWordSpec with Matchers with EitherValues {

  // Helper to get resource path. Assumes test execution from project root.
  def getResourcePath(relativePath: String): String = {
    s"src/main/resources/$relativePath"
  }

  "JobConfigLoader" should {
    "successfully parse the 'generate_transaction_features_job.yaml' from resources" in {
      val jobConfigPath = getResourcePath("configs/jobs/generate_transaction_features_job.yaml")
      val result = JobConfigLoader.loadJobConfig(jobConfigPath)

      result.isRight should be (true)
      val jobConfig = result.value

      jobConfig.job_name shouldBe Some("generate_transaction_features")
      jobConfig.input_source.name shouldBe "temp_fact_mm_transaction_source"
      jobConfig.input_source.version shouldBe Some("v1")
      jobConfig.feature_transformers.size shouldBe 7
      jobConfig.feature_transformers(4).name shouldBe "TransactionValueDeriver"
      jobConfig.feature_transformers(4).params.get("high_value_threshold").flatMap(_.as[Double].toOption) shouldBe Some(1000.0)
      
      jobConfig.output_sink.sink_type shouldBe "delta_table"
      jobConfig.output_sink.config.path shouldBe Some("temp.feature_platform_testing.temp_transaction_features_v1")
      jobConfig.output_sink.config.options.isDefined shouldBe true
      jobConfig.output_sink.config.options.get.get("mergeSchema").flatMap(_.asString) shouldBe Some("true")
    }
  }

  "SourceRegistry" should {
    "successfully parse 'temp_fact_mm_transaction_source.yaml' from the resource catalog" in {
      val sourceCatalogPath = getResourcePath("source_catalog")
      val result = SourceRegistry.loadFromDirectory(sourceCatalogPath)

      result.isRight should be (true)
      val registry = result.value
      
      registry.getAllSourceDefinitions().size should be >= 1 // Could be other files too if tests run in sequence within same JVM

      val sourceDefOpt = registry.getSourceDefinition("temp_fact_mm_transaction_source", "v1")
      sourceDefOpt.isDefined should be (true)
      val sourceDef = sourceDefOpt.get

      sourceDef.name shouldBe "temp_fact_mm_transaction_source"
      sourceDef.version shouldBe "v1"
      sourceDef.location shouldBe Some("temp.feature_platform_testing.temp_fact_mm_transaction")
      sourceDef.`type` shouldBe "delta" // Check the 'type' field in SourceDefinition
      sourceDef.fields.isDefined shouldBe true
      sourceDef.fields.get.length should be >= 2 // Checks at least the example fields provided
      sourceDef.fields.get.head.name shouldBe "flag"
      
      sourceDef.metadata.isDefined shouldBe true
      sourceDef.metadata.get.created_by shouldBe Some("Jules AI Agent")
      sourceDef.metadata.get.tags shouldBe Some(List("temp_data", "fact_table"))
    }
  }
}
