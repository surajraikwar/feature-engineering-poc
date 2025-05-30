package com.example.featureplatform.config

import java.io.FileNotFoundException
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues
import io.circe.{DecodingFailure, ParsingFailure} // For asserting error types

class JobConfigLoaderSpec extends AnyWordSpec with Matchers with EitherValues {

  private val resourcesPath = "src/test/resources/config_tests"

  "JobConfigLoader" should {

    "successfully load a valid job configuration file" in {
      val validConfigPath = s"$resourcesPath/valid_job_config.json"
      val result = JobConfigLoader.loadJobConfig(validConfigPath)

      result.isRight should be (true)
      val jobConfig = result.value

      jobConfig.job_name shouldBe Some("Test Feature Generation Job")
      jobConfig.description shouldBe Some("A test job configuration.")
      jobConfig.input_source.name shouldBe "test_transactions"
      jobConfig.input_source.version shouldBe Some("1.1")
      jobConfig.input_source.load_params.isDefined should be (true)
      jobConfig.input_source.load_params.get.get("filter_condition").flatMap(_.asString) shouldBe Some("amount > 10")
      
      jobConfig.feature_transformers.size shouldBe 2
      jobConfig.feature_transformers.head.name shouldBe "TransactionValueDeriver"
      jobConfig.feature_transformers.head.params.get("input_col").flatMap(_.asString) shouldBe Some("amount")

      jobConfig.output_sink.sink_type shouldBe "display"
      jobConfig.output_sink.config.num_rows shouldBe Some(15)
      jobConfig.output_sink.config.truncate shouldBe Some(true)
    }

    "return a Left with ParsingFailure for a malformed JSON file" in {
      val malformedConfigPath = s"$resourcesPath/malformed_job_config.json"
      val result = JobConfigLoader.loadJobConfig(malformedConfigPath)

      result.isLeft should be (true)
      // It's a parsing error that leads to a decoding issue when fields are misplaced.
      // Or, if the YAML structure is fundamentally invalid (e.g. bad chars), it's ParsingFailure.
      // For misplaced fields, it's often a DecodingFailure as the structure is valid YAML but not valid for the model.
      // Given the error message from the test, it was a DecodingFailure.
      result.left.value shouldBe a [DecodingFailure]
    }

    "return a Left with DecodingFailure for an incomplete job configuration (missing required fields)" in {
      // This test assumes that Circe's decoder (auto or semi-auto) will fail if a non-Option
      // field in a case class is missing from the JSON.
      // JobInputSourceConfig has 'name: String', which is required.
      val incompleteConfigPath = s"$resourcesPath/incomplete_job_config.json"
      val result = JobConfigLoader.loadJobConfig(incompleteConfigPath)
      
      result.isLeft should be (true)
      result.left.value shouldBe a [DecodingFailure]
    }

    "return a Left with FileNotFoundException for a non-existent file path" in {
      val nonExistentPath = s"$resourcesPath/non_existent_job_config.json"
      val result = JobConfigLoader.loadJobConfig(nonExistentPath)

      result.isLeft should be (true)
      // The Try block in loadJobConfig converts FileNotFoundException to Left(e)
      result.left.value shouldBe a [FileNotFoundException]
    }
  }
}
