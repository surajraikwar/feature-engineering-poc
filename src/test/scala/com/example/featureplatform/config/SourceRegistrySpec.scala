package com.example.featureplatform.config

import java.io.FileNotFoundException
import java.nio.file.{Files, Paths} // Added Files import
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues
import io.circe.{ParsingFailure, DecodingFailure} // For asserting error types

class SourceRegistrySpec extends AnyWordSpec with Matchers with EitherValues {

  private val resourcesPath = "src/test/resources/config_tests"

  // Helper to ensure semi-auto decoders for SourceDefinition and its members are in scope
  // if they are not automatically found via `import io.circe.generic.auto._` in SourceRegistry.
  // This is only needed if `SourceDefinition` and its nested case classes also need explicit decoders.
  // For now, assuming `io.circe.generic.auto._` in `ConfigLoader.scala` covers them,
  // or that `SourceDefinition`'s own companion object would have them if using semiauto.
  // If `SourceDefinition` decoding fails, explicit decoders like in `Models.scala` for `JobConfig` would be needed.
  // For this test, we assume SourceDefinition and its children are correctly set up for Circe decoding.
  // (No explicit decoders for SourceDefinition were added to Models.scala in previous steps, relying on auto-derivation)

  "SourceRegistry.loadFromDirectory" should {

    "successfully load all valid source definitions from a directory" in {
      val validSourcesDir = s"$resourcesPath/valid_sources"
      val result = SourceRegistry.loadFromDirectory(validSourcesDir)

      result.isRight should be (true)
      val registry = result.value

      registry.getAllSourceDefinitions().size shouldBe 2

      val source1 = registry.getSourceDefinition("transactions_parquet", "1.0")
      source1.isDefined should be (true)
      source1.get.name shouldBe "transactions_parquet"
      source1.get.`type` shouldBe "parquet"
      source1.get.entity shouldBe "transaction"
      source1.get.location shouldBe Some("/data/transactions/parquet/v1.0")
      source1.get.fields.get.head.name shouldBe "transaction_id"
      source1.get.metadata.get.created_by shouldBe Some("test_user_1")

      val source2 = registry.getSourceDefinition("customer_profiles_delta", "2.1")
      source2.isDefined should be (true)
      source2.get.name shouldBe "customer_profiles_delta"
      source2.get.location shouldBe Some("/data/customers/delta/v2.1")
      source2.get.metadata.get.tags shouldBe Some(List("pii", "delta_table"))
      
      registry.getSourceDefinition("non_existent_source", "1.0") shouldBe None
    }

    "return a Left with DecodingFailure if a directory contains a malformed JSON file that is empty" in {
      val mixedMalformedDir = s"$resourcesPath/mixed_sources_malformed"
      // malformed_source.json is now "{}" which is valid JSON but invalid for SourceDefinition
      val result = SourceRegistry.loadFromDirectory(mixedMalformedDir)

      result.isLeft should be (true)
      // SourceRegistry.loadFromDirectory propagates the first error encountered.
      // Parsing "{}" is fine, but decoding it to SourceDefinition will fail.
      result.left.value shouldBe a [DecodingFailure]
    }

    "return a Left with DecodingFailure if a directory contains an incomplete source definition" in {
      // This assumes `SourceDefinition` requires 'name', which it does.
      val mixedIncompleteDir = s"$resourcesPath/mixed_sources_incomplete"
      val result = SourceRegistry.loadFromDirectory(mixedIncompleteDir)

      result.isLeft should be (true)
      result.left.value shouldBe a [DecodingFailure]
    }

    "return an empty registry for an empty directory" in {
      val emptySourcesDirPath = Paths.get(resourcesPath, "empty_sources_test_temp")
      try {
        Files.createDirectories(emptySourcesDirPath) // Ensure it exists and is empty for this test run
        // Double check it's empty by deleting contents if any - defensive
        Option(emptySourcesDirPath.toFile.listFiles()).foreach(files => files.foreach(f => if(f.isFile) f.delete()))

        val result = SourceRegistry.loadFromDirectory(emptySourcesDirPath.toString)

        result.isRight should be (true)
        val registry = result.value
        registry shouldBe a [SourceRegistry]
        registry.getAllSourceDefinitions() shouldBe List.empty // More direct assertion
      } finally {
        // Clean up the created temp directory
        Option(emptySourcesDirPath.toFile.listFiles()).foreach(files => files.foreach(_.delete()))
        Files.deleteIfExists(emptySourcesDirPath)
      }
    }

    "return a Left with FileNotFoundException for a non-existent directory path" in {
      val nonExistentPath = s"$resourcesPath/non_existent_sources_dir"
      val result = SourceRegistry.loadFromDirectory(nonExistentPath)

      result.isLeft should be (true)
      result.left.value shouldBe a [FileNotFoundException]
    }
  }

  "SourceRegistry instance methods" should {
    // Setup a registry manually for these tests
    val sourceDef1 = SourceDefinition("s1", Some("desc1"), "1.0", "type1", "entity1", Some("loc1"), None, DatabricksSourceDetailConfig(), None, None)
    val sourceDef2 = SourceDefinition("s1", Some("desc2"), "2.0", "type1", "entity1", Some("loc2"), None, DatabricksSourceDetailConfig(), None, None)
    val sourceDef3 = SourceDefinition("s2", Some("desc3"), "1.0", "type2", "entity2", Some("loc3"), None, DatabricksSourceDetailConfig(), None, None)
    val sourcesMap = Map(
      ("s1", "1.0") -> sourceDef1,
      ("s1", "2.0") -> sourceDef2,
      ("s2", "1.0") -> sourceDef3
    )
    val registry = new SourceRegistry(sourcesMap)

    "getSourceDefinition(name, version) should retrieve the correct source" in {
      registry.getSourceDefinition("s1", "1.0") shouldBe Some(sourceDef1)
      registry.getSourceDefinition("s1", "2.0") shouldBe Some(sourceDef2)
      registry.getSourceDefinition("s2", "1.0") shouldBe Some(sourceDef3)
      registry.getSourceDefinition("s1", "3.0") shouldBe None // Non-existent version
      registry.getSourceDefinition("s3", "1.0") shouldBe None // Non-existent name
    }

    "getSourceDefinition(name) should retrieve a source if one exists for that name" in {
      // Current implementation returns the first encountered.
      // This test will depend on the underlying Map's iteration order for "s1",
      // but it should find one of them.
      val s1Def = registry.getSourceDefinition("s1")
      s1Def.isDefined shouldBe true
      s1Def.get should (equal (sourceDef1) or equal (sourceDef2)) // Corrected ScalaTest 'or' syntax

      registry.getSourceDefinition("s2") shouldBe Some(sourceDef3)
      registry.getSourceDefinition("s3") shouldBe None
    }

    "getAllSourceDefinitions should return all loaded definitions" in {
      val allDefs = registry.getAllSourceDefinitions()
      allDefs.size shouldBe 3
      allDefs should contain allOf (sourceDef1, sourceDef2, sourceDef3)
    }
  }
}
