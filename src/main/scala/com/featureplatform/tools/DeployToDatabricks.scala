package com.featureplatform.tools

import java.nio.file.{Files, Paths}
import scala.sys.process._
import scala.util.{Try, Success, Failure}
import play.api.libs.json._
import java.io.File

/**
 * Tool for deploying the feature engineering job to Databricks with Unity Catalog.
 */
object DeployToDatabricks {
  private val BaseDir = Paths.get(".").toAbsolutePath
  private val ConfigDir = BaseDir.resolve("configs")
  private val JobConfigFile = ConfigDir.resolve("jobs/databricks_transaction_features_job.json")
  private val DatabricksConfigFile = BaseDir.resolve("databricks-config.json")
  private val AssemblyJar = BaseDir.resolve("target/scala-2.13/feature-platform-assembly-1.0.0.jar")

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      printUsage()
      System.exit(1)
    }

    args.head match {
      case "validate" => validateConfigs()
      case "create-job" => createJob()
      case "run-job" => 
        if (args.length < 2) {
          println("Error: Job ID is required for run-job command")
          printUsage()
          System.exit(1)
        } else {
          runJob(args(1))
        }
      case "all" => 
        validateConfigs()
        val jobId = createJob()
        runJob(jobId)
      case _ => 
        println(s"Error: Unknown command '${args.head}'")
        printUsage()
        System.exit(1)
    }
  }

  private def validateConfigs(): Unit = {
    println("Validating configurations...")
    
    // Check if assembly JAR exists
    if (!Files.exists(AssemblyJar)) {
      println(s"Error: Assembly JAR not found at $AssemblyJar. Please run 'sbt assembly' first.")
      System.exit(1)
    }
    
    // Validate job config
    val jobConfig = Json.parse(Files.readAllBytes(JobConfigFile))
    val inputSource = (jobConfig \ "input_source").as[JsObject]
    val outputSink = (jobConfig \ "output_sink").as[JsObject]
    
    if ((inputSource \ "source_type").as[String] != "unity_catalog") {
      println("Error: Input source must use Unity Catalog")
      System.exit(1)
    }
    
    if ((outputSink \ "sink_type").as[String] != "unity_catalog") {
      println("Error: Output sink must use Unity Catalog")
      System.exit(1)
    }
    
    println("Configurations validated successfully.")
  }

  private def createJob(): String = {
    println("Creating Databricks job...")
    
    // Upload the assembly JAR to Unity Catalog
    val ucVolumePath = uploadToUnityVolume()
    
    // Create the job using the Databricks CLI
    val result = runCommandWithOutput(s"databricks jobs create --json-file $DatabricksConfigFile")
    
    // Parse the job ID from the output
    val jobIdPattern = "\"job_id\":\s*(\d+)".r
    jobIdPattern.findFirstMatchIn(result)
      .map(_.group(1))
      .getOrElse {
        println("Error: Failed to parse job ID from response")
        println(s"Response: $result")
        System.exit(1)
        ""
      }
  }
  
  private def uploadToUnityVolume(): String = {
    println("Uploading assembly JAR to Unity Catalog Volume...")
    
    // Create a unique path in the Unity Catalog volume
    val timestamp = System.currentTimeMillis()
    val volumePath = s"/Volumes/prod/feature_platform/jars/feature-platform-$timestamp.jar"
    
    // Upload the JAR
    runCommand(s"databricks fs cp --overwrite $AssemblyJar dbfs:$volumePath")
    
    println(s"Uploaded JAR to Unity Catalog Volume: $volumePath")
    volumePath
  }

  private def runJob(jobId: String): Unit = {
    println(s"Running job $jobId...")
    runCommand(s"databricks jobs run-now --job-id $jobId")
  }

  private def runCommand(cmd: String): Unit = {
    println(s"Running: $cmd")
    val exitCode = cmd.!
    if (exitCode != 0) {
      println(s"Command failed with exit code $exitCode")
      System.exit(exitCode)
    }
  }

  private def runCommandWithOutput(cmd: String): String = {
    println(s"Running: $cmd")
    Try(cmd.!!) match {
      case Success(output) => output
      case Failure(e) =>
        println(s"Command failed: ${e.getMessage}")
        System.exit(1)
        ""
    }
  }

  private def printUsage(): Unit = {
    println("""Usage: DeployToDatabricks <command>
      |
      |Commands:
      |  validate    Validate the configuration files
      |  create-job  Create a new Databricks job using Unity Catalog
      |  run-job <id> Run an existing Databricks job
      |  all         Run all deployment steps (validate, create-job, run-job)
      |
      |Note: Ensure you have the following before running:
      |  1. Databricks CLI configured with proper permissions
      |  2. Unity Catalog enabled in your workspace
      |  3. Required Unity Catalog privileges on the target schemas
      |  4. Run 'sbt assembly' to create the assembly JAR
      |""".stripMargin)
  }
}
