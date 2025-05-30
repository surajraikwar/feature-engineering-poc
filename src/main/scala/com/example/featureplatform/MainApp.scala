package com.example.featureplatform

import com.example.featureplatform.runner.JobRunner
import org.slf4j.LoggerFactory

object MainApp {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting MainApp to invoke JobRunner for testing.")

    val jobConfigPath = "src/test/resources/sample_job_config.yaml"
    val sourceCatalogPath = "src/test/resources/sample_source_definitions"
    val sparkMaster = "local[*]" // Run Spark locally for this test

    val runnerArgs = Array(jobConfigPath, sourceCatalogPath, sparkMaster)

    logger.info(s"Calling JobRunner.main with args: ${runnerArgs.mkString(" ")}")
    try {
      JobRunner.main(runnerArgs)
      logger.info("JobRunner finished.")
    } catch {
      case e: Exception =>
        logger.error(s"Exception caught in MainApp while running JobRunner: ${e.getMessage}", e)
        // JobRunner itself should handle System.exit, but this is a fallback.
        System.exit(1) 
    }
  }
}
