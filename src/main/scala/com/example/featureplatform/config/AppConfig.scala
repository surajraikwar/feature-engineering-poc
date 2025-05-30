package com.example.featureplatform.config

import scala.util.Try

/**
 * Main application configuration
 */
case class AppConfig(
  databricks: DatabricksConfig,
  spark: SparkConfig
)

/**
 * Databricks specific configuration
 */
case class DatabricksConfig(
  serverHostname: String,
  httpPath: String,
  token: String,
  clusterId: String,
  catalog: String,
  schema: String
) {
  def sparkRemote: String = 
    s"sc://${serverHostname}:443/;token=${token};x-databricks-cluster-id=${clusterId}"
  
  def jdbcUrl: String =
    s"jdbc:spark://${serverHostname}:443/default;transportMode=http;ssl=1;httpPath=${httpPath};AuthMech=3;UID=token;PWD=${token}"
}

/**
 * Spark configuration
 */
case class SparkConfig(
  master: String = "local[*]",
  appName: String = "feature-engineering-scala",
  config: Map[String, String] = Map.empty
)

object AppConfig {
  /**
   * Load configuration from environment variables
   */
  def fromEnv(): Either[String, AppConfig] = {
    for {
      serverHostname <- sys.env.get("DATABRICKS_SERVER_HOSTNAME").toRight("DATABRICKS_SERVER_HOSTNAME not set")
      httpPath <- sys.env.get("DATABRICKS_HTTP_PATH").toRight("DATABRICKS_HTTP_PATH not set")
      token <- sys.env.get("DATABRICKS_TOKEN").toRight("DATABRICKS_TOKEN not set")
      clusterId <- sys.env.get("DATABRICKS_CLUSTER_ID").toRight("DATABRICKS_CLUSTER_ID not set")
      catalog <- sys.env.get("DATABRICKS_CATALOG").toRight("DATABRICKS_CATALOG not set")
      schema <- sys.env.get("DATABRICKS_SCHEMA").toRight("DATABRICKS_SCHEMA not set")
    } yield {
      val databricksConfig = DatabricksConfig(
        serverHostname = serverHostname,
        httpPath = httpPath,
        token = token,
        clusterId = clusterId,
        catalog = catalog,
        schema = schema
      )
      
      AppConfig(
        databricks = databricksConfig,
        spark = SparkConfig()
      )
    }
  }
  
  /**
   * Load configuration from a file
   */
  def fromFile(configFile: String = "application.conf"): Either[String, AppConfig] = {
    Try(ConfigSource.file(configFile).loadOrThrow[AppConfig])
      .toEither
      .left.map(_.getMessage)
  }
}
