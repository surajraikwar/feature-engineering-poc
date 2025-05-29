lazy val scala212 = "2.12.19" // Using latest 2.12.x patch version

lazy val commonSettings = Seq(
  scalaVersion := scala212,
  organization := "com.example.featureplatform",
  version := "0.1.0-SNAPSHOT",
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val sparkVersion = "3.5.0" // Updated to latest Spark version for better JDK compatibility
lazy val circeVersion = "0.14.1" // Choose a recent stable Circe version
lazy val scalatestVersion = "3.2.11" // Choose a recent stable ScalaTest version
lazy val logbackVersion = "1.2.10" // Choose a recent stable Logback version

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "feature-engineering-scala",
    Compile / run / fork := true, // Ensure javaOptions are applied during sbt run
    javaOptions ++= Seq( // Add JVM options for JDK compatibility
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED" // Often needed too
    ),
    libraryDependencies ++= Seq(
      // Spark and Delta dependencies are "provided" on Databricks
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "io.delta" %% "delta-spark" % "3.1.0" % "provided", // Delta also provided on Databricks

      // Circe YAML parsing
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-yaml" % circeVersion,

      // Logging
      "ch.qos.logback" % "logback-classic" % logbackVersion,

      // Testing
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    ),
    // sbt-assembly settings (example, adjust as needed)
    assembly / mainClass := Some("com.example.featureplatform.MainApp"),
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )
