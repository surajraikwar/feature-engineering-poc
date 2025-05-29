name := "feature-platform"
version := "1.0.0"
scalaVersion := "2.12.15"
organization := "com.featureplatform"

// Spark and Delta Lake versions compatible with Databricks Runtime 13.x
val sparkVersion = "3.4.1"
val deltaVersion = "2.4.0"

// Enable sbt-assembly plugin
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "services", _*) => MergeStrategy.first
  case PathList("META-INF", _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// Main dependencies
libraryDependencies ++= Seq(
  // Spark Core
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  
  // Delta Lake with Unity Catalog support
  "io.delta" %% "delta-core" % deltaVersion % "provided",
  "io.delta" %% "delta-storage" % deltaVersion % "provided",
  
  // JSON processing
  "com.typesafe.play" %% "play-json" % "2.9.4",
  
  // Logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "ch.qos.logback" % "logback-classic" % "1.2.11" % Runtime,
  
  // Testing
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.scalatestplus" %% "mockito-3-4" % "3.2.10.0" % Test,
  "io.delta" %% "delta-core" % deltaVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests"
)

// Assembly settings
assembly / assemblyJarName := s"${name.value}-${version.value}.jar"
assembly / test := {}
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.common.**" -> "shaded.com.google.common.@1").inAll
)

// Skip tests during assembly
test in assembly := {}

// Test configuration
Test / fork := true
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
Test / logBuffered := false
Test / parallelExecution := false  // Run tests sequentially to avoid port conflicts

// Java options for running Spark
javaOptions ++= Seq(
  "-Xms2G",
  "-Xmx4G",
  "-XX:MaxMetaspaceSize=2G",
  "-XX:+CMSClassUnloadingEnabled",
  "-Dlog4j2.formatMsgNoLookups=true"
)

// Configure the main class for running with 'sbt run'
Compile / run / mainClass := Some("com.featureplatform.runner.FeatureJobRunner")
Compile / run / fork := true
Compile / run / javaOptions ++= javaOptions.value

// Add configuration files to the classpath
Compile / unmanagedResources ++= {
  val base = baseDirectory.value
  (base / "configs" * "*.json").get
}

// Add log4j2.properties to the classpath for local execution
Compile / unmanagedResources += baseDirectory.value / "src" / "main" / "resources" / "log4j2.properties"

// Enable better dependency management
updateOptions := updateOptions.value.withCachedResolution(true)

// Configure publishing (optional)
publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

// Add scoverage for test coverage (optional)
coverageEnabled := true
coverageMinimumStmtTotal := 80
coverageFailOnMinimum := true
