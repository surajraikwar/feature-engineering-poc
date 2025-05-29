import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy
import scoverage.ScoverageKeys._

// Common settings for all projects
lazy val commonSettings = Seq(
  organization := "com.featureplatform",
  version := "1.0.0",
  scalaVersion := "2.12.15",
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ypartial-unification",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture",
    "-Ywarn-unused:imports"
  ),
  javacOptions ++= Seq(
    "-source", "1.8",
    "-target", "1.8",
    "-Xlint:deprecation"
  ),
  // Disable parallel execution in tests to avoid Spark session issues
  Test / parallelExecution := false,
  Test / fork := true,
  // Show full stack traces in tests
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
  // Memory settings for forked JVMs
  javaOptions ++= Seq(
    "-Xms2G",
    "-Xmx4G",
    "-XX:MaxMetaspaceSize=2G",
    "-XX:+CMSClassUnloadingEnabled",
    "-Dlog4j2.formatMsgNoLookups=true"
  ),
  // Include resources in the classpath
  Compile / unmanagedResources ++= (baseDirectory.value / "src" / "main" / "resources" * "*").get,
  Test / unmanagedResources ++= (baseDirectory.value / "src" / "test" / "resources" * "*").get
)

// Dependencies
lazy val dependencies = {
  // Version definitions - using stable versions that work well with Scala 2.12
  val sparkVersion = "3.4.1"
  val deltaVersion = "2.4.0"
  val catsEffectVersion = "3.2.9"   // Using version compatible with pureconfig-cats-effect
  val catsCoreVersion = "2.9.0"     // Cats core version
  val pureConfigVersion = "0.17.1"  // For configuration
  val logbackVersion = "1.2.12"     // For logging
  val scalaLoggingVersion = "3.9.5" // For logging
  val circeVersion = "0.14.5"       // For JSON processing
  val enumeratumVersion = "1.7.0"   // For type-safe enums

  Seq(
    // Core dependencies
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
    "io.delta" %% "delta-core" % deltaVersion % Provided,
    
    // Functional programming
    "org.typelevel" %% "cats-core" % catsCoreVersion,
    "org.typelevel" %% "cats-effect" % catsEffectVersion,
    "org.typelevel" %% "cats-effect-kernel" % catsEffectVersion,
    "org.typelevel" %% "cats-effect-std" % catsEffectVersion,
    
    // Configuration - using a version compatible with cats-effect 3.x
    "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
    "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion,
    
    // Logging
    "ch.qos.logback" % "logback-classic" % logbackVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    
    // JSON processing
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    
    // Enumerations
    "com.beachape" %% "enumeratum" % enumeratumVersion,
    "com.beachape" %% "enumeratum-circe" % enumeratumVersion,
    
    // Testing - using versions compatible with Scala 2.12 and cats-effect 3.x
    "org.scalatest" %% "scalatest" % "3.2.15" % Test,
    "org.scalatestplus" %% "scalacheck-1-15" % "3.2.11.0" % Test,
    "org.scalacheck" %% "scalacheck" % "1.15.4" % Test,
    "org.typelevel" %% "cats-effect-testing-scalatest" % "1.4.0" % Test excludeAll(
      ExclusionRule(organization = "org.typelevel", name = "cats-effect-std_2.12"),
      ExclusionRule(organization = "org.typelevel", name = "cats-effect-kernel_2.12")
    )
  )
}

// Assembly settings
lazy val assemblySettings = Seq(
  assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "services", _*) => MergeStrategy.first
    case PathList("META-INF", _*) => MergeStrategy.discard
    case "reference.conf" => MergeStrategy.concat
    case "application.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  },
  assembly / test := {},
  assembly / assemblyShadeRules := Seq(
    ShadeRule.rename("com.google.common.**" -> "shaded.com.google.common.@1").inAll
  ),
  // Skip tests during assembly
  assembly / test := {}
)

// Coverage settings
lazy val coverageSettings = Seq(
  coverageMinimumStmtTotal := 80,
  coverageFailOnMinimum := true,
  coverageHighlighting := true,
  coverageExcludedPackages := """.*""".stripMargin
)

// Main project
lazy val root = (project in file("."))
  .settings(
    name := "feature-platform",
    commonSettings,
    assemblySettings,
    coverageSettings,
    libraryDependencies ++= dependencies,
    // Main class for running with 'sbt run'
    Compile / run / mainClass := Some("com.featureplatform.FeaturePlatformApp"),
    // Add configuration files to the classpath
    Compile / unmanagedResources ++= (baseDirectory.value / "configs" * "*.conf").get,
    // Add log4j2.properties to the classpath for local execution
    Compile / unmanagedResources += baseDirectory.value / "src" / "main" / "resources" / "log4j2.properties"
  )

// Aliases
addCommandAlias("check", "scalafmtCheckAll; scalafmtSbtCheck; test:scalafmtCheckAll")
addCommandAlias("fmt", "all scalafmtSbt scalafmtAll")
