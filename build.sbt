name := "feature-platform"
version := "0.1.0"
scalaVersion := "2.12.15"

// Enable sbt-assembly plugin
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Databricks Runtime version should match your cluster's runtime
val sparkVersion = "3.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "com.typesafe.play" %% "play-json" % "2.9.4",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
)

// Assembly settings
assembly / assemblyJarName := "feature-platform.jar"
