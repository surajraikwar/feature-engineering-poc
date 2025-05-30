lazy val scala212 = "2.12.15" // Stable Scala 2.12.x version

lazy val commonSettings = Seq(
  scalaVersion := scala212,
  organization := "com.example.featureplatform",
  version := "0.1.0-SNAPSHOT",
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "public" at "https://repo1.maven.org/maven2/"
  )
)

// Library versions
lazy val sparkVersion = "3.3.2"
lazy val deltaVersion = "2.3.0" // Delta Lake 2.3.0 is for Spark 3.3.x
lazy val circeVersion = "0.14.5"
lazy val scalatestVersion = "3.2.15"
lazy val logbackVersion = "1.4.7"
lazy val pureconfigVersion = "0.17.4"

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "feature-engineering-scala",
    fork := true,
    javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "-Djava.security.manager=allow",
      "-Dio.netty.tryReflectionSetAccessible=true"
    ),
    Test / javaOptions ++= Seq(
      "-Dspark.master=local[*]",
      "-Dspark.driver.host=localhost",
      "-Dspark.driver.bindAddress=127.0.0.1",
      "-Dspark.sql.shuffle.partitions=1",
      "-Dspark.ui.enabled=false",
      "-Dspark.sql.warehouse.dir=target/spark-warehouse"
    ),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided; Test", // Add Test scope
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided; Test", // Add Test scope
      "io.delta"         %% "delta-core" % deltaVersion,

      "com.github.pureconfig" %% "pureconfig"      % pureconfigVersion,
      "io.circe"              %% "circe-yaml"      % "0.14.2",
      "io.circe"              %% "circe-generic"   % circeVersion,
      "io.circe"              %% "circe-parser"    % circeVersion,
      "org.yaml"              %  "snakeyaml"       % "1.33",
      "ch.qos.logback"        %  "logback-classic" % logbackVersion,
      "org.scalatest"         %% "scalatest"       % scalatestVersion % Test
    ),
    assembly / mainClass := Some("com.example.featureplatform.MainApp"),
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @_*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
