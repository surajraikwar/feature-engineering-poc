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
lazy val sparkVersion = "3.5.0"
lazy val deltaVersion = "3.1.0" // Delta Lake 2.3.0 is for Spark 3.3.x
lazy val circeVersion = "0.14.5" // Reverted Circe version
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
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "io.delta"         %% "delta-spark" % deltaVersion,

      "com.github.pureconfig" %% "pureconfig"      % pureconfigVersion,
      // "io.circe"              %% "circe-yaml"      % "0.14.2", // Removed
      "io.circe"              %% "circe-generic"   % circeVersion,
      "io.circe"              %% "circe-parser"    % circeVersion,
      // "org.yaml"              %  "snakeyaml"       % "1.33", // Removed
      "ch.qos.logback"        %  "logback-classic" % logbackVersion,
      "org.scalatest"         %% "scalatest"       % scalatestVersion % Test
    ),
    assembly / mainClass := Some("com.example.featureplatform.MainApp"),
    assembly / assemblyJarName := s"${name.value}-assembly-${version.value}.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat // Concatenate DataSourceRegister files
      case PathList("META-INF", xs @ _*) =>
        MergeStrategy.discard // Discard other META-INF files
      case _ =>
        MergeStrategy.first // Use the first encountered for other conflicts
    },
    // Explicitly override Cats versions to ensure consistency
    dependencyOverrides ++= Seq(
      "org.typelevel" %% "cats-core"   % "2.9.0",
      "org.typelevel" %% "cats-kernel" % "2.9.0"
    )
  )

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll,
  ShadeRule.rename("cats.kernel.**" -> s"new_cats.kernel.@1").inAll
)
