// Core plugins
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")  // For creating fat JARs
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "2.0.0")     // Git integration
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")  // Code formatting
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")  // Release management
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.7") // Test coverage
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16") // Application packaging
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0") // Build info
addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")    // Project documentation
addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.3") // GitHub Pages integration

// For better dependency management
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.4.2") // Scalac options
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4") // Dependency updates

// For Databricks deployment
addSbtPlugin("com.databricks" % "sbt-databricks" % "0.1.6")
