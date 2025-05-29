// Core plugins
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")  // For creating fat JARs
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.2")     // Git integration
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")  // Code formatting
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")  // Release management
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.7") // Test coverage
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.9.16") // Application packaging
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.11.0") // Build info

// For better dependency management
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.4.4") // Scalac options
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4") // Dependency updates

// Commented out problematic plugins - add them back one by one if needed
// addSbtPlugin("com.typesafe.sbt" % "sbt-site" % "1.4.1")    // Project documentation
// addSbtPlugin("com.typesafe.sbt" % "sbt-ghpages" % "0.6.3") // GitHub Pages integration
// addSbtPlugin("com.databricks" % "sbt-databricks" % "0.1.6") // Databricks deployment
