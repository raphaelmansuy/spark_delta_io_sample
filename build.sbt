name := "my-scala-project"

version := "1.0"


val sparkVersion = "3.3.0"
val scalaLanguageVersion = "2.12"

scalaVersion := scalaLanguageVersion + ".15"


libraryDependencies += "org.scala-lang" % "scala-library" % scalaLanguageVersion

// Add spark dependencies
libraryDependencies += "org.apache.spark" % f"spark-core_$scalaLanguageVersion" % sparkVersion
libraryDependencies += "org.apache.spark" % f"spark-sql_$scalaLanguageVersion" % sparkVersion
// hadoop-client-api-3.3.2.jar is required for spark 3.3.0
libraryDependencies += "org.apache.hadoop" % "hadoop-client-api" % "3.3.2"

// Import delta.io dependencies
libraryDependencies += "io.delta" % f"delta-core_$scalaLanguageVersion" % "2.1.1"


