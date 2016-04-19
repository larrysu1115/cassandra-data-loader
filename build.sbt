name := "CsvToCaxSStable"

version := "1.0"

scalaVersion := "2.10.6"

mainClass in (Compile, run) := Some("org.sws9f.caxloader.CsvToSSTableWriter")

// Cassandra
libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "3.0.2" exclude ("ch.qos.logback", "logback-classic") exclude ("org.slf4j", "log4j-over-slf4j")
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0"

// utility : CSV
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.2.2"
// utility : Logging
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.16"
// libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.16" 
// exclude ("ch.qos.logback", "logback-classic") exclude ("org.slf4j", "log4j-over-slf4j")

// Utility : argument parser
libraryDependencies += "com.github.scopt" %% "scopt" % "3.4.0"
// utility : IO
libraryDependencies += "com.github.scala-incubator.io" %% "scala-io-file" % "0.4.3"


retrieveManaged := true


