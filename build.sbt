name := "SparkStreaming"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.amazonaws" % "amazon-kinesis-client" % "1.6.1"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.6.3"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.3"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kinesis-asl_2.11" % "2.4.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.1.1"