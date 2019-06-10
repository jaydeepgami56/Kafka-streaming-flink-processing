name := "kafka-flink-stream-processing"

version := "0.1"

scalaVersion := "2.12.8"

val flinkVersion = "1.7.2"

libraryDependencies ++= Seq("org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-runtime-web" % flinkVersion % Test,
  "org.apache.flink" %% "flink-table" % flinkVersion % "provided",
  "org.apache.flink" % "flink-json" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
  "org.apache.flink" % "flink-avro" % flinkVersion)