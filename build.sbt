name := "recommender-test"

version := "0.1"

scalaVersion := "2.12.8"

logLevel := Level.Warn

val SparkVersion = "2.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % SparkVersion,
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "org.apache.spark" %% "spark-mllib" % SparkVersion
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}