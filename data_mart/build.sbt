lazy val commonSettings = Seq(
  name := "data_mart",
  version := "1.0",
  scalaVersion := "2.11.12",
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.5",
    "org.apache.spark" %% "spark-sql" % "2.4.5",
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.0",
    "org.elasticsearch" %% "elasticsearch-spark-20" % "7.14.2",
    "org.postgresql" % "postgresql" % "42.2.12"
  )
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "data_mart_2.11-1.0.jar"