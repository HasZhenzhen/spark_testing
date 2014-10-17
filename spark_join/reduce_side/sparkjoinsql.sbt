name := "Spark_Join_Sql"

version := "1.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.1.0",
  "org.apache.spark" %% "spark-sql" % "1.1.0"
)
