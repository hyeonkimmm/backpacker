name := "EcommerceSessionApp"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5",
  "org.apache.spark" %% "spark-sql"  % "3.5.5",
  "org.apache.spark" %% "spark-hive" % "3.5.5"
)
