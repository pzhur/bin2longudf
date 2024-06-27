name := """bin2la_spark_udf"""
ThisBuild / version := "0.1.0"
ThisBuild / organization := "gov.census.das"

ThisBuild / scalaVersion := "2.12.19"
scalacOptions ++= Seq("-unchecked", "-feature", "-deprecation")

lazy val root = (project in file("."))
  .settings(
    name := "bin2longudf",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-sql_2.12" % "3.5.0" % Provided,
      "org.apache.spark" % "spark-core_2.12" % "3.5.1",
      //"org.apache.maven.plugins" % "maven-assembly-plugin" % "3.7.0",
    )
  )
