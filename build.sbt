name := """bin2la_spark_udf"""
ThisBuild / version := "0.1.0"
ThisBuild / organization := "gov.census.das"

ThisBuild / scalaVersion := "3.2.2"
scalacOptions ++= Seq("-unchecked", "-feature", "-deprecation")

lazy val root = (project in file("."))
  .settings(
    name := "bin2longudf",
    libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1" % Provided
  )
