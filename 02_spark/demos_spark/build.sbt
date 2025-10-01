ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "demos_spark"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "4.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.0.1"

libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.8.4"
libraryDependencies +=  "org.plotly-scala" %% "plotly-almond" % "0.8.4"
